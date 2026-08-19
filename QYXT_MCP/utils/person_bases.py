#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文件名:	person_utils.py
作者:	shihy
创建日期:	2026-04-22
描述:	矿井人员定位相关的数据处理与工具函数。用于时间格式化、筛选、统计分析和结构化人员信息，适配 MCP 服务人员定位业务功能需求。
"""



import time
import traceback

from pprint import pp, pprint
import datetime

import json
import logging

import requests
import clickhouse_connect
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union

import sys
import os

from copy import deepcopy
from fuzzywuzzy import fuzz, process

from collections import defaultdict
from bisect import bisect_left


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqls.person_sqls import (
    GET_PERSON_LATEST_SQL,
    GET_PERSON_TRAJECTORY_SQL,
    GET_TODAY_PERSONS_SQL,
    GET_REALTIME_STATION_HEAD_INFO_SQL,
    GET_AREA_LIMITS_SQL, GET_TODAY_CARS_SQL
)

# 配置日志，日志存储在文件中
from logging.handlers import RotatingFileHandler

LOG_DIR = 'logs'
LOG_FILE = os.path.join(LOG_DIR, 'mine_personnel_service.log')

# 确保日志目录存在
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=50 * 1024 * 1024,  # 50MB
    backupCount=5,
    encoding='utf-8'
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(handler)
# 防止日志重复记录（如果已有stream handler则移除）
for h in logging.getLogger().handlers:
    if isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler):
        logging.getLogger().removeHandler(h)
logger = logging.getLogger("MinePersonnelService")



def time_to_seconds(t):
    h, m, s = map(int, t.split(':'))
    return h * 3600 + m * 60 + s

def merge_adjacent_segments(segments):
    if not segments:
        return []
    
    merged = []
    i = 0
    n = len(segments)
    
    while i < n:
        current = segments[i]
        start_time = current['S_E_Time'][0]
        end_time = current['S_E_Time'][1]
        main_area = current['area']          # 保留第一个出现的 area
        
        j = i + 1
        is_jumping = False
        
        while j < n:
            next_seg = segments[j]
            prev_end = end_time
            next_start = next_seg['S_E_Time'][0]
            prev_end = prev_end[11:] 
            next_start = next_start[11:] 
            # 时间间隔超过10秒，停止合并
            if time_to_seconds(next_start) - time_to_seconds(prev_end) > 16:
                break
            
            # 判断是否为来回跳变（与main_area不同，且后续会再跳回来）
            if next_seg['area'] != main_area:
                is_jumping = True
                end_time = next_seg['S_E_Time'][1]   # 扩展结束时间
                j += 1
                continue
            else:
                # 又跳回 main_area，继续扩展
                end_time = next_seg['S_E_Time'][1]
                j += 1
                continue
        
        # 如果发生了来回跳变，则只保留第一个 area，并合并时间区间
        if is_jumping:
            merged.append({
                'S_E_Time': [start_time, end_time],
                'area': main_area
            })
        else:
            # 没有跳变，保留原始 segment
            merged.append(current)
        
        i = j
    
    return merged

def merge_consecutive_same_area(segments):
    """
    合并前后相邻且 area 相同的区间
    :param segments: List[dict]，每个元素如 {'S_E_Time': [start, end], 'area': xxx}
    :return: 合并后的 segments
    """
    if not segments:
        return []

    merged = []
    prev = segments[0].copy()

    for seg in segments[1:]:
        if seg['area'] == prev['area'] :
            prev['S_E_Time'][1] = seg['S_E_Time'][1]
        else:
            merged.append(prev)
            prev = seg.copy()
    merged.append(prev)
    return merged


def normalize(v):
            if v is None:
                return None
            if isinstance(v, str):
                return [v]
            return v

def fuzzy_match(value, filters, threshold=50):
    """
    使用 fuzzywuzzy 支持模糊匹配:
    若 filters 中任意元素与 value 相似度超过 threshold，则视为匹配(True)。
    """
    if not filters:
        return False
    value = str(value) if value is not None else ""
    # 用 process.extract 批量比对，找到得分最高的
    # 如果 filters 不是列表转为列表
    if filters is not None and not isinstance(filters, (list, tuple)):
        filters = [filters]

    all_matches = process.extract(value, filters)
    if all_matches:
        best_match, max_score = max(all_matches, key=lambda x: x[1])
    else:
        best_match, max_score = None, 0

    # 若完全包含直接返回True
    for f in filters:
        if f in value:
            return True

    # 使用门限
    return max_score >= threshold
    

class PersonBase:
    """
    人员相关功能的基类，供工具与utils共用。
    主要封装通用的人员信息方法和属性。实际业务类可继承它。
    """

    def __init__(self,client):
        # 可以定义一些共用的属性，如站点名称缓存等
       
        self.client = client
        self.station_names_time = 0
        self.station_names = {}
        self._init_station_names()
        self._init_urls()

        # 初始化相关接口
    def _init_urls(self):
        self.department_api_url = "https://10.11.22.81:28701/apiaccess/api/SingleDataFactotyWebHttp/GetDeptInfoWeb"
        self.area_info_api_url = "https://10.11.22.81:28701/apiaccess/api/syg/SingleDataFactotyWebHttp/GetAreaInfoWeb"
        self.person_info_api_url = "https://10.11.22.81:28701/apiaccess/api/SingleDataFactotyWebHttp/GetPeopleInfoWebBeiXiang"
        self.car_info_api_url = "https://10.11.22.81:28701/apiaccess/api/SingleDataFactotyWebHttp/getCarInfoWeb"
        self.work_type_api_url = "https://10.11.22.81:28701/apiaccess/api/rydw_getWorkerTypeWeb_n"

        self.person_real_api_url = "https://10.11.22.80:38443/apiaccess/api/syg/SingleDataFactotyWebHttp/getLocationWeb"
        self.person_history_api_rul = "https://10.11.22.81:28701/apiaccess/api/SingleDataFactotyWebHttp/getHistoryLocationWeb"
        self.car_real_api_url = "https://10.11.22.81:28701/apiaccess/api/SingleDataFactotyWebHttp/getCarLocationWeb"
        self.car_history_api_url = "https://10.11.22.81:28701/apiaccess/api/getHistoryLocationWebByDate"

        self.up_down_api_url = "https://10.11.22.81:28701/apiaccess/api/rydw_getHisInOutMineWeb_for_AI"
        self.near_loaction_api_url = "https://10.11.22.81:28701/apiaccess/api/rydw_getLocationDirectionWeb_n"
        
        #初始化地点名
    def _init_station_names(self, force=False):
        """
        初始化 self.station_names，若其为空，则从数据库查询并赋值。
        """
        if time.time() - self.station_names_time > 300 or force:
            self.station_names_time = time.time()
            query = GET_REALTIME_STATION_HEAD_INFO_SQL
            rows = self.client.query(query).result_rows
            self.station_names = {
                row[0]: {
                    'name': row[1],
                    'type': row[2]
                }
                for row in rows
            }

    # ==================== 4. 内部辅助逻辑 ====================
    def parse_time(self, t: str):
        """统一时间格式"""
        t = t.replace("T", " ")
        return datetime.strptime(t, "%Y-%m-%d %H:%M:%S")

    def fetch_in_out_mine_records(self, begin_time: str, end_time: str, name: str) -> dict:
        """
        查询某人员某时间段内的入井记录（通过 up_down_api_url 接口）

        参数:
            begin_time: 开始时间，格式如 "2026-04-22 00:00:00"
            end_time: 结束时间，格式如 "2026-04-23 00:00:00"
            name: 人员卡号，字符串

        返回:
            dict, 格式同接口返回
        """

        url = self.up_down_api_url
        payload = {
            "beginTime": begin_time,
            "endTime": end_time,
            "卡号": ''
        }
        headers = {
            "Content-Type": "application/json"
        }
        try:
            response = requests.post(url, data=json.dumps(payload), headers=headers, timeout=120, verify=False)
            text = response.content.decode("utf-8-sig")
            result = json.loads(text).get("data", []) if response.status_code == 200 else []
            for res in result:
                user_name = res.get('UserName')
                code_sender_address = res.get('CodeSenderAddress', '')
                if user_name is not None:
                    # 更优雅地拼接，避免None问题，并 strip尾部多余下划线
                    res['UserName'] = f"{user_name}_{code_sender_address}".rstrip('_')
     
            return result
        except Exception as e:
            logger.error(f"fetch_in_out_mine_records failed: {e}")
            return []



    def classify_segments_by_inout(
            self,
            persons_dict,
            inout_records,
            tolerance_sec=120
    ):
        """
        按照入井记录对分日期的 persons_dict 的 segments 分组

        persons_dict:
        {
            station_date:{
                person_key:{
                    "工作轨迹记录":[...],
                    "姓名": ...,
                    "卡号": ...,
                    "工种": ...,
                    "班次": ...,
                    "部门": ...,
                    "职位": ...,
                    "工作": ...,
                }
            }
        }
        """

      

        result = defaultdict(dict)

        if not persons_dict:
            return {}

        tol = timedelta(seconds=tolerance_sec)

        # =========================
        # 1. 预处理 inout 索引和去重
        # =========================
        # 合并同一进出井的record（进/出井时间都一致则视为同一条record）

        def inout_key(r):
            return (r.get('InTime', ""), r.get('OutTime', ""))


        filtered_inout_records = []


        for r in inout_records:
            filtered_inout_records.append(r)

        inout_map = defaultdict(list)
        inout_date_map = defaultdict(list)

        for r in filtered_inout_records:
            date = r.get("InTime", "")[:10]
            date2 = r.get("OutTime", "")[:10]
            username = str(r.get("UserName", ""))

            # 时间提前解析
            try:
                in_time = self.parse_time(
                    r["InTime"][:19].replace("+08:00", "")
                )
                out_raw = (
                        r.get("OutTime")
                        or r.get("mainStationTime")
                )
                out_time = self.parse_time(
                    out_raw[:19].replace("+08:00", "")
                )
                r["_in_dt"] = in_time
                r["_out_dt"] = out_time
            except Exception as e:
                print(traceback.format_exc())
                continue
            inout_map[(date, username)].append(r)
            inout_map[(date2, username)].append(r)
            inout_date_map[date].append(r)
            inout_date_map[date2].append(r)

        # =========================
        # 主循环
        # =========================

        for station_date, person_segs in persons_dict.items():
            station_date = str(station_date)
            for person_key, info in person_segs.items():
                segments = info.get("工作轨迹记录", [])

                # 在结果中保留一次人员信息
                person_base_info = {
                    "姓名": info.get("姓名", ""),
                    "卡号": info.get("卡号", ""),
                    "工种": info.get("工种", ""),
                    "班次": info.get("班次", ""),
                    "部门": info.get("部门", ""),
                    "职位": info.get("职位", ""),
                    "工作": info.get("工作", "")
                }

                if not segments:
                    result[station_date][person_key] = {
                        **person_base_info,
                        "轨迹组合": []
                    }
                    continue

                # =========================
                # 获取 inout
                # =========================

                inout_filtered = inout_map.get(
                    (station_date, person_key),
                    []
                )
                if not inout_filtered:
                    # num += 1
                    print(station_date, person_key, f'---------')
                    inout_filtered = inout_date_map.get(
                        station_date,
                        []
                    )

                # =========================
                # segment预处理
                # =========================

                segments_sorted = sorted(
                    segments,
                    key=lambda x: x.get(
                        "轨迹开始时间",
                        ""
                    )
                )
                segment_objs = []
                for seg in segments_sorted:
                    try:
                        s = seg.copy()
                        s["_start"] = self.parse_time(
                            seg["轨迹开始时间"][:19]
                            .replace("+08:00", "")
                        )
                        s["_end"] = self.parse_time(
                            seg["轨迹结束时间"][:19]
                            .replace("+08:00", "")
                        )
                        segment_objs.append(s)
                    except:
                        continue

                if not segment_objs:
                    result[station_date][person_key] = {
                        **person_base_info,
                        "轨迹组合": []
                    }
                    continue

                starts = [
                    x["_start"]
                    for x in segment_objs
                ]
                groups = []
                seg_flags = [False] * len(segment_objs)
                any_grouped = False

                # =========================
                # 无inout情况
                # =========================
                if not inout_filtered:

                    # 删除 _start 和 _end 字段
                    for seg in segment_objs:
                        seg.pop("_start", None)
                        seg.pop("_end", None)
                    first = segment_objs[0]
                    
                    groups.append({
                        "入井时间": first["轨迹开始时间"],
                        "出井时间": None,
                        "入井地点": first.get("主站名称"),
                        "出井地点": None,
                        "入井时长": None,
                        "单次入井井下轨迹段数": len(segment_objs),
                        "具体轨迹变化": segment_objs,
                    })
                    result[station_date][person_key] = {
                        **person_base_info,
                        "轨迹组合": groups
                    }
                    continue

                # =========================
                # 按record分组，合并同样进出井时间
                # =========================
                used_inout_keys = set()
                for idx_r, record in enumerate(inout_filtered):
                    rec_inout_k = inout_key(record)
                    if rec_inout_k in used_inout_keys:
                        continue
                    used_inout_keys.add(rec_inout_k)

                    in_time = record["_in_dt"]
                    out_time = record["_out_dt"]

                    left_bound = in_time - tol
                    right_bound = out_time + tol

                    grouped_indices = []
                    left_idx = bisect_left(
                        starts,
                        left_bound
                    )

                    for idx in range(
                            left_idx,
                            len(segment_objs)
                    ):
                        seg = segment_objs[idx]
                        # 超出右边界提前退出
                        if seg["_start"] > right_bound:
                            break
                        if (
                                seg["_end"] >= left_bound
                                and
                                seg["_start"] <= right_bound
                        ):
                            grouped_indices.append(
                                idx
                            )
                            seg_flags[idx] = True

                    if not grouped_indices:
                        continue
                    any_grouped = True
                    group_segs = [
                        segment_objs[i].copy()
                        for i in grouped_indices
                    ]
                    # 删除 _start 和 _end 字段
                    for seg in group_segs:
                        seg.pop("_start", None)
                        seg.pop("_end", None)
                    #修正首尾
                    if group_segs:
                        group_segs[-1][
                            "轨迹结束时间"
                        ] = record.get(
                            'OutTime'
                        )
                        if record.get(
                                "OutWellPlace"
                        ):
                            group_segs[-1][
                                "主站名称"
                            ] = record[
                                "OutWellPlace"
                            ]
                        if idx_r > 0:
                            group_segs[0][
                                "轨迹开始时间"
                            ] = record[
                                'InTime'
                            ]
                            group_segs[0][
                                "主站名称"
                            ] = record.get(
                                "InWellPlace"
                            )
                    groups.append({
                        "入井时间": record.get("InTime"),
                        "出井时间": record.get("OutTime"),
                        "入井地点": record.get("InWellPlace"),
                        "出井地点": record.get("OutWellPlace"),
                        "入井时长": record.get("ContinueTime"),
                        "单次入井井下轨迹段数": len(group_segs),
                        "具体轨迹变化": group_segs
                    })

                # =========================
                # 一个都没匹配
                # =========================
                if not any_grouped:
                    for seg in segment_objs:
                        seg.pop("_start", None)
                        seg.pop("_end", None)
                    first = segment_objs[0]
                    groups.append({
                        "入井时间": first["轨迹开始时间"],
                        "出井时间": None,
                        "入井地点": first.get("主站名称"),
                        "出井地点": None,
                        "入井时长": None,
                        "单次入井井下轨迹段数": len(segment_objs),
                        "具体轨迹变化": segment_objs
                    })

                # =========================
                # 未匹配segment
                # =========================
                ungrouped = [
                    segment_objs[i]
                    for i, flag in enumerate(
                        seg_flags
                    )
                    if not flag
                ]
                if ungrouped:
                    for seg in ungrouped:
                        seg.pop("_start", None)
                        seg.pop("_end", None)
                    first = ungrouped[0]
                    groups.append({
                        "入井时间": first["轨迹开始时间"],
                        "出井时间": None,
                        "入井地点": first.get("主站名称"),
                        "出井地点": None,
                        "入井时长": None,
                        "单次入井井下轨迹段数": len(ungrouped),
                        "具体轨迹变化": ungrouped
                    })

                result[station_date][person_key] = {
                    **person_base_info,
                    "轨迹组合": groups
                }

        return dict(result)


    def get_persons_by_filters(
            self,
            start_time: str = None,
            end_time: str = None,
    ):
        """
        精简版：仅支持 start_time, end_time 两个参数。其他筛选条件全部取消。
        如果不传 start_time/end_time，默认查询当日数据。
        """

        from datetime import datetime, time
        # 1. 处理默认日期逻辑：如果不传日期，设定为当日全天
        now = datetime.now()
        if not start_time:
            start_time = datetime.combine(now.date(), time.min).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        if not end_time:
            end_time = datetime.combine(now.date(), time.max).strftime(
                "%Y-%m-%d %H:%M:%S"
            )
        # 2. 基础时间条件
        where_conditions = [
            "UPDATE_TIME >= %(start_time)s",
            "UPDATE_TIME < %(end_time)s",
        ]
        params = {"start_time": start_time, "end_time": end_time}
        where_sql = " AND ".join(where_conditions)

        segment_query = f"""
        WITH base AS (
            SELECT
                NAME,
                AREANAME,
                CLASSTIMENAME,
                MAINSTATIONID,
                MAINSTATIONDISTANCE,
                DEPARTMENT,
                WORKTYPE,
                JOB,
                CARDID,
                ELECTRICITY,
                SUBSTATIONID,
                SUBSTATIONDISTANCE,
                toDateTime(MAINSTATIONTIME) AS m_time,
                toDateTime(UPDATE_TIME) AS u_time,
                toDate(MAINSTATIONTIME) AS stationDate,
                DUTYNAME
            FROM PS.HISTORY_PERSONNEL_LOCATION
            WHERE {where_sql}
        ),

        flagged AS (
            SELECT
                *,
                lagInFrame(MAINSTATIONID)
                    OVER (
                        PARTITION BY NAME, CARDID
                        ORDER BY u_time
                    ) AS prev_station,
                lagInFrame(stationDate)
                    OVER (
                        PARTITION BY NAME, CARDID
                        ORDER BY u_time
                    ) AS prev_date
            FROM base
        ),

        grouped AS (
            SELECT
                *,
                sum(
                    if(
                        MAINSTATIONID != prev_station
                        OR stationDate != prev_date,
                        1,
                        0
                    )
                ) OVER (
                    PARTITION BY NAME, CARDID
                    ORDER BY u_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS segment_id
            FROM flagged
        )

        SELECT
            any(NAME) as name,
            any(AREANAME) as areaName,
            any(CLASSTIMENAME) as classTimeName,
            any(MAINSTATIONID) as mainStationId,
            any(stationDate) as stationDate,

            min(m_time) as enterTime,
            max(m_time) as leaveTime,

            count(*) as count,

            dateDiff('second', min(m_time), max(m_time)) as duration,

            groupUniqArray(100)(MAINSTATIONDISTANCE) as mainStationDistance,

            any(DEPARTMENT) as department,
            any(WORKTYPE) as workType,
            any(JOB) as job,
            any(CARDID) as cardid,

            groupArray(1000)(u_time) as updateTimes,

            any(ELECTRICITY) as electricity,
            any(SUBSTATIONID) as subStationId,
            any(SUBSTATIONDISTANCE) as subStationDistance,
            any(DUTYNAME) as dutyname

        FROM grouped

        GROUP BY
            NAME,
            CARDID,
            segment_id

        ORDER BY min(u_time) ASC
        """

        try:
            result = self.client.query(segment_query, parameters=params)

            persons_dict = {}
            total_segments = 0

            for row in result.result_rows:
                name = row[0]
                cardid = row[13]
                station_date = str(row[4])  # 使用 stationDate 作为第一层 key

                person_key = f"{name}_{cardid}"
                if station_date not in persons_dict:
                    persons_dict[station_date] = {}

                if person_key not in persons_dict[station_date]:
                    persons_dict[station_date][person_key] = {
                        "姓名": name,
                        "卡号": cardid,
                        "工种": row[11],
                        "班次": row[2],
                        "部门": row[10],
                        "职位": row[18],
                        "工作": row[12],
                        "工作轨迹记录": [],
                    }

                # 记录分段的出入信息
                record = {
                    "区域名称": row[1],
                    "轨迹开始时间": str(row[5]).replace('+08:00', ''),
                    "轨迹结束时间": str(row[6]).replace('+08:00', ''),
                    "电量": row[15],
                    "主站名称": self.station_names.get(int(row[3]), {}).get("name", row[3]),
                    "距离主站距离/m": row[9],
                    "分站名称": self.station_names.get(int(row[16]), {}).get("name", row[16]) if row[16] else row[16],
                    "距离分站距离/m": row[17],
                    "变化次数": row[7],
                    "停留时长/s": row[8],
                    # "变化时间段": list({str(x) for x in row[14]}) if row[14] else [],
                }
                persons_dict[station_date][person_key]["工作轨迹记录"].append(record)
                total_segments += 1

            inout_records = self.fetch_in_out_mine_records(start_time, end_time, '')
            outs = self.classify_segments_by_inout(persons_dict,inout_records)
            return {
                "total": total_segments,
                "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "persons": outs,
            }
     

        except Exception as e:
            import traceback
            print(f"查询失败: {e}\n{traceback.format_exc()}")
            return {"total": 0, "update_time": "", "persons": {}, "error": str(e)}


      # ==================== 3. 封装cache功能 =====================
    def get_person_infos_daytype_with_cache(
            self,
            person_name_filters: Union[List[str], str, None] = None,
            department_filters: Union[List[str], str, None] = None,
            classtype_filters: Union[List[str], str, None] = None,
            worktype_filters: Optional[Dict[str, tuple]] = None,
            duty_filters: Union[List[str], str, None] = None,
            electricity_filters: Optional[Dict[str, tuple]] = None,
            station_filters: Optional[Dict[str, tuple]] = None,
            area_filters: Optional[Dict[str, tuple]] = None,
            in_places_filters: Optional[Dict[str, tuple]] = None,
            out_places_filters: Optional[Dict[str, tuple]] = None,
            start_date: Union[str, datetime, None] = None,
            end_date: Union[str, datetime, None] = None,
    ) -> Dict:
        """
        简化缓存，仅以day_str为key。所有过滤通过函数处理，不在缓存中做过滤。
        每日数据，适配条件过滤。
        """
        import sqlite3
        from datetime import datetime, timedelta
        import json
        import os

        # SQLite缓存数据库文件，仅以day_str为key
        db_path = os.path.join(os.path.dirname(__file__), "person_analysis_cache.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS person_atom_cache (
                day_str TEXT PRIMARY KEY,
                result_json TEXT
            )
        ''')
        conn.commit()

        # 日期范围准备
        def parse_dt(dt):
            if not dt:
                return datetime.now()
            if isinstance(dt, datetime):
                return dt
            if isinstance(dt, str):
                try:
                    return datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
                except Exception:
                    # 容错 针对 "YYYY-MM-DD"
                    return datetime.strptime(dt, "%Y-%m-%d")
            return datetime.now()

        s_dt = parse_dt(start_date) if start_date else datetime.now()
        e_dt = parse_dt(end_date) if end_date else s_dt
        # 若 end_date 是 00:00:00 形态, 则处理为前一天23:59:59，防止漏掉天
        if isinstance(end_date, str):
            dt_parts = end_date.strip().split(' ')
            if len(dt_parts) == 2 and dt_parts[1] in ("00:00:00", "00:00:00.000"):
                try:
                    day = datetime.strptime(dt_parts[0], "%Y-%m-%d").date()
                    prev_day = day - timedelta(days=1)
                    end_date_new = f"{prev_day.strftime('%Y-%m-%d')} 23:59:59"
                    e_dt = parse_dt(end_date_new)
                except Exception:
                    pass

        now = datetime.now()
        if e_dt > now:
            e_dt = now
        if s_dt > e_dt:
            s_dt = e_dt

        req_dates = []
        curr_d = s_dt.date()
        while curr_d <= e_dt.date():
            req_dates.append(curr_d.strftime("%Y-%m-%d"))
            curr_d += timedelta(days=1)
        today_str = datetime.now().strftime("%Y-%m-%d")

        missed_days = []

        final_output = {}  # day -> {person_key: data}

        # 1. 先读取非今日的缓存
        for day in req_dates:
            if day == today_str:
                missed_days.append(day)
                continue
            cursor.execute('''
                SELECT result_json FROM person_atom_cache
                WHERE day_str=?
            ''', (day,))
            rows = cursor.fetchall()
            if rows and len(rows) > 0:
                try:
                    day_data = json.loads(rows[0][0])
                    final_output[day] = day_data
                except Exception:
                    final_output[day] = {}
            else:
                missed_days.append(day)

        # 2. 对未命中的天（包括今天），查库并写入cache
        print(f'missed_days-----{missed_days}')
        for day in missed_days:

            fetch_start = f"{day} 00:00:00"
            fetch_end = f"{day} 23:59:59"
            fetch_result = self.get_persons_by_filters(start_time=fetch_start, end_time=fetch_end)

            # cache 全量数据，不走过滤
            day_data = {}
            if "persons" in fetch_result:
                for k, persons in fetch_result["persons"].items():
                    for pk, pdata in persons.items():
                        day_data[pk] = pdata

            # 写入cache
            cursor.execute('''
                INSERT OR REPLACE INTO person_atom_cache
                (day_str, result_json)
                VALUES (?, ?)
            ''', (day, json.dumps(day_data, ensure_ascii=False, default=str)))
            conn.commit()

            final_output[day] = day_data


        # 4. 按每日输出过滤后的数据
        filtered_output = {}
        for day, day_dict in final_output.items():
            data_out = self.person_filter(
                day_dict,
                person_name_filters=person_name_filters,
                department_filters=department_filters,
                classtype_filters=classtype_filters,
                worktype_filters=worktype_filters,
                duty_filters=duty_filters,
                electricity_filters=electricity_filters,
                station_filters=station_filters,
                area_filters=area_filters,
                in_places_filters=in_places_filters,
                out_places_filters=out_places_filters,
            )
        
            if len(data_out) > 0:
                filtered_output[day] = data_out

        output = {"每日数据": filtered_output, "总共天数": len(filtered_output)}
        conn.close()
        return output

    
    
    # 3. 所有过滤全部在读取时做
    def person_filter(
        self,
        pdata,
        person_name_filters: Union[List[str], str, None] = None,
        department_filters: Union[List[str], str, None] = None,
        classtype_filters: Union[List[str], str, None] = None,
        worktype_filters: Optional[Dict[str, tuple]] = None,
        duty_filters: Union[List[str], str, None] = None,
        electricity_filters: Optional[Dict[str, tuple]] = None,
        station_filters: Optional[Dict[str, tuple]] = None,
        area_filters: Optional[Dict[str, tuple]] = None,
        in_places_filters: Optional[Dict[str, tuple]] = None,
        out_places_filters: Optional[Dict[str, tuple]] = None,
    ):


        
 

        result = {}
        person_name_filters = normalize(person_name_filters)
        department_filters = normalize(department_filters)
        classtype_filters = normalize(classtype_filters)
        duty_filters = normalize(duty_filters)

        for person_key, person in pdata.items():
            person = deepcopy(person)

            # ===== 基础字段过滤 =====
            if person_name_filters:
                # 增加模糊匹配
                if not fuzzy_match(person["姓名"], person_name_filters,60):
                    continue
            if department_filters:
                if not fuzzy_match(person["部门"], department_filters):
                    continue
            if classtype_filters:
                if not fuzzy_match(person["班次"], classtype_filters):
                    continue
            if worktype_filters:
                if not fuzzy_match(person["工种"], worktype_filters):
                    continue
                
            if duty_filters:
                if not fuzzy_match(person["职位"], duty_filters):
                    continue

            # ===== segments_grouped内部过滤 =====
            has_segments = False
            use_segments_grouped = []

            for index,segments in enumerate(person["轨迹组合"]):
                keep = True
                # 入场地点(subStation)
                if keep and in_places_filters:
                    value = segments.get("入井地点")
                    if not fuzzy_match(value, in_places_filters):
                        keep = False

                # 出场地点(mainStation)
                if keep and out_places_filters:
                    value = segments.get("出井地点")
                    if not fuzzy_match(value, out_places_filters):
                        keep = False
                filtered_records = []
                for record in segments["具体轨迹变化"]:
                    keep = True
                    # electricity
                    if keep and electricity_filters:
                        value = record.get("电量")
                        if not fuzzy_match(value, electricity_filters):
                            keep = False

                    # station
                    if keep and station_filters:
                        value1 = record.get("主站名称")
                        value2 = record.get("分站名称")

                        if not fuzzy_match(value1, station_filters,70)  and  not fuzzy_match(value2, station_filters,70):
                            keep = False

                    # area
                    if keep and area_filters:
                        value = record.get("区域名称")
                        if not fuzzy_match(value, area_filters):
                            keep = False
                    if keep:
                        filtered_records.append(record)
                        
                        
                if filtered_records:
                    segments["具体轨迹变化"] = filtered_records
                    has_segments = True
                    use_segments_grouped.append(segments)
                else:
                    del segments["具体轨迹变化"]

                    # del person["轨迹组合"][index]

            if has_segments:
                person["轨迹组合"] = use_segments_grouped
                result[person_key] = person


        return result

if __name__ == "__main__":
  

    client = clickhouse_connect.get_client(
        host="10.11.22.80", port=9120, username="nethouse",
        password="CGC%EVXr.ET10Y_N", secure=True, verify=False,autogenerate_session_id=False
    )
    person_util = PersonBase(client)
    # start_time="2026-06-13 00:00:00",
    # end_time="2026-06-16 00:00:00",
    # # 测试 get_persons_by_filters 方法
    # data = person_util.get_persons_by_filters(start_time=start_time, end_time=end_time)
    # print("=== get_persons_by_filters 输出 ===")
    # pprint(data)

    # 增加对 get_person_infos_daytype_with_cache 的测试
    print("\n=== get_person_infos_daytype_with_cache 输出 ===")
    daytype_data = person_util.get_person_infos_daytype_with_cache(
        start_date="2026-06-15 00:00:00",
        end_date="2026-06-16 00:00:00",
        person_name_filters=None,
        department_filters=None,
        classtype_filters=None,
        worktype_filters=None,
        duty_filters=None,
        electricity_filters=None,
        station_filters="辅运4联巷16号",
        area_filters=None,
        in_places_filters=None,
        out_places_filters=None,
    )
    pprint(daytype_data)