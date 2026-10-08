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

import datetime

import json
import logging

import requests
import clickhouse_connect
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union

import sqlite3
from datetime import datetime, timedelta
import json
import os
import sys
import os
import threading
from collections import defaultdict
from bisect import bisect_left
import concurrent.futures
from copy import deepcopy


sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from person_sqls import (
    GET_PERSON_LATEST_SQL,
    GET_PERSON_TRAJECTORY_SQL,
    GET_TODAY_PERSONS_SQL,
    GET_REALTIME_STATION_HEAD_INFO_SQL,
    GET_AREA_LIMITS_SQL, GET_TODAY_CARS_SQL
)
from base_utils import  check_numeric_condition,normalize,fuzzy_match,generate_statistics



class PersonBase:
    """
    人员相关功能的基类，供工具与utils共用。
    主要封装通用的人员信息方法和属性。实际业务类可继承它。
    """

    def __init__(self, client, logger=None):
        # 增加 self.logger，logger不能为空则创建默认logger
        if logger is None:
            self.logger = logging.getLogger("car_bases")
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
            if not self.logger.handlers:
                self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        else:
            self.logger = logger
       
        self.client = client
        self.station_names_time = 0
        self.station_names = {}
        self._init_station_names()
        self._init_urls()
        # self.start_auto_analysis_thread()

        # 初始化相关接口
    def _init_urls(self):
        try:
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
            self.logger.info("_init_urls successful.")
        except Exception as e:
            self.logger.error(f"_init_urls failed: {e} \n{traceback.format_exc()}")

        #初始化地点名
    def _init_station_names(self, force=False):
        """
        初始化 self.station_names，若其为空，则从数据库查询并赋值。
        """
        import time
        try:
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
                self.logger.info("_init_station_names refreshed station_names")
        except Exception as e:
            self.logger.error(f"_init_station_names failed: {e} \n{traceback.format_exc()}")

    # ==================== 4. 内部辅助逻辑 ====================
    def parse_time(self, t: str):
        """统一时间格式"""
        t = t.replace("T", " ")
        try:
            return datetime.strptime(t, "%Y-%m-%d %H:%M:%S")
        except Exception as e:
            self.logger.error(f"parse_time failed: {t} | {e} \n{traceback.format_exc()}")
            return None

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
        import requests
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
            self.logger.info(f"fetch_in_out_mine_records got {len(result)} records for {begin_time} - {end_time}")
            return result
        except Exception as e:
            self.logger.error(f"fetch_in_out_mine_records failed: {e} \n{traceback.format_exc()}")
            return []

    def seconds_to_time_str(self,seconds):
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60

        return f"{hours}时{minutes}分{seconds}秒"
    
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
        try:
            if not persons_dict:
                return {}

            tol = timedelta(seconds=tolerance_sec)

            # =========================
            # 1. 预处理 inout 索引和去重
            # =========================
            # 合并同一进出井的record（进/出井时间都一致则视为同一条record）

            def inout_key(r):
                return (r.get('InTime', ""), r.get('OutTime', ""),r.get('UserNo', ""))

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
                    self.logger.error(f"classify_segments_by_inout time parse fail: {e} \n{traceback.format_exc()}\n")
                    continue
                # if 'username' != "高相波":
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
                        # 有可能只有一个
                        **(
                            {"职位": info.get("职位", "")}
                            if (info.get("职位", "") == info.get("工作", "")) or not info.get("工作", "")
                            else {"职位": info.get("职位", ""), "工作": info.get("工作", "")}
                        )
                 
                    }

                    if not segments:
                        result[station_date][person_key] = {
                            **person_base_info,
                            "出入井记录": []
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
                        self.logger.info(f"classify_segments_by_inout no inout for {station_date} {person_key}")
                        inout_filtered = []
                  
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
                        except Exception as e:
                            self.logger.error(f"classify_segments_by_inout segment parse fail: {e} \n{traceback.format_exc()}")
                            continue

                    if not segment_objs:
                        result[station_date][person_key] = {
                            **person_base_info,
                            "出入井记录": []
                        }
                        continue

                    starts = [
                        x["_start"]
                        for x in segment_objs
                    ]
                    ends = [
                        x["_end"]
                        for x in segment_objs
                    ]
                    groups = []
                    seg_flags = [False] * len(segment_objs)
                    any_grouped = False

                    # =========================
                    # 无inout情况
                    # =========================
                    if not inout_filtered:
                        for seg in segment_objs:
                            seg.pop("_start", None)
                            seg.pop("_end", None)
                        first = segment_objs[0]
                        station = first['主站名称']
                        time_dur =  int((ends[-1]-starts[0]).total_seconds())
                        time_str = self.seconds_to_time_str(time_dur)
                        groups.append({
                            "入井时间": starts[0].strftime("%Y-%m-%d %H:%M:%S"),
                            "出井时间": None,
                            "入井地点": station,
                            "出井地点": None,
                            "入井时长": time_str,
                            "入井时长(秒)": time_dur,
                            "单次入井井下轨迹段数": len(segment_objs),
                            "具体轨迹变化": segment_objs,
                        })
                        result[station_date][person_key] = {
                            **person_base_info,
                            "出入井记录": groups
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
                            # 将 '入井时长' 字符串(如'9时15分17秒')转化为总秒数
                            "入井时长(秒)": (
                                lambda t: (
                                    int(t.split("时")[0]) * 3600 if "时" in t else 0
                                ) + (
                                    int(t.split("时")[1].split("分")[0]) * 60 if "分" in t else 0
                                ) + (
                                    int(t.split("分")[-1].split("秒")[0]) if "秒" in t else 0
                                ) if isinstance(t, str) and t else None
                            )(record.get("ContinueTime")),

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
                        "出入井记录": groups
                    }
            self.logger.info(f'classify_segments_by_inout completed for {len(result)} days.')
            return dict(result)
        except Exception as e:
            self.logger.error(f"classify_segments_by_inout failed: {e} \n{traceback.format_exc()}\n")
            return {}

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
        try:
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
                "toDateTime(MAINSTATIONTIME) >= %(start_time)s",
                "toDateTime(MAINSTATIONTIME) < %(end_time)s",
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
                        # 如果职位和工作一样，只保留一个（职位优先）
                        "职位": row[18] if row[18] != row[12] else row[18],
                        **({} if row[18] == row[12] else {"工作": row[12]}),
                 
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
            n = 0
            names = []
            for record in inout_records:
                if record['OutTime']=='' and len(record['CodeSenderAddress']) ==4 and 'CarTypeName' not in record:
                    # print(record)
                    names.append(record['UserNo'])
                    n+=1
                    
         
            outs = self.classify_segments_by_inout(persons_dict, inout_records)
            aa = []
            for key,day in outs.items():
                for name,out in day.items():
                    if '李治军' in name:
                        aa.append(out)
            self.logger.info(f"get_persons_by_filters total_segments: {total_segments} for {start_time} - {end_time}")
            return {
                "total": total_segments,
                "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "persons": outs,
            }
        except Exception as e:
            import traceback
            self.logger.error(f"get_persons_by_filters failed: {e} \n{traceback.format_exc()}\n")
            return {"total": 0, "update_time": "", "persons": {}, "error": str(e)}

    def start_auto_analysis_thread(self):
        """
        启动一个后台线程，定时执行 _run_daily_auto_analysis
        """
        try:
            thread = threading.Thread(target=self._run_daily_auto_analysis, daemon=True)
            thread.start()
            return thread
        except Exception as e:
            if hasattr(self, 'logger'):
                self.logger.error(f"start_auto_analysis_thread启动失败: {traceback.format_exc()}")
            return None
    
    def _run_daily_auto_analysis(self):
        """
        每天定时自动分析前一月全量数据，并用print_tongfeng_today_with_cache和calc_gonglv_energy_with_cache预热缓存。
        """
        while True:
            now = datetime.now()
            next_time = (now + timedelta(days=1)).replace(hour=2, minute=0, second=0, microsecond=0)
            wait_seconds = (next_time - now).total_seconds()
            if hasattr(self, '_ran_auto_analysis'):
                if wait_seconds > 0:
                    time.sleep(wait_seconds)
            else:
                self._ran_auto_analysis = True
            try:
                end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                start_date = end_date - timedelta(days=30)
                try:
                    self.get_person_infos_daytype_with_cache(
                        start_date=start_date,
                        end_date=end_date,
                    )
                except Exception as e:
                    self.logger.info(f"自动分析: print_tongfeng_today_with_cache 异常: {e}")
            except Exception as e:
                self.logger.info(f"自动分析: 总体异常: {e}")
                
      # ==================== 3. 封装cache功能 =====================
    def get_person_infos_daytype_with_cache(
        self,
        person_name_filters: Union[List[str], str, None] = None,
        department_filters: Union[List[str], str, None] = None,
        classtype_filters: Union[List[str], str, None] = None,
        worktype_filters: Union[List[str], str, None] = None,
        duty_filters: Union[List[str], str, None] = None,
        electricity_filters: Union[List[str], str, None] = None,
        station_filters: Union[List[str], str, None] = None,
        area_filters: Union[List[str], str, None] = None,
        in_places_filters: Union[List[str], str, None] = None,
        out_places_filters: Union[List[str], str, None] = None,
        numeric_filters: Optional[Dict[str, Dict]] = None,
        statistics_filter: Union[List[str], str, None] = None,
        start_date: Union[str, datetime, None] = None,
        end_date: Union[str, datetime, None] = None,
    ) -> Dict:
        """
        简化缓存，仅以day_str为key。所有过滤通过函数处理，不在缓存中做过滤。
        每日数据，适配条件过滤。
        优化：当查询范围 <= 1天时，跳过缓存直接查库。
        """
        try:
            # SQLite缓存数据库文件
            db_path = os.path.join(os.path.dirname(__file__), "person_analysis_cache.db")
            
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
                        return datetime.strptime(dt, "%Y-%m-%d")
                return datetime.now()

            s_dt = parse_dt(start_date) if start_date else datetime.now()
            e_dt = parse_dt(end_date) if end_date else s_dt
            
            # 若 end_date 是 00:00:00 形态, 则处理为前一天23:59:59
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
                
            # ========== 核心优化：判断是否使用缓存 ==========
            # 如果请求的天数 <= 1，则不走缓存，直接查库
            use_cache = len(req_dates) > 1
            # ================================================

            final_output = {}  # day -> {person_key: data}
            missed_days = []

            if use_cache:
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute('''
                    CREATE TABLE IF NOT EXISTS person_atom_cache (
                        day_str TEXT PRIMARY KEY,
                        result_json TEXT
                    )
                ''')
                conn.commit()

                today_str = datetime.now().strftime("%Y-%m-%d")

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
                            self.logger.error(f"cache-load failed for {day}: JSON decode error")
                    else:
                        missed_days.append(day)

                # 2. 对未命中的天（包括今天），查库并写入cache
                self.logger.info(f'missed_days-----{missed_days}')
                for day in missed_days:
                    fetch_start = f"{day} 00:00:00"
                    fetch_end = f"{day} 23:59:59"
                    try:
                        fetch_result = self.get_persons_by_filters(start_time=fetch_start, end_time=fetch_end)
                    except Exception as e:
                        self.logger.error(f"fetch_result error for {day}: {e} \n{traceback.format_exc()}")
                        fetch_result = {}

                    day_data = {}
                    if "persons" in fetch_result:
                        for k, persons in fetch_result["persons"].items():
                            for pk, pdata in persons.items():
                                day_data[pk] = pdata

                    try:
                        cursor.execute('''
                            INSERT OR REPLACE INTO person_atom_cache
                            (day_str, result_json)
                            VALUES (?, ?)
                        ''', (day, json.dumps(day_data, ensure_ascii=False, default=str)))
                        conn.commit()
                        self.logger.info(f"cache updated for day {day}")
                    except Exception as e:
                        self.logger.error(f"cache-write failed for day {day}: {e} \n{traceback.format_exc()}")

                    final_output[day] = day_data
            else:
                # ========== 不走缓存，直接查库 ==========
                self.logger.info(f"Query range <= 1 day ({len(req_dates)} day(s)), skipping cache.")
                for day in req_dates:
                    fetch_start = f"{day} 00:00:00"
                    fetch_end = f"{day} 23:59:59"
                    try:
                        fetch_result = self.get_persons_by_filters(start_time=fetch_start, end_time=fetch_end)
                    except Exception as e:
                        self.logger.error(f"fetch_result error for {day}: {e} \n{traceback.format_exc()}")
                        fetch_result = {}

                    day_data = {}
                    if "persons" in fetch_result:
                        for k, persons in fetch_result["persons"].items():
                            for pk, pdata in persons.items():
                                day_data[pk] = pdata
                    
                    final_output[day] = day_data
                # ========================================

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
                    numeric_filters=numeric_filters,
                    statistics_filter=statistics_filter
                )
            
                if len(data_out) > 0:
                    filtered_output[day] = data_out

            output = {"每日数据": filtered_output, "总共天数": len(filtered_output)}
            
            # 安全关闭连接（仅在使用了缓存时）
            if use_cache:
                try:
                    conn.close()
                except Exception:
                    pass
                    
            self.logger.info(f"get_person_infos_daytype_with_cache completed. Total {len(filtered_output)} days.")
            return output
            
        except Exception as e:
            self.logger.error(f"get_person_infos_daytype_with_cache failed: {e} \n{traceback.format_exc()}")
            return {}

    # 3. 所有过滤全部在读取时做 (多线程加速改进)
    def person_filter(
            self,
            pdata,
            person_name_filters: Union[List[str], str, None] = None,
            department_filters: Union[List[str], str, None] = None,
            classtype_filters: Union[List[str], str, None] = None,
            worktype_filters: Union[List[str], str, None] = None,
            duty_filters: Union[List[str], str, None] = None,
            electricity_filters: Union[List[str], str, None] = None,
            station_filters: Union[List[str], str, None] = None,
            area_filters: Union[List[str], str, None] = None,
            in_places_filters: Union[List[str], str, None] = None,
            out_places_filters: Union[List[str], str, None] = None,
            numeric_filters: Optional[Dict[str, Dict]] = None,  # 新增
            statistics_filter: Union[List[str], str, None] = None,
    ):
        try:
            person_name_filters = normalize(person_name_filters)
            department_filters = normalize(department_filters)
            classtype_filters = normalize(classtype_filters)
            duty_filters = normalize(duty_filters)

            # ==================== process_person ====================
            def process_person(person_item):
                person_key, person = person_item
                person = deepcopy(person)

                # ===== 基础字段过滤 =====
                if person_name_filters:
                    if not fuzzy_match(person.get("姓名", ""), person_name_filters, 60):
                        return None
                if department_filters:
                    if not fuzzy_match(person.get("部门", ""), department_filters):
                        return None
                if classtype_filters:
                    if not fuzzy_match(person.get("班次", ""), classtype_filters):
                        return None
                if worktype_filters:
                    if not fuzzy_match(person.get("工种", ""), worktype_filters):
                        return None
                if duty_filters:
                    if not fuzzy_match(person.get("职位", ""), duty_filters):
                        return None

                # ===== segments_grouped内部过滤 =====
                has_segments = False
                use_segments_grouped = []

                for segments in person.get("出入井记录", []):
                    keep = True

                    # 入井/出井地点过滤
                    if keep and in_places_filters:
                        if not fuzzy_match(segments.get("入井地点"), in_places_filters):
                            keep = False
                    if keep and out_places_filters:
                        if not fuzzy_match(segments.get("出井地点"), out_places_filters):
                            keep = False
                    if keep and numeric_filters:

                        for field_name, condition in numeric_filters.items():
                            rec_data = segments.get(field_name,-1)
                            if rec_data == -1: continue
                            need_keep = check_numeric_condition(rec_data, condition)
                            if not need_keep:
                                return None

                    if not keep:
                        continue

                    filtered_records = []
                    for record in segments.get("具体轨迹变化", []):
                        keep_inner = True

                        # === 原有过滤 ===
                        if keep_inner and electricity_filters:
                            if not fuzzy_match(record.get("电量"), electricity_filters):
                                keep_inner = False

                        if keep_inner and station_filters:
                            v1 = record.get("主站名称")
                            v2 = record.get("分站名称")
                            if not (fuzzy_match(v1, station_filters, 80) or fuzzy_match(v2, station_filters, 80)):
                                keep_inner = False

                        if keep_inner and area_filters:
                            if not fuzzy_match(record.get("区域名称"), area_filters):
                                keep_inner = False

                        # === 新增：数值过滤（核心）===
                        if keep_inner and numeric_filters:

                            for field_name, condition in numeric_filters.items():
                                rec_data = record.get(field_name,-1)
                                if rec_data == -1:
                                    continue
                                if isinstance(rec_data, list):
                                    if not any(check_numeric_condition(val, condition) for val in rec_data):
                                        keep_inner = False
                                        break
                                else:
                                    if not check_numeric_condition(rec_data, condition):
                                        keep_inner = False
                                        break

                        if keep_inner:
                            filtered_records.append(record)

                    # 如果该出入井记录还有有效轨迹，则保留
                    if filtered_records:
                        segments["具体轨迹变化"] = filtered_records
                        has_segments = True
                        use_segments_grouped.append(segments)
                    # else: 可选择删除空记录

                if has_segments:
                    person["出入井记录"] = use_segments_grouped
                    
                    return (person_key, person)
                else:
                    return None

            # ==================== 多线程执行 ====================
            result = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
                futures = [executor.submit(process_person, item) for item in pdata.items()]
                for f in concurrent.futures.as_completed(futures):
                    try:
                        res = f.result()
                        if res is not None:
                            person_key, person_val = res
                            result[person_key] = person_val
                    except Exception as e:
                        self.logger.error(f"person_filter process_person thread failed: {e} \n{traceback.format_exc()}")

            # ==================== 新增：如果需要统计 ====================
            if statistics_filter is not None and len(statistics_filter) > 0 :
                try:
                    stats = generate_statistics(result, statistics_filter)
                    self.logger.info(f"person_filter statistics generated for keys: {statistics_filter}")
                    return {
                        # "filtered_data": result,  # 过滤后的原始数据
                        "statistics": stats  # 统计结果
                    }
                except Exception as e:
                    self.logger.error(f"person_filter statistics fail: {e} \n{traceback.format_exc()}")
                    return {"statistics": {}}

            return result
        except Exception as e:
            self.logger.error(f"person_filter failed: {e} \n{traceback.format_exc()}")
            return {}



if __name__ == "__main__":
    from pprint import  pprint

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
    numeric_filters = {
        #
        # "距离主站距离/m": {
        #     "op": "<",
        #     "value": 150
        # },
        # "电量": {
        #     "op": "!=",
        #     "value": "正常"
        # },
        # "轨迹开始时间": {
        #     "op": "between",
        #     "value": ["2026-07-02 04:00:00", "2026-07-02 08:00:00"]
        # },
        # "轨迹结束时间": {
        #     "op": ">",
        #     "value": "2026-07-02 00:00:00"
        # },
        # '入井时长(秒)': {'op': '>', 'value': 14400}
        # "入井时间": {
        #      "op": "between",
        #     "value": ["2026-07-01 04:00:00", "2026-07-01 08:00:00"]
        # }
    }

    # 列表列出所有stats的key
    # stats_keys 来自 base_utils.py generate_statistics 返回值中的key，保持一致
    stats_keys = [
        "总人数",
        "人员列表_姓名_卡号_入井次数",
        "入井时长分布/人次",
        "入井时间段分布/人次",
        "出井时间段分布/人次",
        "入井地点分布/人次",
        "出井地点分布/人次",
        "区域分布/条",
        "主站分布/条",
        "分站分布/条",
        "站点停留时长分布/条",
        "部门分布/人",
        "职位分布/人",
        "工种分布/人",
        "班次分布/人",
    ]
    print("统计字段 keys:", stats_keys)
    statistics_filter_values = ["人员列表_姓名_卡号_入井次数","每小时人数统计/人", '入井时间段分布/人次', '出井时间段分布/人次','当前在井下人数']
    daytype_data = person_util.get_person_infos_daytype_with_cache(
        start_date="2026-08-13 00:00:00", end_date="2026-08-13 23:59:59",
        person_name_filters=None,
        department_filters=None,
        classtype_filters=None,
        worktype_filters=None,
        duty_filters=None,
        electricity_filters=None,
        station_filters=None,
        area_filters=None,
        in_places_filters=None,
        out_places_filters=None,
        # numeric_filters=numeric_filters,
        # statistics_filter = statistics_filter_values,  # 新增参数
    )
   
    daytype_json = json.dumps(daytype_data, ensure_ascii=False, indent=2)
    with open("history_person_data.txt", "w", encoding="utf-8") as f:
        f.write(daytype_json)
        print("全部数据已成功写入 history_person_data.txt")
    # print(daytype_json)
    print("json长度:", len(daytype_json))

