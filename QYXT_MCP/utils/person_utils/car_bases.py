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
import threading
import sqlite3
from datetime import datetime, timedelta
import json
import os
import sys
import os

from collections import defaultdict
from bisect import bisect_left
import concurrent.futures
from copy import deepcopy
from fuzzywuzzy import fuzz, process

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from person_sqls import    GET_REALTIME_STATION_HEAD_INFO_SQL
from car_utils import  check_numeric_condition,normalize,fuzzy_match,generate_car_statistics
from datetime import timezone

UTC = timezone.utc
def parse_datetime(val):
    if val is None:
        return None
    if isinstance(val, datetime):
        return val
    s = str(val).replace("Z", "+00:00")
    formats = [
        None,  # ISO
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
    ]
    for fmt in formats:
        try:
            if fmt is None:
                return datetime.fromisoformat(s)
            return datetime.strptime(s, fmt)
        except:
            continue
    raise ValueError(f"无法解析时间: {val}")

class CarBase:
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
        try:
            self._init_station_names()
            self._init_urls()
        except Exception as e:
            self.logger.error(f"初始化CarBase失败: {e}")
        try:
            self.start_auto_analysis_thread()
        except Exception as e:
            self.logger.error(f"启动分析线程失败: {e}")

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
        except Exception as e:
            self.logger.error(f"_init_urls失败: {e}")
        
    #初始化地点名
    def _init_station_names(self, force=False):
        """
        初始化 self.station_names，若其为空，则从数据库查询并赋值。
        """
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
        except Exception as e:
            self.logger.error(f"_init_station_names失败: {e}")

    # ==================== 4. 内部辅助逻辑 ====================
    def parse_time(self, s: str):
        """统一时间格式"""
        if isinstance(s, datetime):
            return s

        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S"
        ):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                pass

        raise ValueError(f"无法解析时间: {s}")

    def seconds_to_time_str(self,seconds):
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        seconds = seconds % 60

        return f"{hours}时{minutes}分{seconds}秒"


    def format_duration_seconds(self,start_str):
        """计算秒数差，用于后续格式化"""
        if not start_str :
            
            return None,None
      
        s1 = str(start_str).replace("+08:00", "").replace("T", " ")
        t2 = datetime.now()
        t1 = self.parse_time(s1)
        t2 = self.ensure_utc(t2)
        t1 = self.ensure_utc(t1)
        diff = int((t2 - t1).total_seconds())

        return  diff,self.seconds_to_time_str(diff)
    
    def ensure_utc(self,dt):
        if dt is None:
            return None

        # 没有时区，认为是 UTC
        if dt.tzinfo is None:
            return dt.replace(tzinfo=UTC)

        # 有时区，转 UTC
        return dt.replace(UTC)
        
    
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
                    res['UserName'] = f"{user_name}".rstrip('_')
            return result
        except Exception as e:
            self.logger.error(f"fetch_in_out_mine_records failed: {e}")
            return []

    def classify_segments_by_inout(
            self,
            cars_dict,
            inout_records,
            tolerance_sec=120
    ):
        """
        按照入井记录对车辆的轨迹记录 segments 进行分组
        """
        result = {}
        try:
            if not cars_dict:
                return {}
            tol = timedelta(seconds=tolerance_sec)

            def inout_key(r):
                return (r.get('InTime', ""), r.get('OutTime', ""))

            filtered_inout_records = []
            for r in inout_records:
                filtered_inout_records.append(r)

            inout_map = defaultdict(list)
            inout_date_map = defaultdict(list)
            for r in filtered_inout_records:
                car_name = str(r.get('UserName', ""))  # 车辆ID主键
                car_id = str(r.get('CodeSenderAddress', ""))
                if not len(car_id):
                    self.logger.info(f"入井出井记录car_id缺失: {r}")
                date = r.get("InTime", "")[:10]
                date2 = r.get("OutTime", "")[:10]
                try:
                    in_time = self.parse_time(r["InTime"][:19].replace("+08:00", "").replace("T", " "))
                    out_raw = r.get("OutTime") or r.get("mainStationTime")
                    out_time = self.parse_time(out_raw[:19].replace("+08:00", "").replace("T", " "))
                    r["_in_dt"] = in_time
                    r["_out_dt"] = out_time
                except Exception as e:
                    self.logger.error(f"classify_segments_by_inout时间解析异常: {traceback.format_exc()}")
                    continue
                inout_map[car_name].append(r)
                if car_id:
                    inout_map[car_id].append(r)
            keys_in = list(inout_map.keys())

            for car_id, info in cars_dict.items():
                segments = info.get("轨迹记录", [])
                car_id1 = info['车辆编号']
                person_base_info = {k: v for k, v in info.items() if k != "轨迹记录"}
                person_base_info['车辆名称'] = car_id

                if not segments:
                    result[car_id] = {
                        **person_base_info,
                        "出入井记录": []
                    }
                    continue

                inout_filtered = inout_map.get(car_id, [])
                if not inout_filtered and segments:
                    inout_filtered = inout_map.get(car_id1, [])

                segments_sorted = sorted(segments, key=lambda x: x.get("轨迹开始时间", ""))
                segment_objs = []
                for seg in segments_sorted:
                    try:
                        s = seg.copy()
                        s["_start"] = self.parse_time(s["轨迹开始时间"][:19].replace("+08:00", ""))
                        s["_end"] = self.parse_time(s["轨迹结束时间"][:19].replace("+08:00", ""))
                        segment_objs.append(s)
                    except Exception as e:
                        self.logger.error(f"轨迹段时间解析异常: {traceback.format_exc()}")
                        continue

                if not segment_objs:
                    result[car_id] = {
                        **person_base_info,
                        "出入井记录": []
                    }
                    continue

                starts = [x["_start"] for x in segment_objs]
                groups = []
                seg_flags = [False] * len(segment_objs)
                any_grouped = False

                if not inout_filtered:
                    for seg in segment_objs:
                        seg.pop("_start", None)
                        seg.pop("_end", None)
                    first = segment_objs[0]
                    t1,t2 = self.format_duration_seconds(first["轨迹开始时间"])
                    groups.append({
                        "入井时间": first["轨迹开始时间"],
                        "出井时间": None,
                        "入井地点": first.get("主站名称"),
                        "出井地点": None,
                        "入井时长": t2,
                        "入井时长(秒)": t1,
                        "单次入井井下轨迹段数": len(segment_objs),
                        "具体轨迹变化": segment_objs,
                    })
                    result[car_id] = {
                        **person_base_info,
                        "出入井记录": groups
                    }
                    continue

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
                    left_idx = bisect_left(starts, left_bound)

                    for idx in range(left_idx, len(segment_objs)):
                        seg = segment_objs[idx]
                        if seg["_start"] > right_bound:
                            break
                        if seg["_end"] >= left_bound and seg["_start"] <= right_bound:
                            grouped_indices.append(idx)
                            seg_flags[idx] = True

                    if not grouped_indices:
                        continue
                    any_grouped = True
                    group_segs = [segment_objs[i].copy() for i in grouped_indices]
                    for seg in group_segs:
                        seg.pop("_start", None)
                        seg.pop("_end", None)
                    if group_segs:
                        group_segs[-1]["轨迹结束时间"] = record.get('OutTime')
                        if record.get("OutWellPlace"):
                            group_segs[-1]["主站"] = record["OutWellPlace"]
                        if idx_r > 0:
                            group_segs[0]["轨迹开始时间"] = record['InTime']
                            group_segs[0]["主站"] = record.get("InWellPlace")
                    groups.append({
                        "入井时间": record.get("InTime"),
                        "出井时间": record.get("OutTime"),
                        "入井地点": record.get("InWellPlace"),
                        "出井地点": record.get("OutWellPlace"),
                        "入井时长": record.get("ContinueTime"),
                        "入井时长(秒)": (
                            lambda t: (
                                int(t.split("时")[0]) * 3600 if isinstance(t, str) and "时" in t else 0
                            ) + (
                                int(t.split("时")[1].split("分")[0]) * 60 if isinstance(t, str) and "分" in t else 0
                            ) + (
                                int(t.split("分")[-1].split("秒")[0]) if isinstance(t, str) and "秒" in t else 0
                            ) if t else None
                        )(record.get("ContinueTime")),
                        "单次入井井下轨迹段数": len(group_segs),
                        "具体轨迹变化": group_segs,
                    })

                if not any_grouped:
                    for seg in segment_objs:
                        seg.pop("_start", None)
                        seg.pop("_end", None)
                    first = segment_objs[0]
                    t1,t2 = self.format_duration_seconds(first["轨迹开始时间"])
                    groups.append({
                        "入井时间": first["轨迹开始时间"],
                        "出井时间": None,
                        "入井地点": first.get("主站名称"),
                        "出井地点": None,
                        "入井时长": t2,
                        "入井时长(秒)": t1,
                        "单次入井井下轨迹段数": len(segment_objs),
                        "具体轨迹变化": segment_objs,
                    })
                ungrouped = [
                    segment_objs[i]
                    for i, flag in enumerate(seg_flags)
                    if not flag
                ]
                if ungrouped:
                    for seg in ungrouped:
                        seg.pop("_start", None)
                        seg.pop("_end", None)
                    first = ungrouped[0]
                    
                    t1,t2 = self.format_duration_seconds(first["轨迹开始时间"])
                    groups.append({
                        "入井时间": first["轨迹开始时间"],
                        "出井时间": None,
                        "入井地点": first.get("主站名称"),
                        "出井地点": None,
                        "入井时长": t2,
                        "入井时长(秒)": t1,
                        "单次入井井下轨迹段数": len(segment_objs),
                        "具体轨迹变化": segment_objs,
                    })
                result[car_id] = {
                    **person_base_info,
                    "出入井记录": groups
                }
        except Exception as e:
            self.logger.error(f"classify_segments_by_inout失败: {traceback.format_exc()}")
        return result

    def get_cars_by_filters(
            self,
            cardids=None,
            car_names=None,
            electricitys=None,
            start_time=None,
            end_time=None,
    ):
        try:
            from datetime import datetime, time

            self._init_station_names()

            now = datetime.now()

            if not start_time:
                start_time = datetime.combine(
                    now.date(),
                    time.min
                ).strftime("%Y-%m-%d %H:%M:%S")

            if not end_time:
                end_time = datetime.combine(
                    now.date(),
                    time.max
                ).strftime("%Y-%m-%d %H:%M:%S")

            where_conditions = [
                "ENTRY_TIME >= %(start_time)s",
                "ENTRY_TIME < %(end_time)s"
            ]
            params = {
                "start_time": start_time,
                "end_time": end_time
            }

            if cardids:
                cardid_clauses = []
                for i, cid in enumerate(cardids):
                    key = f"cardid_{i}"
                    cardid_clauses.append(f"CARD_ID LIKE %({key})s")
                    params[key] = f"%{cid}%"
                where_conditions.append(f"({' OR '.join(cardid_clauses)})")

            if electricitys:
                placeholders = []
                for i, v in enumerate(electricitys):
                    key = f"elec_{i}"
                    params[key] = v
                    placeholders.append(f"%({key})s")
                where_conditions.append(f"ELECTRICITY IN ({','.join(placeholders)})")

            if car_names:
                clauses = []
                for i, v in enumerate(car_names):
                    key = f"car_{i}"
                    params[key] = f"%{v}%"
                    clauses.append(f"CAR_NAME LIKE %({key})s")
                where_conditions.append(f"({' OR '.join(clauses)})")

            where_sql = " AND ".join(where_conditions)
            query = f"""
            WITH base AS
            (
                SELECT
                    CARD_ID,
                    CAR_NAME,
                    DEPARTMENT,
                    CAR_TYPE_NAME,
                    ELECTRICITY,
                    MAIN_STATION_ID,
                    SUB_STATION_ID,

                    ENTRY_TIME AS STATION_TIME,

                    MAIN_STATION_DISTANCE,
                    SUB_STATION_DISTANCE

                FROM PS.SYG_RYDW_CAR_LOCATION

                WHERE {where_sql}
            ),

            mark_segment AS
            (
                SELECT
                    *,

                    if(
                        (
                            lagInFrame(MAIN_STATION_ID)
                            OVER(
                                PARTITION BY CARD_ID
                                ORDER BY STATION_TIME
                            )
                            != MAIN_STATION_ID
                        )

                        OR

                        (
                            lagInFrame(SUB_STATION_ID)
                            OVER(
                                PARTITION BY CARD_ID
                                ORDER BY STATION_TIME
                            )
                            != SUB_STATION_ID
                        )

                        OR

                        (
                            dateDiff(
                                'minute',

                                lagInFrame(STATION_TIME)
                                OVER(
                                    PARTITION BY CARD_ID
                                    ORDER BY STATION_TIME
                                ),

                                STATION_TIME
                            ) > 20
                        ),

                        1,
                        0

                    ) AS new_seg

                FROM base
            ),

            segment_ids AS
            (
                SELECT
                    *,

                    sum(
                        ifNull(new_seg,1)
                    )

                    OVER(
                        PARTITION BY CARD_ID
                        ORDER BY STATION_TIME
                        ROWS BETWEEN UNBOUNDED PRECEDING
                        AND CURRENT ROW
                    )

                    AS segment_id

                FROM mark_segment
            )

            SELECT

                CARD_ID,
                CAR_NAME,
                DEPARTMENT,
                CAR_TYPE_NAME,
                ELECTRICITY,

                any(MAIN_STATION_ID),
                any(SUB_STATION_ID),

                min(STATION_TIME),
                max(STATION_TIME),

                max(MAIN_STATION_DISTANCE),
                max(SUB_STATION_DISTANCE),

                segment_id

            FROM segment_ids

            GROUP BY

                CARD_ID,
                CAR_NAME,
                DEPARTMENT,
                CAR_TYPE_NAME,
                ELECTRICITY,
                segment_id

            ORDER BY
                CARD_ID,
                max(STATION_TIME)
            """

            try:
                result = self.client.query(query, parameters=params)
                cars_dict = {}
                total_segments = 0
                for row in result.result_rows:
                    card_name = row[1]
                    if card_name not in cars_dict:
                        cars_dict[card_name] = {
                            "车辆编号": row[0],
                            "车辆名称": card_name,
                            "所属部门": row[2],
                            "车辆类型": row[3],
                            "轨迹记录": []
                        }
                    duration_seconds = -1
                    try:
                        start_dt = parse_datetime(row[7])
                        end_dt = parse_datetime(row[8])
                        if start_dt and end_dt:
                            duration_seconds = int((end_dt - start_dt).total_seconds())
                    except Exception as e:
                        self.logger.error(f"轨迹持续时间计算失败: {traceback.format_exc()}")
                    if duration_seconds == 0:
                        continue
                    record = {
                        "电量": row[4],
                        "主站名称": self.station_names.get(int(row[5]), {}).get("name", row[5]) if row[5] else row[5],
                        "分站名称": self.station_names.get(int(row[6]), {}).get("name", row[6]) if row[6] else row[6],
                        "轨迹开始时间": str(row[7]),
                        "轨迹结束时间": str(row[8]),
                        "分段持续时间/s": duration_seconds,
                        "距离主站距离/m": row[9],
                        "距离分站距离/m": row[10],
                    }
                    cars_dict[card_name]["轨迹记录"].append(record)
                    total_segments += 1
                # to_delete = []
                # for k, v in cars_dict.items():
                #     if not v.get("轨迹记录"):
                #         to_delete.append(k)
                #         total_segments -= 1
                # for k in to_delete:
                #     del cars_dict[k]

                inout_records = self.fetch_in_out_mine_records(start_time, end_time, '')
               
                outs = self.classify_segments_by_inout(cars_dict, inout_records)
                
                return {
                    "total": total_segments,
                    "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "cars": outs
                }
            except Exception as e:
                self.logger.error(f"Clickhouse数据查询错误: {traceback.format_exc()}")
                return {
                    "total": 0,
                    "persons": {},
                    "error": str(e)
                }
        except Exception as e:
            if hasattr(self, 'logger'):
                self.logger.error(f"get_cars_by_filters总异常: {traceback.format_exc()}")
            else:
                print(f"get_cars_by_filters总异常: {e}")
            return {
                "total": 0,
                "persons": {},
                "error": str(e)
            }
    
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
                    self.get_cars_infos_daytype_with_cache(
                        start_date=start_date,
                        end_date=end_date,
                    )
                except Exception as e:
                    self.logger.info(f"自动分析: print_tongfeng_today_with_cache 异常: {e}")
            except Exception as e:
                self.logger.info(f"自动分析: 总体异常: {e}")

    # ==================== 3. 封装cache功能 =====================
    def get_cars_infos_daytype_with_cache(
            self,
            car_names_filters: Union[List[str], str, None] = None,
            electricitys_filters: Union[List[str], str, None] = None,
            departments_filters: Union[List[str], str, None] = None,
            car_types_filters: Union[List[str], str, None] = None,
            station_filters: Union[List[str], str, None] = None,
            area_names_filters: Union[List[str], str, None] = None,
            in_places_filters: Union[List[str], str, None] = None,
            out_places_filters: Union[List[str], str, None] = None,
            numeric_filters: Optional[Dict[str, Dict]] = None,
            statistics_filter: Union[List[str], str, None] = None,
            start_date: Union[str, 'datetime', None] = None,
            end_date: Union[str, 'datetime', None] = None,
    ) -> dict:
        """
        封装分日期缓存的车辆数据获取。以 day_str 为key, 每日缓存。
        其它filter同 get_cars_by_filters。
        """
        try:
            db_path = os.path.join(os.path.dirname(__file__), "car_analysis_cache.db")
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS car_atom_cache (
                    day_str TEXT PRIMARY KEY,
                    result_json TEXT
                )
            ''')
            conn.commit()

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
            today_str = datetime.now().strftime("%Y-%m-%d")

            missed_days = []
            final_output = {}
            for day in req_dates:
                if day == today_str:
                    missed_days.append(day)
                    continue
                cursor.execute('''
                    SELECT result_json FROM car_atom_cache
                    WHERE day_str=?
                ''', (day,))
                rows = cursor.fetchall()
                if rows and len(rows) > 0:
                    try:
                        day_data = json.loads(rows[0][0])
                        final_output[day] = day_data
                    except Exception as e:
                        self.logger.error(f"car_atom_cache缓存json解码失败: {e}")
                        final_output[day] = {}
                else:
                    missed_days.append(day)

            self.logger.info(f'missed_days-----{missed_days}')
            for day in missed_days:
                fetch_start = f"{day} 00:00:00"
                fetch_end = f"{day} 23:59:59"
                fetch_result = self.get_cars_by_filters(
                    start_time=fetch_start,
                    end_time=fetch_end,
                )
                day_data = {}
                if "cars" in fetch_result:
                    for car_id, carinfo in fetch_result["cars"].items():
                        day_data[car_id] = carinfo
                cursor.execute('''
                    INSERT OR REPLACE INTO car_atom_cache
                    (day_str, result_json)
                    VALUES (?, ?)
                ''', (day, json.dumps(day_data, ensure_ascii=False, default=str)))
                conn.commit()
                final_output[day] = day_data

            filtered_output = {}
            for day, day_dict in final_output.items():
                data_out = self.car_filter(
                    day_dict,
                    car_names_filters=car_names_filters,
                    departments_filters=departments_filters,
                    car_types_filters=car_types_filters,
                    electricitys_filters=electricitys_filters,
                    area_names_filters=area_names_filters,
                    station_filters=station_filters,
                    in_places_filters=in_places_filters,
                    out_places_filters=out_places_filters,
                    numeric_filters=numeric_filters,
                    statistics_filter=statistics_filter
                )
                if len(data_out) > 0:
                    filtered_output[day] = data_out

            output = {"每日数据": filtered_output, "总共天数": len(filtered_output)}
            conn.close()
            return output
        except Exception as e:
            if hasattr(self, 'logger'):
                self.logger.error(f"get_cars_infos_daytype_with_cache异常: {traceback.format_exc()}")
            return {"error": str(e)}

    def car_filter(
            self,
            pdata,
            car_names_filters: Union[List[str], str, None] = None,
            departments_filters: Union[List[str], str, None] = None,
            car_types_filters: Union[List[str], str, None] = None,
            electricitys_filters: Union[List[str], str, None] = None,
            area_names_filters: Union[List[str], str, None] = None,
            station_filters: Union[List[str], str, None] = None,
            in_places_filters: Union[List[str], str, None] = None,
            out_places_filters: Union[List[str], str, None] = None,
            numeric_filters: Optional[Dict[str, Dict]] = None,
            statistics_filter: Union[List[str], str, None] = None,
    ):
        car_names_filters = normalize(car_names_filters)
        department_filters = normalize(departments_filters)
        classtype_filters = normalize(car_types_filters)

        def process_person(person_item):
            person_key, person = person_item
            person = deepcopy(person)
            try:
                if car_names_filters:
                    if not fuzzy_match(person.get("车辆名称", ""), car_names_filters, 90):
                        return None
                if department_filters:
                    if not fuzzy_match(person.get("所属部门", ""), department_filters):
                        return None
                if classtype_filters:
                    if not fuzzy_match(person.get("车辆类型", ""), classtype_filters):
                        return None
                has_segments = False
                use_segments_grouped = []
                for segments in person.get("出入井记录", []):
                    keep = True
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
                                keep = False
                            else:
                                keep = True
                    if not keep:
                        continue
                    filtered_records = []
                    for record in segments.get("具体轨迹变化", []):
                        keep_inner = True
                        if keep_inner and electricitys_filters:
                            if not fuzzy_match(record.get("电量"), electricitys_filters):
                                keep_inner = False
                        if keep_inner and station_filters:
                            v1 = record.get("主站名称")
                            v2 = record.get("分站名称")
                            if not (fuzzy_match(v1, station_filters, 80) or fuzzy_match(v2, station_filters, 80)):
                                keep_inner = False
                        if keep_inner and area_names_filters:
                            if not fuzzy_match(record.get("区域名称"), area_names_filters):
                                keep_inner = False
                        if keep_inner and numeric_filters:
                            for field_name, condition in numeric_filters.items():
                                rec_data = record.get(field_name,-1)
                                if rec_data == -1:
                                    continue
                                if isinstance(rec_data, list):
                                    if not any(check_numeric_condition(val, condition) for val in rec_data):
                                        keep_inner = False
                                    else:
                                        keep_inner = True
                                else:
                                    if not check_numeric_condition(rec_data, condition):
                                        keep_inner = False
                                    else:
                                        keep_inner = True
                        if keep_inner:
                            filtered_records.append(record)
                    if filtered_records:
                        segments["具体轨迹变化"] = filtered_records
                        has_segments = True
                        use_segments_grouped.append(segments)
                if has_segments:
                    person["出入井记录"] = use_segments_grouped
                    return (person_key, person)
                else:
                    return None
            except Exception as e:
                if hasattr(self, 'logger'):
                    self.logger.error(f"car_filter.process_person异常: {traceback.format_exc()}")
                return None

        result = {}
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
                futures = [executor.submit(process_person, item) for item in pdata.items()]
                for f in concurrent.futures.as_completed(futures):
                    res = f.result()
                    if res is not None:
                        person_key, person_val = res
                        result[person_key] = person_val
            if statistics_filter is not None and len(statistics_filter) > 0:
                stats = generate_car_statistics(result,statistics_filter)
                return {
                    "statistics": stats
                }
            return result
        except Exception as e:
            if hasattr(self, 'logger'):
                self.logger.error(f"car_filter异常: {traceback.format_exc()}")
            return {}

if __name__ == "__main__":
    from pprint import pprint

    client = clickhouse_connect.get_client(
        host="10.11.22.80", port=9120, username="nethouse",
        password="CGC%EVXr.ET10Y_N", secure=True, verify=False, autogenerate_session_id=False
    )
    # 增加 logger 实例
    logger = logging.getLogger('carbase_main')
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
    if not logger.handlers:
        logger.addHandler(handler)

    car_util = CarBase(client, logger=logger)

    print("\n=== get_cars_infos_daytype_with_cache 输出 ===")
    car_names_filters = None
    electricitys_filters = None
    departments_filters = None
    car_types_filters = None
    station_filters = None
    area_names_filters = None
    in_places_filters = None
    out_places_filters = None
    numeric_filters = None
    statistics_filter = [
         "总车辆数",
        "车辆总览",
        "车辆列表_名称_编号_出入井次数",
        "出入井时长分布/辆次",
        "入井时间段分布/辆次",
        "出井时间段分布/辆次",
        "入井地点分布/辆次",
        "出井地点分布/辆次",
        "区域分布/条",
        "主站分布/条",
        "分站分布/条",
        "站点停留时长分布/条",
        "所属部门分布/辆",
        "车辆类型分布/辆"
    ]
    car_stat_keys = [
        "总车辆数",
        "车辆总览",
        "车辆列表_名称_编号_出入井次数",
        "出入井时长分布/辆次",
        "入井时间段分布/辆次",
        "出井时间段分布/辆次",
        "入井地点分布/辆次",
        "出井地点分布/辆次",
        "区域分布/条",
        "主站分布/条",
        "分站分布/条",
        "站点停留时长分布/条",
        "所属部门分布/辆",
        "车辆类型分布/辆"
    ]
    try:
        daytype_data = car_util.get_cars_infos_daytype_with_cache(
            car_names_filters=car_names_filters,
            electricitys_filters=electricitys_filters,
            departments_filters=departments_filters,
            car_types_filters=car_types_filters,
            station_filters=station_filters,
            area_names_filters=area_names_filters,
            in_places_filters=in_places_filters,
            out_places_filters=out_places_filters,
            numeric_filters=numeric_filters,
            statistics_filter=['all'],
            start_date="2026-08-06 00:00:00",
            end_date="2026-08-09 00:00:00",
        )
        daytype_json = json.dumps(daytype_data, ensure_ascii=False, indent=2)
        with open("history_person_data.txt", "w", encoding="utf-8") as f:
            f.write(daytype_json)
            print("全部数据已成功写入 history_person_data.txt")
        print("json长度:", len(daytype_json))
    except Exception as e:
        logger.error(f"主函数发生异常: {traceback.format_exc()}")
