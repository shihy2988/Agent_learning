#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文件名:	person_utils.py
作者:	shihy
创建日期:	2026-04-22
描述:	矿井人员定位相关的数据处理与工具函数。用于时间格式化、筛选、统计分析和结构化人员信息，适配 MCP 服务人员定位业务功能需求。
"""


import re
from datetime import datetime
from typing import Dict, List, Optional
import redis
import time

import requests
import json
from pprint import pp, pprint
import datetime
from fastmcp import FastMCP
from collections import defaultdict

from email import message
import json
import logging
import re
import urllib3
import requests
import clickhouse_connect
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union


import sys
import os
import copy
from fuzzywuzzy import fuzz, process
from tqdm import tqdm

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

def get_redis_client():
    """
    获取Redis客户端连接（单例模式）。
    用法：
        r = get_redis_client()
    """
    return redis.Redis(host="10.11.6.15", port=9702, decode_responses=True)

def set_type_data_to_redis(type_name: str, data, expire_seconds: int = 7*24*3600):
    """
    将 type 下的数据 JSON 序列化后写入 redis，默认过期时间7天
    推荐：每次读取外部API/数据库后调用此方法做备份
    """
    assert isinstance(type_name, str) and type_name
    key = f"mcp:{type_name}"
    redis_client = get_redis_client()
    redis_client.set(key, json.dumps(data, ensure_ascii=False), ex=expire_seconds)

def get_type_data_from_redis(type_name: str):
    """
    按 type 名称从 redis 获取数据并反序列化
    如无数据则返回 None
    """
    import json
    assert isinstance(type_name, str) and type_name
    key = f"mcp:{type_name}"
    redis_client = get_redis_client()
    raw = redis_client.get(key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None




def get_time_stats(time_changes: List[datetime]) -> Dict:
    if not time_changes:
        return {
            "earliest": None,
            "latest": None,
            "duration_seconds": 0,
            "earliest_dt": None,
            "latest_dt": None,
        }

    earliest_dt = min(time_changes)
    latest_dt = max(time_changes)
    duration_seconds = int((latest_dt - earliest_dt).total_seconds())

    return {
        "earliest": earliest_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "latest": latest_dt.strftime("%Y-%m-%d %H:%M:%S"),
        "duration_seconds": duration_seconds,
        "earliest_dt": earliest_dt,
        "latest_dt": latest_dt,
    }


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

def fetch_and_process_car_history(card_id="0099", begin_time="2026-04-23 00:00:00", end_time="2026-04-23 20:00:00"):
    url = "https://10.11.22.81:28701/apiaccess/api/rydw_getCarHistoryLocation_n"

    payload = json.dumps({
        "cardID": card_id,
        "beginTime": begin_time,
        "endTime": end_time
    })
    headers = {
        'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'Host': '10.11.22.81:28701',
        'Connection': 'keep-alive'
    }

    response = requests.request("POST", url, headers=headers, data=payload,verify=False)
    text = response.content.decode("utf-8-sig")
    car_records = json.loads(text).get("data", []) if response.status_code == 200 else []

    simple_segments = []
    prev_area = None
    segment_start = None
    segment_end = None

    first_car_name = None
    first_card_id = None
    first_department = None
    first_found = False

    for seg in car_records:
        current_area = seg.get("mainStationID", "") or seg.get("areaName", "")
        seg_time = seg.get("mainStationTime", "")

        if not first_found:
            first_car_name = seg.get("carName", "")
            first_card_id = seg.get("cardId", "")
            first_department = seg.get("department", "")
            first_found = True

        if prev_area != current_area:
            if prev_area is not None:
                simple_segments.append({
                    "area": prev_area,
                    "S_E_Time": [segment_start,segment_end],
                })
            prev_area = current_area
            segment_start = seg_time
            segment_end = seg_time
        else:
            if segment_start is None or seg_time < segment_start:
                segment_start = seg_time
            if segment_end is None or seg_time > segment_end:
                segment_end = seg_time

    if prev_area is not None:
        simple_segments.append({
            "area": prev_area,
            "S_E_Time": [segment_start,segment_end],
        })

    car_info = {
        "carName": first_car_name, 
        "cardId": first_card_id,
        "department": first_department,
        "total_count": len(simple_segments),
        "query_date": json.loads(payload).get("findDate", ""),
        "time_now": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    result = {
        "carInfo": car_info,
        "segments": simple_segments
    }

    merged_segments = merge_adjacent_segments(result['segments'])
    merged_segments = merge_consecutive_same_area(merged_segments)
    result['segments'] = merged_segments
    result["carInfo"]['total_count'] = len(merged_segments)
    return result, len(simple_segments)

    # INSERT_YOUR_CODE

class PersonBase:
    """
    人员相关功能的基类，供工具与utils共用。
    主要封装通用的人员信息方法和属性。实际业务类可继承它。
    """

    def __init__(self):
        # 可以定义一些共用的属性，如站点名称缓存等
        self.station_names = {}
        self.station_names_time = 0

    # ==================== 4. 内部辅助逻辑 ====================
    def get_person_name_cardid_dicts(self):
        """
        获取 name->cardid 和 cardid->name 的映射字典

        Returns:
            tuple: (name2cardid, cardid2name)
        """
        try:
            # 尝试先从缓存拿数据
            data = None
            # 只对前5种类型使用redis缓存
            if time.time() - self.last_query_time < 3600:
                self.last_query_time = time.time()
            else:
                data = get_type_data_from_redis("person")
            if not data:
                # 若缓存没有，则拉取API数据
                payload = json.dumps({
                    "mineCode": "",
                    "deptID": "",
                    "nameOrID": ""
                })
                headers = {
                    'Content-Type': 'application/json',
                    'Accept': '*/*',
                }
                resp = requests.post(self.person_info_api_url, headers=headers, data=payload, timeout=30, verify=False)
                text = resp.content.decode("utf-8-sig")
                persons = json.loads(text).get("data", []) if resp.status_code == 200 else []
                set_type_data_to_redis("person", persons)
                data = persons

            name2cardid = {}
            cardid2name = {}
            for p in data or []:
                name = p.get("name") or p.get("personName")  # 兼容不同字段
                cardid = p.get("cardID") or p.get("cardId")
                if name and cardid:
                    name2cardid[name] = cardid
                    cardid2name[cardid] = name

            return name2cardid, cardid2name
        except Exception as e:
            logger.error(f"get_person_name_cardid_dicts异常: {e}", exc_info=True)
            return {}, {}

    def parse_time(self, t: str):
        """统一时间格式"""
        t = t.replace("T", " ")
        return datetime.strptime(t, "%Y-%m-%d %H:%M:%S")

    def classify_segments_by_inout(self, segments, inout_records, tolerance_sec=120):
        """
        按入井记录对 segments 分组

        tolerance_sec: 容忍秒（解决边界漂移）
        若 inout_records 的InTime/InPlace 为空或者未分组，则选择segments第一条记录的 segmentStartTime 和 mainStationId

        对于多段分组，需要将第一段的最后一条seg调整为当前record的结束时间和地点，
        第二段的第一条seg调整为下一record的起始时间和地点
        """
        from copy import deepcopy

        result = []

        # 如果 inout_records 为空或为 None，则直接返回 segments
        if not inout_records:
            if not segments:
                return []
            first_seg = segments[0] if segments else {}
            return [{
                "inTime": first_seg.get("segmentStartTime"),
                "outTime": None,
                "inPlace": first_seg.get("mainStationId"),
                "outPlace": None,
                "duration": None,
                "segments_count": len(segments),
                "segments": segments
            }]

        grouped_seg_ids = set()
        any_grouped = False

        # 排序segments（按segmentStartTime升序）
        segments_sorted = sorted(segments, key=lambda s: s.get("segmentStartTime", ""))
        total_segments = len(segments_sorted)
        seg_flags = [False] * total_segments  # 标记是否已分组

        segment_objs = [deepcopy(seg) for seg in segments_sorted]

        for idx_r, record in enumerate(inout_records):
            in_time_raw = record.get("InTime")
            in_place = record.get("InWellPlace")
            # 若 inTime/inPlace 为空，直接处理为segments[0]
            if not in_time_raw or not in_place:
                if segments_sorted and len(result) == 0:
                    first_seg = segments_sorted[0]
                    result.append({
                        "inTime": first_seg.get("segmentStartTime"),
                        "outTime": None,
                        "inPlace": first_seg.get("mainStationId"),
                        "outPlace": None,
                        "duration": None,
                        "segments_count": len(segments_sorted),
                        "segments": segments_sorted
                    })
                continue

            out_time_record = record.get("OutTime")
            if not out_time_record or out_time_record == '':
                out_time_record = record.get("mainStationTime")

            grouped_indices = []
            grouped_segments = []
            try:
                in_time = self.parse_time(in_time_raw)
                out_time = self.parse_time(out_time_record)
            except Exception:
                continue

            # 找属于该分组的 segment 索引
            for idx, seg in enumerate(segment_objs):
                try:
                    seg_start = self.parse_time(seg["segmentStartTime"][:19].replace('+08:00', ''))
                    seg_end = self.parse_time(seg["segmentEndTime"][:19].replace('+08:00', ''))
                except Exception:
                    continue

                if (
                        seg_end >= (in_time - timedelta(seconds=tolerance_sec)) and
                        seg_start <= (out_time + timedelta(seconds=tolerance_sec))
                ):
                    grouped_indices.append(idx)
                    grouped_seg_ids.add(idx)
                    seg_flags[idx] = True

            if grouped_indices:
                any_grouped = True

                # 按新要求：多段时，当前group最后一seg结束时间地点用record的outTime/outPlace
                # 下一个group第一seg起始时间和地点用record的inTime/inPlace
                group_segs = [deepcopy(segment_objs[i]) for i in grouped_indices]
                n_group = len(group_segs)
                # 衔接修正仅当有多组时处理
                # 修改当前组最后一个seg
                if n_group:
                    # 修改最后一个seg的结束时间/主站点
                    group_segs[-1]["segmentEndTime"] = out_time_record
                    if record.get("OutWellPlace"):
                        group_segs[-1]["mainStationId"] = record.get("OutWellPlace")
                # 如果不是第一组，且上一组存在，修改当前组第一个seg的开始时间/主站点
                if idx_r > 0 and n_group:
                    prev_record = inout_records[idx_r]
                    group_segs[0]["segmentStartTime"] = in_time_raw
                    group_segs[0]["mainStationId"] = in_place

                result.append({
                    "inTime": in_time_raw,
                    "outTime": out_time_record,
                    "inPlace": in_place,
                    "outPlace": record.get("OutWellPlace"),
                    "duration": record.get("ContinueTime"),
                    "segments_count": n_group,
                    "segments": group_segs
                })

        # 如前面都未分组且 inout_records 有异常(如全部 inTime/inPlace 为空), 仍需按segments[0]
        if not any_grouped and segments_sorted and len(result) == 0:
            first_seg = segments_sorted[0]
            return [{
                "inTime": first_seg.get("segmentStartTime"),
                "outTime": None,
                "inPlace": first_seg.get("mainStationId"),
                "outPlace": None,
                "duration": None,
                "segments_count": len(segments_sorted),
                "segments": segments_sorted
            }]

        # segments 中未被分组的部分还要单独返回
        ungrouped_segments = [segment_objs[idx] for idx, flag in enumerate(seg_flags) if not flag]
        if ungrouped_segments:
            first_seg = ungrouped_segments[0]
            result.append({
                "inTime": first_seg.get("segmentStartTime"),
                "outTime": None,
                "inPlace": first_seg.get("mainStationId"),
                "outPlace": None,
                "duration": None,
                "segments_count": len(ungrouped_segments),
                "segments": ungrouped_segments
            })

        return result

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

    def _fetch_person_realtime_api(self) -> List[Dict]:
        try:
            headers = {"Content-Type": "application/json"}
            resp = requests.post(
                self.person_real_api_url,
                json={"mineCode": ""},
                headers=headers,
                verify=False,
                timeout=8,
            )
            return resp.json().get("data", []) if resp.status_code == 200 else []
        except Exception as e:
            logger.error(f"API Error: {e}")
            return []

    def _fetch_car_realtime_api(self) -> List[Dict]:
        try:
            headers = {"Content-Type": "application/json"}
            response = requests.post(
                self.car_real_api_url,
                data={},
                headers=headers,
                verify=False,
                timeout=30,
            )

            text = response.content.decode("utf-8-sig")
            car_now_raw = json.loads(text).get("data", []) if response.status_code == 200 else []

            self._init_station_names()

            # 过滤并处理字段：去除 '_sortTime'，mainStationID/subStationID 替换为 names 的中文名
            def resolve_station_name(station_id_key, row):
                sid = row.get(station_id_key)
                if sid is not None and str(sid).isdigit():  # 数字字符串或数字
                    sid_int = int(sid)
                    return self.station_names.get(sid_int, {}).get('name', sid)
                return sid

            car_now = []
            for raw in car_now_raw:
                if not isinstance(raw, dict):
                    continue
                row = raw.copy()
                row.pop('_sortTime', None)
                row.pop('mainStationHeadPlace', None)
                row.pop('subStationHeadPlace', None)
                row.pop('otherInfo', None)
                row.pop('tunnelDistance', None)
                row.pop('tunnelID', None)
                row.pop('tunnelName', None)
                # 替换 mainStationID 和 subStationID 的值为中文名（如果可映射）
                row['mainStationID'] = resolve_station_name('mainStationID', raw)
                row['subStationID'] = resolve_station_name('subStationID', raw)
                car_now.append(row)
            return car_now

        except Exception as e:
            logger.error(f"API Error: {e}")
            return []

    def get_persons_by_filters(
            self,
            cardids: list = None,  # 工号
            names: list = None,
            electricitys: list = None,  # 电量：['正常', '低电量']
            areas: list = None,
            work_types: list = None,
            class_names: list = None,
            departments: list = None,
            start_time: str = None,
            end_time: str = None,
            main_stations: list = None,  # 新增 主站（中文名模糊）
            sub_stations: list = None,  # 新增 分站（中文名模糊）
    ):
        """
        支持全字段模糊查询。
        如果不传 start_time/end_time，默认查询当日数据。
        cardids 支持精确或模糊。
        electricitys 仅支持 ['正常', '低电量'] 这两种状态过滤。
        main_stations/sub_stations 支持模糊中文名传入，自动匹配到ID
        同一个人名name会有不同的cardid，需要进行区分。
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

        # 初始化主站/分站名-ID映射表
        self._init_station_names()
        # main_stations匹配到的主站ID列表
        matched_main_station_ids = []
        if main_stations:
            for user_word in main_stations:
                for sid, sdict in self.station_names.items():
                    s_name = sdict.get('name', "")
                    if s_name and fuzz.partial_ratio(user_word, s_name) > 70:
                        matched_main_station_ids.append(sid)
            matched_main_station_ids = list(set(matched_main_station_ids))
        # sub_stations匹配到的分站ID列表
        matched_sub_station_ids = []
        if sub_stations:
            for user_word in sub_stations:
                for sid, sdict in self.station_names.items():
                    s_name = sdict.get('name', "")
                    if s_name and fuzz.partial_ratio(user_word, s_name) > 70:
                        matched_sub_station_ids.append(sid)
            matched_sub_station_ids = list(set(matched_sub_station_ids))

        # 2. 基础时间条件
        where_conditions = [
            "UPDATE_TIME >= %(start_time)s",
            "UPDATE_TIME < %(end_time)s",
        ]
        params = {"start_time": start_time, "end_time": end_time}

        # 3.1. 处理 cardids（工号） - 支持多个精确或模糊
        if cardids:
            cardid_clauses = []
            for i, cid in enumerate(cardids):
                key = f"cardid_{i}"
                if isinstance(cid, str) and "%" in cid:
                    cardid_clauses.append(f"CARDID LIKE %({key})s")
                    params[key] = f"{cid}"
                else:
                    cardid_clauses.append(f"CARDID LIKE %({key})s")
                    params[key] = f"%{cid}%"
            where_conditions.append(f"({' OR '.join(cardid_clauses)})")

        # 3.2. 处理 electricitys（电量）-- 只支持['正常','低电量','其他']
        if electricitys:
            statuses = set([str(x) for x in electricitys])
            normal_selected = "正常" in statuses
            low_selected = "低电量" in statuses
            other_selected = "其他" in statuses

            elec_clauses = []
            sub_statuses = []
            if normal_selected:
                sub_statuses.append("正常")
            if low_selected:
                sub_statuses.append("低电量")
            if sub_statuses:
                placeholder = []
                for idx, val in enumerate(sub_statuses):
                    params[f"elec_status_{idx}"] = val
                    placeholder.append(f"%({f'elec_status_{idx}'})s")
                elec_clauses.append(f"ELECTRICITY IN ({', '.join(placeholder)})")

            if other_selected:
                other_condition = "ELECTRICITY NOT IN (%(elec_not_normal)s, %(elec_not_low)s)"
                params["elec_not_normal"] = "正常"
                params["elec_not_low"] = "低电量"
                elec_clauses.append(other_condition)

            if elec_clauses:
                where_conditions.append(f"({' OR '.join(elec_clauses)})")

        # 3.3 主站ID和分站ID的过滤
        if matched_main_station_ids:
            placeholders = []
            for idx, sid in enumerate(matched_main_station_ids):
                params[f"mainStation_id_{idx}"] = sid
                placeholders.append(f"%({f'mainStation_id_{idx}'})s")
            where_conditions.append(f"(MAINSTATIONID IN ({', '.join(placeholders)}))")
        if matched_sub_station_ids:
            placeholders = []
            for idx, sid in enumerate(matched_sub_station_ids):
                params[f"subStation_id_{idx}"] = sid
                placeholders.append(f"%({f'subStation_id_{idx}'})s")
            where_conditions.append(f"(SUBSTATIONID IN ({', '.join(placeholders)}))")

        # 3.4 定义模糊查询字段映射
        fuzzy_filters = {
            "NAME": (names, "name"),
            "AREANAME": (areas, "area"),
            "WORKTYPE": (work_types, "wtype"),
            "CLASSTIMENAME": (class_names, "cname"),
            "DEPARTMENT": (departments, "dept"),
        }

        # 4. 动态构建模糊查询子句 (每个维度的多个关键字用 OR 连接)
        for col, (val_list, prefix) in fuzzy_filters.items():
            if val_list:
                clauses = []
                for i, val in enumerate(val_list):
                    key = f"{prefix}_{i}"
                    clauses.append(f"{col} LIKE %({key})s")
                    params[key] = f"%{val}%"
                where_conditions.append(f"({' OR '.join(clauses)})")

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
                toDate(MAINSTATIONTIME) AS stationDate
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
            any(SUBSTATIONDISTANCE) as subStationDistance

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
                # 同一个 name 可能有不同 cardid，必须区分
                key = f"{name}_{cardid}"
                if key not in persons_dict:
                    persons_dict[key] = {
                        "name": name,
                        "cardid": cardid,
                        "workType": row[11],
                        "classTimeName": row[2],
                        "department": row[10],
                        "records": [],
                    }
                # 记录分段的出入信息
                record = {
                    "areaName": row[1],
                    "segmentStartTime": str(row[5]).replace('+08:00', ''),
                    "segmentEndTime": str(row[6]).replace('+08:00', ''),
                    "electricity": row[15],
                    "mainStationId": self.station_names.get(int(row[3]), {}).get("name", row[3]),
                    "mainStationDistance": row[9],
                    "subStationId": self.station_names.get(int(row[16]), {}).get("name", row[16]) if row[16] else row[
                        16],
                    "subStationDistance": row[17],
                    "stationDate": str(row[4]),
                    "count": row[7],
                    "duration": row[8],
                    "job": row[12],
                    "updateTimes": list({str(x) for x in row[14]}) if row[14] else [],
                }
                persons_dict[key]["records"].append(record)
                total_segments += 1

            return {
                "total": total_segments,
                "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "persons": persons_dict,
            }

        except Exception as e:
            import traceback
            print(f"查询失败: {e}\n{traceback.format_exc()}")
            return {"total": 0, "update_time": "", "persons": {}, "error": str(e)}

    def get_cars_by_filters(
            self,
            cardids: list = None,  # 车辆ID
            car_names: list = None,  # 车辆名称
            electricitys: list = None,  # 电量：['正常', '低电量']
            area_names: list = None,  # 区域名称
            departments: list = None,  # 部门
            car_types: list = None,  # 车辆类型
            start_time: str = None,
            end_time: str = None,
            main_stations: list = None,  # 新增 主站（中文名模糊）
            sub_stations: list = None,  # 新增 分站（中文名模糊）
    ):
        """
        支持全字段模糊查询。
        cardids 支持精确或模糊。只要带 % 就是 LIKE，否则 LIKE '%xxx%'
        electricitys 仅支持 ['正常', '低电量'] 这两种状态过滤。
        main_stations/sub_stations 支持模糊中文名传入，自动匹配到ID。
        其它字段皆模糊。
        """
        from datetime import datetime, time

        self._init_station_names()

        # 处理 main_stations & sub_stations（模糊匹配到ID）
        matched_main_station_ids = []
        if main_stations:
            for user_word in main_stations:
                for sid, sdict in self.station_names.items():
                    s_name = sdict.get('name', "")
                    if s_name and fuzz.partial_ratio(user_word, s_name) > 70:
                        matched_main_station_ids.append(sid)
            matched_main_station_ids = list(set(matched_main_station_ids))

        matched_sub_station_ids = []
        if sub_stations:
            for user_word in sub_stations:
                for sid, sdict in self.station_names.items():
                    s_name = sdict.get('name', "")
                    if s_name and fuzz.partial_ratio(user_word, s_name) > 70:
                        matched_sub_station_ids.append(sid)
            matched_sub_station_ids = list(set(matched_sub_station_ids))

        # area_names to main_station_ids (同上)
        areas = []
        if area_names:
            for key, v in self.station_names.items():
                for word in area_names:
                    if fuzz.partial_ratio(word, v.get("name", "")) > 70:
                        areas.append(key)
                        break  # 一个区域名字只需加入一次
            areas = list(set(areas))

        now = datetime.now()
        if not start_time:
            start_time = datetime.combine(now.date(), time.min).strftime("%Y-%m-%d %H:%M:%S")
        if not end_time:
            end_time = datetime.combine(now.date(), time.max).strftime("%Y-%m-%d %H:%M:%S")

        where_conditions = [
            "ENTRY_TIME >= %(start_time)s",
            "ENTRY_TIME < %(end_time)s"
        ]
        params = {"start_time": start_time, "end_time": end_time}

        # 卡号过滤
        if cardids:
            cardid_clauses = []
            for i, cid in enumerate(cardids):
                key = f"cardid_{i}"
                if isinstance(cid, str) and "%" in cid:
                    cardid_clauses.append(f"CARD_ID LIKE %({key})s")
                    params[key] = f"{cid}"
                else:
                    cardid_clauses.append(f"CARD_ID LIKE %({key})s")
                    params[key] = f"%{cid}%"
            where_conditions.append(f"({' OR '.join(cardid_clauses)})")

        # 电量
        if electricitys:
            statuses = set([str(x) for x in electricitys])
            normal_selected = "正常" in statuses
            low_selected = "低电量" in statuses
            other_selected = "其他" in statuses

            elec_clauses = []
            sub_statuses = []
            if normal_selected:
                sub_statuses.append("正常")
            if low_selected:
                sub_statuses.append("低电量")
            if sub_statuses:
                placeholder = []
                for idx, val in enumerate(sub_statuses):
                    params[f"elec_status_{idx}"] = val
                    placeholder.append(f"%({f'elec_status_{idx}'})s")
                elec_clauses.append(f"ELECTRICITY IN ({', '.join(placeholder)})")

            if other_selected:
                other_condition = "ELECTRICITY NOT IN (%(elec_not_normal)s, %(elec_not_low)s)"
                params["elec_not_normal"] = "正常"
                params["elec_not_low"] = "低电量"
                elec_clauses.append(other_condition)

            if elec_clauses:
                where_conditions.append(f"({' OR '.join(elec_clauses)})")

        # 按主站ID过滤
        if matched_main_station_ids:
            placeholders = []
            for idx, sid in enumerate(matched_main_station_ids):
                params[f"mainStation_id_{idx}"] = sid
                placeholders.append(f"%({f'mainStation_id_{idx}'})s")
            where_conditions.append(f"(MAIN_STATION_ID IN ({', '.join(placeholders)}))")
        # 按分站ID过滤
        if matched_sub_station_ids:
            placeholders = []
            for idx, sid in enumerate(matched_sub_station_ids):
                params[f"subStation_id_{idx}"] = sid
                placeholders.append(f"%({f'subStation_id_{idx}'})s")
            where_conditions.append(f"(SUB_STATION_ID IN ({', '.join(placeholders)}))")

        # 其它模糊字段
        fuzzy_filters = {
            "CAR_NAME": (car_names, "cname"),
            "MAIN_STATION_ID": (areas, "area"),
            "DEPARTMENT": (departments, "dept"),
            "CAR_TYPE_NAME": (car_types, "ctype"),
        }
        for col, (val_list, prefix) in fuzzy_filters.items():
            # 如果已用主/分站 in，则跳过这个字段，否则用模糊
            if col == "MAIN_STATION_ID" and (matched_main_station_ids or matched_sub_station_ids):
                continue
            if val_list:
                clauses = []
                for i, val in enumerate(val_list):
                    key = f"{prefix}_{i}"
                    clauses.append(f"{col} LIKE %({key})s")
                    params[key] = f"%{val}%"
                where_conditions.append(f"({' OR '.join(clauses)})")

        # 拼接SQL
        where_sql = " AND ".join(where_conditions)
        query = f"""
            SELECT 
                CARD_ID,                   -- 0
                CAR_NAME,                  -- 1
                DEPARTMENT,                -- 2
                CAR_TYPE_NAME,             -- 3
                ELECTRICITY,               -- 4
                MAIN_STATION_ID,           -- 5
                SUB_STATION_ID,            -- 6
                min(ENTER_TIME),           -- 7
                max(ENTRY_TIME),           -- 8
                MAIN_STATION_DISTANCE,     -- 9
                SUB_STATION_DISTANCE       -- 10
            FROM PS.SYG_RYDW_CAR_LOCATION
            WHERE {where_sql}
            GROUP BY 
                CARD_ID, CAR_NAME, DEPARTMENT, CAR_TYPE_NAME, ELECTRICITY, MAIN_STATION_ID, SUB_STATION_ID, MAIN_STATION_DISTANCE, SUB_STATION_DISTANCE
            ORDER BY max(ENTRY_TIME) DESC
        """

        try:
            result = self.client.query(query, parameters=params)

            cars_dict = {}
            total_segments = 0

            for row in result.result_rows:
                card_id = row[0]
                # 参照 (1939-1983)：按车分组，每辆车是卡号结构体下挂 records 列表，每个 record 是一个 segment
                if card_id not in cars_dict:
                    cars_dict[card_id] = {
                        "carId": card_id,
                        "carName": row[1],
                        "department": row[2],
                        "carType": row[3],
                        "records": [],
                    }

                record = {
                    "electricity": row[4],
                    "mainStationId": self.station_names.get(int(row[5]), {}).get("name", row[5]) if row[5] else row[5],
                    "subStationId": self.station_names.get(int(row[6]), {}).get("name", row[6]) if row[6] else row[6],
                    "segmentStartTime": str(row[7]) if row[7] is not None else "",
                    "segmentEndTime": str(row[8]) if row[8] is not None else "",
                    "mainStationDistance": row[9] if len(row) > 9 else None,
                    "subStationDistance": row[10] if len(row) > 10 else None,
                }
                cars_dict[card_id]["records"].append(record)
                total_segments += 1

            return {
                "total": total_segments,
                "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "cars": cars_dict,
            }

        except Exception as e:

            logger.error(f"车辆查询失败: {e}\n{traceback.format_exc()}")

            return {"total": 0, "update_time": "", "cars": {}, "error": str(e)}

    def get_person_latest(self, name: str = None, cardid: str = None) -> Optional[Dict]:
        """获取某人最新一条历史记录（支持按姓名或卡号查询）"""

        if not name and not cardid:
            return {"success": False, "message": "必须提供 name 或 cardid 参数"}

        # 构建查询条件
        where_clause = ""
        parameters = {}

        if cardid:
            where_clause = "WHERE CARDID = %(cardid)s"
            parameters = {"cardid": cardid}
        else:
            where_clause = "WHERE NAME = %(name)s"
            parameters = {"name": name}

        query = GET_PERSON_LATEST_SQL.format(where_clause=where_clause)

        result = self.client.query(query, parameters=parameters).result_rows

        if not result:
            return {
                "success": False,
                "message": f"未找到该人员的记录（{'CARDID: ' + cardid if cardid else 'NAME: ' + name}）"
            }

        self._init_station_names()

        row = result[0]

        main_name = self.station_names.get(int(row[8]), {}).get("name") if int(row[8]) in self.station_names else row[8]
        sub_name = self.station_names.get(int(row[11]), {}).get("name") if int(row[11]) in self.station_names else row[
            11]

        # 明确列索引含 MAINSTATIONID, SUBSTATIONID, 并返回
        return {
            "success": True,
            "name": row[0],
            "department": row[1],
            "classTimeName": row[2],
            "dutyName": row[3],
            "workType": row[4],
            "areaName": row[5],
            "mainStationTime": str(row[6]) if row[6] is not None else None,
            "mainStationDistance": row[7],
            "mainStationId": main_name,  # MAINSTATIONID
            "subStationTime": str(row[9]) if row[9] is not None else None,
            "subStationDistance": row[10],
            "subStationId": sub_name,  # SUBSTATIONID
            "areaTime": str(row[12]) if row[12] is not None else None,
            "enterTime": str(row[13]) if row[13] is not None else None,
            "updateTime": str(row[14]) if row[14] is not None else None,
            "cardId": row[15],  # CARDID
        }

    def get_time_stats(self, time_changes: List[datetime]) -> Dict:
        """
        从时间列表中计算最早时间、最新时间和持续秒数

        参数:
            time_changes: datetime 对象列表（可带时区或不带）

        返回:
            {
                "earliest": "2026-04-02 08:55:46",     # 最早时间（字符串）
                "latest": "2026-04-02 10:16:00",       # 最新时间（字符串）
                "duration_seconds": 4824,              # 持续秒数
                "earliest_dt": datetime_obj,           # 原始最早 datetime 对象
                "latest_dt": datetime_obj              # 原始最新 datetime 对象
            }
        """
        return get_time_stats(time_changes)

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

        if name == '':
            card_id = ''
        else:
            name2cardid, cardid2name = self.get_person_name_cardid_dicts()
            card_id = name2cardid[name]
        url = self.up_down_api_url
        payload = {
            "beginTime": begin_time,
            "endTime": end_time,
            "cardID": card_id
        }
        headers = {
            "Content-Type": "application/json"
        }
        try:
            response = requests.post(url, data=json.dumps(payload), headers=headers, timeout=120, verify=False)
            text = response.content.decode("utf-8-sig")
            result = json.loads(text).get("data", []) if response.status_code == 200 else []
            return result
        except Exception as e:
            logger.error(f"fetch_in_out_mine_records failed: {e}")
            return []

    def get_person_trajectory_with_stay(self, name: str, start_time: str, end_time: str):
        """
        查询指定姓名在指定时间段内的多卡人员轨迹。返回每个cardid为单独分组。
        返回格式举例见上文
        """
        query = GET_PERSON_TRAJECTORY_SQL
        query2 = GET_REALTIME_STATION_HEAD_INFO_SQL
        try:
            result = self.client.query(query, parameters={
                "name": name,
                "start": start_time,
                "end": end_time
            })

            result2 = self.client.query(query2, parameters={})
            station_map = {
                str(row[0]): (row[1], row[2])
                for row in result2.result_rows
            }

            # cardid -> {
            #    dept, workType, job, segments:[]
            # }
            card_dict = {}

            for row in result.result_rows:
                dept, w_type, job_title, card_id = row[10], row[11], row[12], str(row[13])

                # 保证每个card_id有基本信息
                if card_id not in card_dict:
                    card_dict[card_id] = {
                        "cardId": card_id,
                        "department": dept,
                        "workType": w_type,
                        "job": job_title,
                        "segments": []
                    }

                time_list = list(row[14])
                station_id = str(row[3])
                station_name = (
                    station_map[station_id][0]
                    if station_id in station_map
                    else f"主分站id{station_id},分站信息未入库"
                )
                stats = self.get_time_stats(time_list)
                try:
                    segment = {
                        # "areaName": row[1], # 如有需求可补充
                        "classTimeName": row[2],
                        "mainStationId": station_name,
                        "segmentStartTime": stats["earliest"],
                        "segmentEndTime": stats["latest"],
                        "segmentDurationSeconds": stats["duration_seconds"],
                        "areaChanges": [str(d) for d in list(row[9])],
                        "recordCount": int(row[7])
                    }
                    card_dict[card_id]["segments"].append(segment)
                except Exception as err:
                    logger.error(f"Error appending segment for row: {row}, error: {err}")

            # 按返回要求格式化
            cards_out = []
            for c in card_dict.values():
                c["total_segments"] = len(c["segments"])
                c['name'] = name + '_' + c['cardId']
                cards_out.append(c)

            res = {
                "name": name,
                "total_cards": len(cards_out),
                "cards": cards_out
            }

            return res

        except Exception as e:
            import traceback
            logger.error(f"分段轨迹查询失败: {e}\n{traceback.format_exc()}")
            return {"error": "查询失败", "message": str(e)}

    def get_today_persons(self) -> List[str]:
        """获取今天出现过的人员名单（去重）"""
        query = GET_TODAY_PERSONS_SQL

        result = self.client.query(query).result_rows
        return result

