#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件名:\tqyjc_bases.py
作者:\tshihongyu
创建日期:\t2026-09-09
描述:\t淖尔壕煤矿 安全监控(AQJC) 数据查询与过滤基类, 供 tools 与 utils 共用。
      设计参考 HJL_agent/utils/person_utils/person_bases.py:
        - 封装通用 logger / client
        - 按天缓存(近7天强制实时查询, 7天前走 sqlite 缓存), 后台线程每日预热并清理
        - 过滤全部在读取时做(多线程), 支持测点级/时序级 数值与时间过滤
      数据源: ClickHouse, database = PS_NAO
"""
import os
import sys
import time
import json
import logging
import sqlite3
import threading
import traceback
from copy import deepcopy
from bisect import bisect_left
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union

import concurrent.futures

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)
for path in [current_dir, parent_dir, grandparent_dir]:
    if path not in sys.path:
        sys.path.append(path)

from sqls.AQJC_sqls import (
    query_fzdy_info,
    query_cddy_info,
    query_fz_history,
    query_cd_realtime,
    query_yb_info,
)
from base_utils import (
    check_numeric_condition,
    normalize,
    fuzzy_match,
    generate_statistics,
)

STRFTIME_FMT = "%Y-%m-%d %H:%M:%S"
CACHE_DB = os.path.join(current_dir, "aqjc_analysis_cache.db")

# 时序表粒度 -> 保留窗口(天)
CDSS_1D_DAYS = 2          # 1D 表只保留最近约1天
CDSS_30D_DAYS = 30        # 30D 表保留近30天
CDSS_2Y_DAYS = 2 * 365    # 2Y 归档表


class AQjcBase:
    """
    安全监控(AQJC)相关功能的基类。
    封装测点时序查询(按天+缓存)、测点信息/分站状态查询、以及读取时过滤与统计。
    实际业务类(MCP service)可继承它并覆盖 _fetch_window_points 定制查询。
    """

    def __init__(self, client, logger=None, auto_analysis: bool = True):
        if logger is None:
            self.logger = logging.getLogger("qyjc_bases")
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
            if not self.logger.handlers:
                self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        else:
            self.logger = logger

        self.client = client

        if auto_analysis:
            self.start_auto_analysis_thread()

    # ==================== 基础字典 / 分站状态 查询 ====================
    def get_fzdy_info(self, where_clause=None) -> Dict:
        """分站基本信息字典 {分站编码: {...中文}}"""
        try:
            return query_fzdy_info(self.client, where_clause)
        except Exception as e:
            self.logger.error(f"get_fzdy_info failed: {e} \n{traceback.format_exc()}")
            return {}

    def get_cddy_info(self, where_clause=None) -> Dict:
        """测点基本信息字典 {测点编码: {...中文, 含门限/量程}}"""
        try:
            fz = query_fzdy_info(self.client) 
            return query_cddy_info(self.client,fz_dict=fz, where_clause=where_clause)
        except Exception as e:
            self.logger.error(f"get_cddy_info failed: {e} \n{traceback.format_exc()}")
            return {}

    def get_fz_status(self, realtime: bool = True, where_clause=None) -> Dict:
        """分站运行/供电状态(状态岛聚合) {分站编码: [时段...]}"""
        try:
            fn = query_fz_history
            return fn(self.client, where_clause)
        except Exception as e:
            self.logger.error(f"get_fz_status failed: {e} \n{traceback.format_exc()}")
            return {}

    def get_yb_info(self, point_code=None, start_time=None, end_time=None) -> List[Dict]:
        """测点异常信息列表"""
        try:
            return query_yb_info(self.client, point_code, start_time, end_time)
        except Exception as e:
            self.logger.error(f"get_yb_info failed: {e} \n{traceback.format_exc()}")
            return []

    # ==================== 时序窗口查询(可被业务类覆盖) ====================
    def _fetch_window_points(self, start_time: str, end_time: str, point_code=None) -> Dict:
        """
        查询 [start_time, end_time] 窗口内测点时序, 按天分组。
        默认走 30D 表; 业务类可覆盖以支持更长历史(自动切 2Y 表)。
        :return: {day_str: {point_code: {测点信息..., "数据":[...]}}}
        """
        try:
            return query_cd_realtime(self.client, point_code=point_code, start_time=start_time, end_time=end_time)
        except Exception as e:
            self.logger.error(f"_fetch_window_points failed: {e} \n{traceback.format_exc()}")
            return {}

    def _fetch_day_points(self, day: str, point_code=None) -> Dict:
        """
        查询某一天(前后各扩2天以覆盖表粒度边界), 返回 {day: {...}} 或 {}。
        按天数自动选择表粒度: 近2天->1D, 近30天->30D, 更早->2Y。
        """
        try:
            day_dt = datetime.strptime(day, "%Y-%m-%d")
            # age_days = (datetime.now().date() - day_dt.date()).days
            # if age_days <= CDSS_1D_DAYS:
            #     table = "ODS_AQJK_CDSS_1D"
            # elif age_days <= CDSS_30D_DAYS:
            #     table = "ODS_AQJK_CDSS_30D"
            # else:
            table = "ODS_AQJK_CDSS_2Y"
            fetch_start = day_dt.strftime("%Y-%m-%d 00:00:00")
            fetch_end = day_dt.strftime("%Y-%m-%d 23:59:59")
            data = query_cd_realtime(
                self.client, point_code=point_code, start_time=fetch_start, end_time=fetch_end, table_name=table
            )
            return {day: data[day]} if day in data else {}
        except Exception as e:
            self.logger.error(f"_fetch_day_points failed for {day}: {e} \n{traceback.format_exc()}")
            return {}

    # ==================== 后台自动分析线程 ====================
    def start_auto_analysis_thread(self):
        """启动后台线程, 每日定时预热缓存并清理过期数据。"""
        try:
            thread = threading.Thread(target=self._run_daily_auto_analysis, daemon=True)
            thread.start()
            return thread
        except Exception as e:
            self.logger.error(f"start_auto_analysis_thread 启动失败: {traceback.format_exc()}")
            return None

    def _run_daily_auto_analysis(self):
        """
        每天凌晨2点: 预热近30天缓存, 并删除 60 天前的缓存数据。
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
                    self.get_points_daytype_with_cache(start_date=start_date, end_date=end_date)
                except Exception as e:
                    self.logger.info(f"自动分析: 预热异常: {e}")
                # 清理60天前缓存
                try:
                    conn = sqlite3.connect(CACHE_DB)
                    cursor = conn.cursor()
                    cursor.execute("CREATE TABLE IF NOT EXISTS point_atom_cache (day_str TEXT PRIMARY KEY, result_json TEXT)")
                    cutoff_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
                    cursor.execute("DELETE FROM point_atom_cache WHERE day_str < ?", (cutoff_date,))
                    deleted = cursor.rowcount
                    conn.commit()
                    conn.close()
                    self.logger.info(f"自动分析: 已删除60天前的缓存数据, cutoff={cutoff_date}, deleted={deleted}")
                except Exception as e:
                    self.logger.info(f"自动分析: 缓存清理异常: {e}")
            except Exception as e:
                self.logger.info(f"自动分析: 总体异常: {e}")

    # ==================== 按天缓存查询(核心) ====================
    def get_points_daytype_with_cache(
        self,
        point_code_filters: Union[List[str], str, None] = None,
        sensor_type_filters: Union[List[str], str, None] = None,
        location_filters: Union[List[str], str, None] = None,
        substation_code_filters: Union[List[str], str, None] = None,
        numeric_filters: Optional[Dict[str, Dict]] = None,
        statistics_filter: Union[List[str], str, None] = None,
        start_date: Union[str, datetime, None] = None,
        end_date: Union[str, datetime, None] = None,
        now_or_today: bool = False,
    ) -> Dict:
        """
        按天分组查询测点时序数据, 带 7天新鲜度逻辑:
          1. 近7天数据: 强制重新查询, 不读不写缓存;
          2. 7天前数据: 正常走 sqlite 缓存(读+写)。
        :return: {"每日数据": {day: {point_code: {...}}}, "总共天数": N}
        """
        try:
            # --- 1. 初始化缓存表 ---
            conn = sqlite3.connect(CACHE_DB)
            cursor = conn.cursor()
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS point_atom_cache (
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
                    s = dt.strip()
                    for fmt in (STRFTIME_FMT, "%Y-%m-%d"):
                        try:
                            return datetime.strptime(s, fmt)
                        except Exception:
                            continue
                return datetime.now()

            s_dt = parse_dt(start_date) if start_date else datetime.now()
            e_dt = parse_dt(end_date) if end_date else s_dt

            now = datetime.now()
            if e_dt > now:
                e_dt = now
            if s_dt > e_dt:
                s_dt = e_dt

            # 生成日期列表
            req_dates = []
            curr_d = s_dt.date()
            while curr_d <= e_dt.date():
                req_dates.append(curr_d.strftime("%Y-%m-%d"))
                curr_d += timedelta(days=1)

            # --- 2. 区分 近7天 / 历史 ---
            threshold_date_str = (datetime.now() - timedelta(days=3)).strftime("%Y-%m-%d")
            final_output = {}
            missed_days = []

            for day in req_dates:
                if day < threshold_date_str:
                    cursor.execute('SELECT result_json FROM point_atom_cache WHERE day_str=?', (day,))
                    rows = cursor.fetchall()
                    if rows:
                        try:
                            day_data = json.loads(rows[0][0])
                            if day_data is None:
                                day_data = {}
                            final_output[day] = day_data
                        except Exception:
                            self.logger.error(f"cache-load failed for {day}: JSON decode error")
                            missed_days.append(day)
                    else:
                        missed_days.append(day)
                else:
                    missed_days.append(day)  # 近7天强制实时

            self.logger.info(f'missed_days (needs fetch)-----{missed_days}')
            already_fetched_days = set()

            # --- 3. 对 missed_days 查询 ---
            for day in missed_days:
                if day in final_output:
                    continue
                try:
                    fetch_result = self._fetch_day_points(day, point_code=None)
                except Exception as e:
                    self.logger.error(f"fetch_result error for {day}: {e}")
                    fetch_result = {}

                day_data = fetch_result.get(day, {})
                final_output[day] = day_data

                # 只有 7天以前 的数据才写入数据库
                if day < threshold_date_str:
                    try:
                        cursor.execute(
                            'INSERT OR REPLACE INTO point_atom_cache (day_str, result_json) VALUES (?, ?)',
                            (day, json.dumps(day_data, ensure_ascii=False, default=str))
                        )
                        conn.commit()
                        self.logger.info(f"cache updated for history day {day}")
                    except Exception as e:
                        self.logger.error(f"cache-write failed: {e}")

            # --- 4. 过滤与返回 ---
            filtered_output = {}
            for day, day_dict in final_output.items():
                data_out = self.point_filter(
                    day_dict,
                    point_code_filters=point_code_filters,
                    sensor_type_filters=sensor_type_filters,
                    location_filters=location_filters,
                    numeric_filters=numeric_filters,
                    statistics_filter=statistics_filter,
                )
                filtered_output[day] = data_out if len(data_out) > 0 else {}

            conn.close()
            final_result = {"每日数据": filtered_output, "总共天数": len(filtered_output)}
            self.logger.info(f"get_points_daytype_with_cache completed. Total {len(filtered_output)} days.")
            return final_result

        except Exception as e:
            self.logger.error(f"get_points_daytype_with_cache failed: {e} \n{traceback.format_exc()}")
            return {}

    # ==================== 读取时过滤(多线程) ====================
    def point_filter(
        self,
        pdata: Dict,
        point_code_filters: Union[List[str], str, None] = None,
        sensor_type_filters: Union[List[str], str, None] = None,
        location_filters: Union[List[str], str, None] = None,
        numeric_filters: Optional[Dict[str, Dict]] = None,
        statistics_filter: Union[List[str], str, None] = None,
    ) -> Dict:
        """
        在内存中对 天->测点 数据做过滤:
          * 测点级: point_code / sensor_type / location / substation_code (模糊)
          * numeric_filters: 测点信息字段(如 上限报警门限/测点值) + 时序字段(测点值/数据时间/测点状态)
          * statistics_filter: 非空时仅返回统计结果
        """
        try:
            point_code_filters = normalize(point_code_filters)
            sensor_type_filters = normalize(sensor_type_filters)
            location_filters = normalize(location_filters)
          
            # 测点信息级 数值过滤字段(作用于测点元信息)
            meta_numeric_fields = [
                "高量程", "低量程", "上限报警门限", "上限解报门限",
                "下限报警门限", "下限解报门限", "上限断电门限", "上限复电门限",
                "下限断电门限", "下限复电门限",'平均值','中位数','标准差','最小值','最大值','趋势'
            ]
            # 时序级 数值过滤字段(作用于每条数据)
            record_numeric_fields = ["持续秒数","持续小时","起始时间","结束时间"]
            status_numeric_fields = ['状态']

            def process_point(point_item):
                point_key, point = point_item
                point = deepcopy(point)

                # ===== 测点级 模糊过滤 =====
                if point_code_filters and not fuzzy_match(point.get("测点编码", ""), point_code_filters, 95):
                    return None
                if sensor_type_filters and not fuzzy_match(point.get("传感器类型名称", ""), sensor_type_filters,70):
                    return None
                if location_filters and not fuzzy_match(point.get("测点安装位置", ""), location_filters,70):
                    return None
              
                # ===== 测点级 数值过滤(门限/量程等元信息) =====
                if numeric_filters:
                    for key in numeric_filters :
                        if key in meta_numeric_fields:
                            condition = numeric_filters[key]
                            value = point.get(key, None)
                            if not value :
                                continue
                            if not check_numeric_condition(value, condition, field_name=key):
                                return None

                # ===== 时序级 过滤 =====
                records = point.get("稳定阶段", [])
                has_record_filter = bool(numeric_filters) and any(f in numeric_filters for f in record_numeric_fields)
                if has_record_filter:
                    filtered_records = []
                    for rec in records:
                        keep = True
                        for field_name in record_numeric_fields:
                            if field_name not in numeric_filters:
                                continue
                            condition = numeric_filters[field_name]
                            rec_data = rec.get(field_name, None)
                            if rec_data is None:
                                continue
                            if isinstance(rec_data, list):
                                if not any(check_numeric_condition(val, condition, field_name=field_name) for val in rec_data):
                                    keep = False
                                    break
                            else:
                                if not check_numeric_condition(rec_data, condition, field_name=field_name):
                                    keep = False
                                    break
                        if keep:
                            filtered_records.append(rec)
                    if not filtered_records:
                        return None
                    point["稳定阶段"] = filtered_records

                # ===== 状态级 过滤 =====
                records = point.get("测点状态", [])
                has_record_filter = bool(numeric_filters) and any(f in numeric_filters for f in status_numeric_fields)
                if has_record_filter:
                    filtered_records = []
                    for rec in records:
                        keep = True
                        for field_name in status_numeric_fields:
                            if field_name not in numeric_filters:
                                continue
                            condition = numeric_filters[field_name]
                            rec_data = rec.get(field_name, None)
                            if rec_data is None:
                                continue
                            if isinstance(rec_data, list):
                                if not any(check_numeric_condition(val, condition, field_name=field_name) for val in rec_data):
                                    keep = False
                                    break
                            else:
                                if not check_numeric_condition(rec_data, condition, field_name=field_name):
                                    keep = False
                                    break
                        if keep:
                            filtered_records.append(rec)
                    if not filtered_records:
                        return None
                    point["测点状态"] = filtered_records
                return (point_key, point)

            # ===== 多线程执行 =====
            result = {}
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
                futures = [executor.submit(process_point, item) for item in pdata.items()]
                for f in concurrent.futures.as_completed(futures):
                    try:
                        res = f.result()
                        if res is not None:
                            point_key, point_val = res
                            result[point_key] = point_val
                    except Exception as e:
                        self.logger.error(f"point_filter process_point thread failed: {e} \n{traceback.format_exc()}")

            # ===== 统计 =====
            if statistics_filter is not None and len(statistics_filter) > 0:
                try:
                    stats = generate_statistics(result, statistics_filter)
                    self.logger.info(f"point_filter statistics generated for keys: {statistics_filter}")
                    return {"statistics": stats}
                except Exception as e:
                    self.logger.error(f"point_filter statistics fail: {e} \n{traceback.format_exc()}")
                    return {"statistics": {}}

            return result
        except Exception as e:
            self.logger.error(f"point_filter failed: {e} \n{traceback.format_exc()}")
            return {}


if __name__ == "__main__":
    import clickhouse_connect
    from pprint import pprint

    client = clickhouse_connect.get_client(
        host="10.11.3.210",
        port=8123,
        database="PS_NAO",
        username="default",
        password="xt123456",
        autogenerate_session_id=False
    )

    qyjc = QyjcBase(client)

    # 示例: 查询某天测点, 过滤 测点值>某值, 并输出统计
    numeric_filters = {
        "结束时间": {"op": "<=", "value": '2026-09-10 01:21:59'},
        "起始时间": {"op": ">=", "value": '2026-09-09 22:10:40'},
        "状态": {"op": "==", "value": "标校"},
    }
    statistics_filter_values = [
        "总测点数", "测点列表", "传感器类型分布/个",
        "超上限报警测点/个", "超下限报警测点/个", 
    ]

    daytype_data = qyjc.get_points_daytype_with_cache(
        # point_code_filters=['1101MN000300000273'],
        sensor_type_filters=None,
        location_filters=None,
        substation_code_filters=None,
        numeric_filters=numeric_filters,
        # statistics_filter=['all'],
        start_date="2026-09-08 00:00:00",
        end_date="2026-09-10 23:59:59",
    )
    pprint(daytype_data)
    print("json长度:", len(json.dumps(daytype_data, ensure_ascii=False)))
