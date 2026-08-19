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


current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
grandparent_dir = os.path.dirname(parent_dir)

for path in [current_dir, parent_dir, grandparent_dir]:
    if path not in sys.path:
        sys.path.append(path)
from sqls.persons_sqls import (
    query_person_history
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
        # self._init_station_names()
     
        self.start_auto_analysis_thread()


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
            
            # 修复: 防止 start_time 为 tuple
            if isinstance(start_time, tuple):
                use_start_time = start_time[0]
            else:
                use_start_time = start_time

            start_day = datetime.strptime(use_start_time, "%Y-%m-%d %H:%M:%S").strftime("%Y-%m-%d")
            res = {}
            data = query_person_history(
                    self.client,
                    start_time = start_time,
                    end_time = end_time
                )
            return data
        
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
                    # 删除60天之前的缓存数据
                    db_path = os.path.join(os.path.dirname(__file__), "person_analysis_cache.db")
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()
                    cutoff_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
                    
                    cursor.execute(
                        "DELETE FROM person_atom_cache WHERE day_str < ?",
                        (cutoff_date,)
                    )
                    deleted = cursor.rowcount
                    conn.commit()
                    conn.close()
                    self.logger.info(f"自动分析: 已删除60天前的缓存数据, cutoff={cutoff_date}, deleted={deleted}")
         
                except Exception as e:
                    self.logger.info(f"自动分析: print_tongfeng_today_with_cache 异常: {e}")
            except Exception as e:
                self.logger.info(f"自动分析: 总体异常: {e}")
                
    def get_person_infos_daytype_with_cache(
        self, 
        person_name_filters: Union[List[str], str, None] = None,
        department_filters: Union[List[str], str, None] = None,
        worktype_filters: Union[List[str], str, None] = None,
        duty_filters: Union[List[str], str, None] = None,
        area_filters: Union[List[str], str, None] = None,
        station_filters: Union[List[str], str, None] = None,
        numeric_filters: Optional[Dict[str, Dict]] = None,
        statistics_filter: Union[List[str], str, None] = None,
        start_date: Union[str, datetime, None] = None,
        end_date: Union[str, datetime, None] = None,
        now_or_today: bool = False,
    ) -> Dict:
        """
        修改逻辑：
        1. 近7天的数据：强制重新查询，不使用缓存，也不写入缓存。
        2. 7天以前的数据：正常走缓存逻辑（读+写）。
        """
        try:
            self.logger.info(f"Starting data fetch with 7-day fresh logic...")
            
            # --- 1. 初始化数据库连接 ---
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

            # --- 2. 解析日期范围 ---
            def parse_dt(dt):
                if not dt: return datetime.now()
                if isinstance(dt, datetime): return dt
                if isinstance(dt, str):
                    try: return datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
                    except Exception: return datetime.strptime(dt, "%Y-%m-%d")
                return datetime.now()

            s_dt = parse_dt(start_date) if start_date else datetime.now()
            e_dt = parse_dt(end_date) if end_date else s_dt
            
            # 处理 end_date 为 00:00:00 的边界情况
            if isinstance(end_date, str):
                dt_parts = end_date.strip().split(' ')
                if len(dt_parts) == 2 and dt_parts[1] in ("00:00:00", "00:00:00.000"):
                    try:
                        day = datetime.strptime(dt_parts[0], "%Y-%m-%d").date()
                        prev_day = day - timedelta(days=2) # 保持原有逻辑
                        end_date_new = f"{prev_day.strftime('%Y-%m-%d')} 23:59:59"
                        e_dt = parse_dt(end_date_new)
                    except Exception: pass
            
            now = datetime.now()
            if e_dt > now: e_dt = now
            if s_dt > e_dt: s_dt = e_dt

            # 生成请求的日期列表
            req_dates = []
            curr_d = s_dt.date()
            while curr_d <= e_dt.date():
                req_dates.append(curr_d.strftime("%Y-%m-%d"))
                curr_d += timedelta(days=1)

            # --- 3. 核心逻辑：区分“近7天”和“7天前” ---
            # 计算7天前的日期字符串 (例如今天是28号，threshold就是21号)
            threshold_date_str = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
            
            final_output = {} # 存储最终结果
            missed_days = []  # 需要去接口查询的日期（包含所有近7天 + 缓存未命中的历史天）

            # 第一步：处理“7天前”的历史数据（走缓存）
            for day in req_dates:
                if day < threshold_date_str:
                    # 【历史数据】尝试读缓存
                    cursor.execute('SELECT result_json FROM person_atom_cache WHERE day_str=?', (day,))
                    rows = cursor.fetchall()
                    
                    if rows and len(rows) > 0 and rows != [('"{\'当日无数据\'}"',)]:
                        try:
                            data = rows[0]
                            day_data = json.loads(data[0])
                            final_output[day] = day_data
                        except Exception:
                            self.logger.error(f"cache-load failed for {day}: JSON decode error")
                            final_output[day] = {}
                            missed_days.append(day) # 解析失败也重新查
                    else:
                        # 缓存未命中
                        missed_days.append(day)
                else:
                    # 【近7天数据】强制标记为 missed，不走缓存读取
                    missed_days.append(day)

            # 第二步：对 missed_days 进行接口查询
            self.logger.info(f'missed_days (needs API fetch)-----{missed_days}')
            already_fetched_days = set()
            
            for day in missed_days:
                if day in final_output: continue # 防止重复处理

                # 确定查询的时间范围
                if now_or_today:
                    fetch_start = start_date if isinstance(start_date, str) else s_dt.strftime("%Y-%m-%d %H:%M:%S")
                    fetch_end = end_date if isinstance(end_date, str) else e_dt.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    # 保持原有逻辑：查询当天前后各2天的数据
                    fetch_start = (datetime.strptime(day, "%Y-%m-%d") - timedelta(days=2)).strftime("%Y-%m-%d 00:00:00")
                    fetch_end = (datetime.strptime(day, "%Y-%m-%d") + timedelta(days=2)).strftime("%Y-%m-%d 23:59:59")

                try:
                    fetch_result = self.get_persons_by_filters(start_time=fetch_start, end_time=fetch_end)
                except Exception as e:
                    self.logger.error(f"fetch_result error for {day}: {e}")
                    fetch_result = {}

                # 处理查询结果
                if isinstance(fetch_result, dict):
                    if not fetch_result:
                        # 接口返回空数据
                        final_output[day] = {}
                        # 【关键修改】只有 7天以前 的数据才写入数据库
                        if day < threshold_date_str:
                            try:
                                cursor.execute('INSERT OR REPLACE INTO person_atom_cache (day_str, result_json) VALUES (?, ?)', 
                                            (day, json.dumps({"当日无数据"}, ensure_ascii=False, default=str)))
                                conn.commit()
                                self.logger.info(f"cache updated (empty) for history day {day}")
                            except Exception as e:
                                self.logger.error(f"cache-write failed: {e}")
                    else:
                        # 接口返回有数据
                        # fetch_result 可能包含多天数据，需遍历
                        for k, day_data in fetch_result.items():
                            if k == day and k not in already_fetched_days:
                                final_output[k] = day_data
                                already_fetched_days.add(k)
                                
                                # 【关键修改】只有 7天以前 的数据才写入数据库
                                if k < threshold_date_str:
                                    try:
                                        cursor.execute('INSERT OR REPLACE INTO person_atom_cache (day_str, result_json) VALUES (?, ?)', 
                                                    (k, json.dumps(day_data, ensure_ascii=False, default=str)))
                                        conn.commit()
                                        self.logger.info(f"cache updated for history day {k}")
                                    except Exception as e:
                                        self.logger.error(f"cache-write failed: {e}")

                    # 确保当天即使没数据也有个空dict占位
                    if day not in final_output:
                        final_output[day] = {}

            # --- 4. 数据过滤与返回 ---
            filtered_output = {}
            for day, day_dict in final_output.items():
                data_out = self.person_filter(
                    day_dict, person_name_filters=person_name_filters,
                    department_filters=department_filters, worktype_filters=worktype_filters,
                    duty_filters=duty_filters, area_filters=area_filters,
                    station_filters=station_filters, numeric_filters=numeric_filters,
                    statistics_filter=statistics_filter
                )
                filtered_output[day] = data_out if len(data_out) > 0 else {}

            final_result = {"每日数据": filtered_output, "总共天数": len(filtered_output)}
            conn.close()
            self.logger.info(f"get_person_infos_daytype_with_cache completed. Total {len(final_result)} days.")
            return final_result

        except Exception as e:
            self.logger.error(f"get_person_infos_daytype_with_cache failed: {e} \n{traceback.format_exc()}")
            return {}

    # 3. 所有过滤全部在读取时做 (多线程加速改进)
    def person_filter(
            self,
            pdata,
            person_name_filters: Union[List[str], str, None] = None,  # 姓名
            department_filters: Union[List[str], str, None] = None,  # 队组班组/部门
            worktype_filters: Union[List[str], str, None] = None,  # 工种
            duty_filters: Union[List[str], str, None] = None,  # 职位
           
            area_filters: Union[List[str], str, None] = None,  # 区域筛选
            station_filters: Union[List[str], str, None] = None,  # 基站筛选
            numeric_filters: Optional[Dict[str, Dict]] = None,  #
            statistics_filter: Union[List[str], str, None] = None,
    ):
        try:
            person_name_filters = normalize(person_name_filters)
            department_filters = normalize(department_filters)
            worktype_filters = normalize(worktype_filters)
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
                    if not fuzzy_match(person.get("队组班组/部门", ""), department_filters):
                        return None
                if worktype_filters:
                    if not fuzzy_match(person.get("职务", ""), worktype_filters):
                        return None
                if worktype_filters:
                    if not fuzzy_match(person.get("工种", ""), worktype_filters):
                        return None
               
                # 处理 numeric_filters 中特殊key
                special_keys = ["学历", "是否矿领导", "是否特种人员", "出生年月","工作状态","当日是否已出井"]
                
                            
                if numeric_filters:
                    keep_inners = []
                    for key in special_keys:
                        if key in numeric_filters:
                            condition = numeric_filters[key]
                            value = person.get(key, None)
                            out = check_numeric_condition(value, condition,field_name=key)
                            if not out:
                                return None

                # ===== segments_grouped内部过滤 =====
                has_segments = False
                use_segments_grouped = []

                for segments in person.get("出入井记录", []):
                    keep = True

                    # 根据要求，加入 入井时间、出井时间、入井时长 的字段过滤支持
                    if keep and numeric_filters:
                        # 入井时间
                        if "入井时间" in numeric_filters:
                            entry_time = segments.get("入井时间")
                            keep_t = check_numeric_condition(entry_time, numeric_filters["入井时间"], field_name="入井时间")
                            if not keep_t:
                                keep = False
                        # 出井时间
                        if "出井时间" in numeric_filters:
                            exit_time = segments.get("出井时间")
                            keep_t = check_numeric_condition(exit_time, numeric_filters["出井时间"], field_name="出井时间")
                            if not keep_t:
                                keep = False
                        # 入井时长
                        if "入井时长" in numeric_filters:
                            entry_duration = segments.get("入井时长")
                            keep_t = check_numeric_condition(entry_duration, numeric_filters["入井时长"], field_name="入井时长")
                            if not keep_t:
                                keep = False
       


                    if not keep:
                        continue

                    filtered_records = []
                    for record in segments.get("具体轨迹变化", []):
                        keep_inner = True

                        if keep_inner and station_filters:
                            v1 = record.get("基站")
                            if not fuzzy_match(v1, station_filters, 80) :
                                keep_inner = False

                        if keep_inner and area_filters:
                            if not fuzzy_match(record.get("区域"), area_filters):
                                keep_inner = False

                        # === 新增：数值过滤（核心）===
                        # 仅针对指定的四个字段：开始时间、结束时间、距离变化、持续时间，进行数值过滤
                        filter_fields = ["轨迹开始时间", "轨迹结束时间", "轨迹距离变化", "持续时间"]
                        if keep_inner and numeric_filters:
                            for field_name in filter_fields:
                                if field_name not in numeric_filters:
                                    continue
                                condition = numeric_filters[field_name]
                                rec_data = record.get(field_name, -1)
                                if rec_data == -1:
                                    continue
                                if isinstance(rec_data, list):
                                    # 对于list（如 距离变化），只要有任一满足即可
                                    if not any(check_numeric_condition(val, condition, field_name=field_name) for val in rec_data):
                                        keep_inner = False
                                        break
                                else:
                                    if not check_numeric_condition(rec_data, condition, field_name=field_name):
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
        host="10.11.3.210",
        port=8123,
        database="PS",
        username="default",
        password="xt123456",
        autogenerate_session_id = False
    )

    person_util = PersonBase(client)

    numeric_filters = {

        "学历": {
            "op": "in",
            "value": ["小学","初中", "高中", "专科", "中专", "大专", "本科", "硕士", "博士"]

        },
        "是否矿领导": {
            "op": "==",
            "value": "否"  # 是/否
        },
        "是否特种人员": {
            "op": "==",
            "value": "否"   # 是/否
        },
        "出生年月": {
            "op": ">=",
            "value": "1990-01-01"
        },

        "工作状态": {
            "op": "==",
            "value": "正常"  # 正常/求救
        },
        
        "当日是否已出井": {
            "op": "!=",
            "value": "已出井"  # 已出井 / 井下 / 井口
        },

        "入井时间": {
            "op": ">=",
            "value": "2026-07-14 08:00:00"
        },
        
        "出井时间": {
            "op": "<=",
            "value": "2026-07-14 18:00:00"
        },
        
        "入井时长": {
            "op": ">",
            "value": "05:00:00"  # 1小时
        },

        "轨迹持续时间": {
            "op": ">",
            "value": "01:00:00"  # 1小时
        },
        
        "轨迹距离变化": {
            "op": ">",
            "value": 40
        },

        "轨迹开始时间": {
            "op": "between",
            "value": ["2026-07-14 14:00:00", "2026-07-14 18:00:00"]
        },
        
        "轨迹结束时间": {
            "op": ">",
            "value": "2026-07-14 15:00:00"
        },
       
    }

   
    # 列表列出所有stats的key
    # stats_keys 来自 base_utils.py generate_statistics 返回值中的key，保持一致
    statistics_filters = [
        "总人数",
        "人员列表_姓名_卡号_入井次数",
        "入井时长分布/人次",
        "入井时间段分布/人次",
        "出井时间段分布/人次",
        "区域分布/条",
        "基站分布/条",
        "基站停留时长分布/条",
        "部门分布/人",
        "职位分布/人",
        "工种分布/人",
    ]
    
    
    numeric_filters = {
        "当日是否已出井": {
            "op": "!=",
            "value": "已出井"  # 已出井 / 井下 / 井口
        },
    }
    # print("统计字段 keys:", stats_keys)
    statistics_filter_values = ["总人数",
        "人员列表_姓名_卡号_入井次数",
        "入井时长分布/人次",
        "入井时间段分布/人次",
        "出井时间段分布/人次",
        "区域分布/条",
        "基站分布/条",
        "基站停留时长分布/条",
        "部门分布/人",
        "职位分布/人",
        "工种分布/人",]
    # numeric_filters = {
    #      "轨迹开始时间": {
    #         "op": ">",
    #         "value": "2026-07-27 10:00:00"
    #     },
    # }
    daytype_data = person_util.get_person_infos_daytype_with_cache(
        person_name_filters=None,
        department_filters=None,
        worktype_filters=None,
        duty_filters=None,
        area_filters=None,
        station_filters=None,
        # numeric_filters = numeric_filters,
        # statistics_filter = ['人员列表_姓名_卡号_入井次数', '入井时长分布/人次'],
        statistics_filter = ['all'],
        start_date="2026-08-06  00:00:00",
        end_date = "2026-08-07  23:59:59"
    )
    print(daytype_data)
    daytype_json = json.dumps(daytype_data, ensure_ascii=False, indent=2)
    print("json长度:", len(daytype_json))

