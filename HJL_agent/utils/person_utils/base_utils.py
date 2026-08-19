#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文件名:	person_utils.py
作者:	shihy
创建日期:	2026-04-22
描述:	矿井人员定位相关的数据处理与工具函数。用于时间格式化、筛选、统计分析和结构化人员信息，适配 MCP 服务人员定位业务功能需求。
"""
import sys
import os
import traceback
from fuzzywuzzy import fuzz, process
from datetime import datetime
from collections import defaultdict
from collections import defaultdict, Counter

import redis
import json


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
  
    key = f"mcp:{type_name}"
    redis_client = get_redis_client()
    redis_client.set(key, json.dumps(data, ensure_ascii=False), ex=expire_seconds)

def get_type_data_from_redis(type_name: str):
    """
    按 type 名称从 redis 获取数据并反序列化
    如无数据则返回 None
    """
   
    key = f"mcp:{type_name}"
    redis_client = get_redis_client()
    raw = redis_client.get(key)
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None
    
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

 # ==================== 时间解析函数 ====================
def parse_time(t_str):
    """
    解析时间字符串，支持的时间格式包括：
    - "%Y-%m-%dT%H:%M:%S"  (如: 2026-07-01T23:52:20)
    - "%Y-%m-%d %H:%M:%S"  (如: 2026-07-02 00:04:05)
    - "%Y-%m-%d %H:%M:%S.%f"  (带毫秒)
    - "%Y-%m-%dT%H:%M:%S.%f"
    - "%Y-%m-%d"
    - "%H:%M:%S"       # 支持纯时分秒，会返回秒数（int）
    """
    if not t_str:
        return None
    t_str = str(t_str).strip()

    formats = [
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%dT%H:%M:%S.%f",
        "%Y-%m-%d",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(t_str, fmt)
        except ValueError:
            continue
    
    
    # 如果是纯 "%H:%M:%S" 格式，转为秒数
    try:
        t = datetime.strptime(t_str, "%H:%M:%S")
        return t.hour * 3600 + t.minute * 60 + t.second
    except ValueError:
        pass
    
    # 支持任意 HH:MM:SS（HH 可大于 23）
    parts = t_str.split(":")
    if len(parts) == 3:
        try:
            h = int(parts[0])
            m = int(parts[1])
            s = int(parts[2])

            if 0 <= m < 60 and 0 <= s < 60:
                return h * 3600 + m * 60 + s
        except ValueError:
            pass
        
    # ISO格式最后尝试
    try:
        return datetime.fromisoformat(t_str.replace("Z", "+00:00"))
    except:
        print(t_str, traceback.format_exc())
        return None

def check_numeric_condition(value, condition: dict, field_name: str = "") -> bool:
    """
    增强版：支持数值与多种时间格式的过滤。支持如下判断条件：

    - ">", ">="：大于/大于等于
    - "<", "<="：小于/小于等于
    - "==", "="：等于
    - "!="：不等于
    - "between"：区间内（前后闭区间）
    - "not_between"：不在区间内
    - "in"：包含于列表或集合
    - "after", "later", "since"：时间/数值大于/大于等于
    - "before", "earlier", "until"：时间/数值小于/小于等于

    同时支持字符串/数值/时间等多类型字段的匹配判断。
    """
    if value is None or not condition:
        return True
    
    if isinstance(condition, tuple):
        condition = condition[0]
       
    op = condition.get("op")
    target = condition.get("value")

    # ==================== 判断是否为时间字段 ====================
    time_keywords = ["时间", "入井", "轨迹开始", "轨迹结束","出生年月"]
    is_time_field = any(kw in field_name for kw in time_keywords)

    if is_time_field:
        try:
            parsed_value = parse_time(value)
            dt_value = parsed_value

            # 解析目标时间
            if isinstance(target, (list, tuple)):
                dt_targets = [parse_time(t) for t in target]
            else:
                dt_targets = [parse_time(target)]

            if op in [">", "after", "later"]:
                dt_target = dt_targets[0]
                return dt_value > dt_target if dt_target else False

            elif op in ["<", "before", "earlier"]:
                dt_target = dt_targets[0]
                return dt_value < dt_target if dt_target else False

            elif op in [">=", "since"]:
                dt_target = dt_targets[0]
                return dt_value >= dt_target if dt_target else False

            elif op in ["<=", "until"]:
                dt_target = dt_targets[0]
                return dt_value <= dt_target if dt_target else False

            elif op == "between":
                if len(dt_targets) >= 2 and dt_targets[0] and dt_targets[1]:
                    return dt_targets[0] <= dt_value <= dt_targets[1]
                return False

            elif op == "in":
                return dt_value in dt_targets

            return True
        except Exception:
            pass  # 解析失败则继续走数值/字符串逻辑

    # ==================== 数值处理（保持不变，加入in） ====================
    try:
        num_value = float(value)
        if op == ">":
            return num_value > target
        elif op == ">=":
            return num_value >= target
        elif op == "<":
            return num_value < target
        elif op == "<=":
            return num_value <= target
        elif op in ["==", "="]:
            return abs(num_value - target) < 1e-6
        elif op == "between":
            if isinstance(target, (list, tuple)) and len(target) == 2:
                return target[0] <= num_value <= target[1]
        elif op == "not_between":
            if isinstance(target, (list, tuple)) and len(target) == 2:
                return not (target[0] <= num_value <= target[1])
        elif op == "in":
            # target必须是可迭代；这里支持字符串也能or
            if isinstance(target, (list, tuple, set)):
                return num_value in target
            else:
                return num_value == target
        elif op == "!=":
            return str(value) != str(target)
        return True
    except (ValueError, TypeError):
        # 字符串兜底，加in
        if op == "in":
            if isinstance(target, (list, tuple, set)):
                return str(value) in [str(t) for t in target]
            else:
                return str(value) == str(target)
        elif op == "!=":
            return str(value) != str(target)
           
        return str(value) == str(target)




def sort_key(x):
    # x like "0-1小时"
    try:
        return int(x.split('-')[0])
    except:
        return 0

from collections import defaultdict, Counter
from datetime import timedelta


def generate_statistics(filtered_persons, keep_keys=["all"]):
    """
    统计指定人员数据，增加keep_keys功能：只保留keep_keys中出现的key，其他不返回。
    并增加按照 部门 职位 工种 班次 分布统计。
    区域分布/条、基站分布/条 按照同一时间为前key，地点为后key统计，统计每时点内不同人员数量。
    每人同一时间同一位置 只能算一条。每个时间段加一个总条数字段。
    新增：每小时人数统计，轨迹与某小时有交集即计入，同一人同小时只计一次。
    :param filtered_persons: 人员dict
    :param keep_keys: 仅保留的key集合或list，若为None则全部保留
    :return: stats统计字典
    """
    stats = {
        "总人数": len(filtered_persons),
        "人员列表_姓名_卡号_入井次数": [],
        "入井时长分布/人次": defaultdict(int),
        "入井时间段分布/人次": defaultdict(int),
        "出井时间段分布/人次": defaultdict(int),
        "区域分布/条": defaultdict(lambda: defaultdict(set)),
        "基站分布/条": defaultdict(set),
        "基站停留时长分布/条": defaultdict(int),
        "部门分布/人": Counter(),
        "职位分布/人": Counter(),
        "工种分布/人": Counter(),
        # === 新增：每小时人数统计 ===
        "每小时人数统计/人": defaultdict(set),  # {period_str: set(姓名)}
    }

    person_set = set()

    for person_key, person in filtered_persons.items():
        name = person.get("姓名", "")
        if name in person_set:
            continue
        person_set.add(name)

        department = person.get("队组班组/部门")
        post = person.get("职务")
        worktype = person.get("工种")

        if department:
            stats["部门分布/人"][department] += 1
        if post:
            stats["职位分布/人"][post] += 1
        if worktype:
            stats["工种分布/人"][worktype] += 1

        stats["人员列表_姓名_卡号_入井次数"].append([
            name,
            person.get("卡号"),
            len(person.get("出入井记录", []))
        ])

        for segment in person.get("出入井记录", []):
            # 入井时长分布/人次
            try:
                ts = parse_time(segment['入井时长'])
                hours = max(0, int(ts) // 3600)
                key = f"{hours}-{hours + 1}小时"
                stats["入井时长分布/人次"][key] += 1
            except Exception:
                pass

            in_time = parse_time(segment.get("入井时间"))
            out_time = parse_time(segment.get("出井时间"))

            if in_time:
                hour = in_time.hour
                next_hour = (hour + 1) % 24
                period = f"{in_time.strftime('%Y-%m-%d')} {hour:02d}-{next_hour:02d}点"
                stats["入井时间段分布/人次"][period] += 1

            if out_time:
                hour = out_time.hour
                next_hour = (hour + 1) % 24
                period = f"{out_time.strftime('%Y-%m-%d')} {hour:02d}-{next_hour:02d}点"
                stats["出井时间段分布/人次"][period] += 1

            area_seen = set()
            for record in segment.get("具体轨迹变化", []):
                area = record.get("区域")
                main = record.get("基站")
                record_in_time = parse_time(record.get("轨迹开始时间")) or in_time

                # ========== 新增：每小时人数统计核心逻辑 ==========
                track_start = parse_time(record.get("轨迹开始时间"))
                time_end = record.get("轨迹结束时间")
                if time_end == '':
                    track_end =  datetime.now()
                else:
                    track_end = parse_time(time_end)
                

                if track_start and track_end and track_start <= track_end:
                    start_hour_dt = track_start.replace(minute=0, second=0, microsecond=0)
                    end_hour_dt = track_end.replace(minute=0, second=0, microsecond=0)
                    # 结束时间恰好为整点时回退一小时，避免多算
                    if track_end == end_hour_dt and track_end > track_start:
                        effective_end_hour_dt = end_hour_dt - timedelta(hours=1)
                    else:
                        effective_end_hour_dt = end_hour_dt

                    current = start_hour_dt
                    while current <= effective_end_hour_dt:
                        h = current.hour
                        next_h = (h + 1) % 24
                        period_str = f"{current.strftime('%Y-%m-%d')} {h:02d}-{next_h:02d}点"
                        stats["每小时人数统计/人"][period_str].add(name)
                        current += timedelta(hours=1)

                elif track_start:
                    h = track_start.hour
                    next_h = (h + 1) % 24
                    period_str = f"{track_start.strftime('%Y-%m-%d')} {h:02d}-{next_h:02d}点"
                    stats["每小时人数统计/人"][period_str].add(name)

                elif track_end:
                    h = track_end.hour
                    next_h = (h + 1) % 24
                    period_str = f"{track_end.strftime('%Y-%m-%d')} {h:02d}-{next_h:02d}点"
                    stats["每小时人数统计/人"][period_str].add(name)
                # ===================================================

                # 区域分布/条（保持原有逻辑）
                period = None
                if record_in_time:
                    hour = record_in_time.hour
                    period = f"{hour:02d}-{hour + 1:02d}点"

                if period and area and (name, period, area) not in area_seen:
                    stats["区域分布/条"][period][area].add(name)
                    area_seen.add((name, period, area))

                # 基站分布
                if main:
                    stats["基站分布/条"][main].add(name)

                # 基站停留时长分布/条
                stay_time = parse_time(record.get("轨迹持续时间"))
                try:
                    if stay_time is not None:
                        try:
                            stay_sec = int(stay_time)
                        except Exception:
                            stay_sec = 0
                        if stay_sec < 300:
                            interval = "0-5分钟"
                        elif stay_sec < 900:
                            interval = "5-15分钟"
                        elif stay_sec < 3600:
                            interval = "15-60分钟"
                        elif stay_sec < 7200:
                            interval = "1-2小时"
                        elif stay_sec < 10800:
                            interval = "2-3小时"
                        else:
                            interval = "3小时以上"
                        key = interval
                    else:
                        key = "未知"
                    stats["基站停留时长分布/条"][key] += 1
                except Exception:
                    pass

    # ==================== 格式化输出 ====================
    def format_with_unit(counter_dict):
        return {k: f"{v}" for k, v in sorted(counter_dict.items())}

    def format_with_tiaoshu(counter_dict):
        return {k: f"{v}" for k, v in sorted(counter_dict.items())}

    def format_with_renming(counter_dict):
        return {k: f"{v}" for k, v in sorted(counter_dict.items())}

    stats["入井时间段分布/人次"] = format_with_unit(stats["入井时间段分布/人次"])
    stats["出井时间段分布/人次"] = format_with_unit(stats["出井时间段分布/人次"])

    def format_nested_set_counter_with_total(nested_set_counter):
        res = {}
        for period, loc_dict in sorted(nested_set_counter.items()):
            res[period] = {}
            total = 0
            for loc, s in sorted(loc_dict.items()):
                n = len(s)
                if n > 0:
                    res[period][loc] = str(n)
                    total += n
            res[period]['总人数'] = str(total)
        return res

    stats["区域分布/条"] = format_nested_set_counter_with_total(stats["区域分布/条"])

    def format_set_counter(set_counter):
        return {k: str(len(v)) for k, v in sorted(set_counter.items())}

    stats["基站分布/条"] = format_set_counter(stats["基站分布/条"])

    stats["基站停留时长分布/条"] = format_with_tiaoshu(stats["基站停留时长分布/条"])
    stats["部门分布/人"] = format_with_renming(stats["部门分布/人"])
    stats["职位分布/人"] = format_with_renming(stats["职位分布/人"])
    stats["工种分布/人"] = format_with_renming(stats["工种分布/人"])

    stats["入井时长分布/人次"] = {
        k: f"{v}" for k, v in sorted(
            stats["入井时长分布/人次"].items(),
            key=lambda x: sort_key(x)
        )
    }

    # === 新增：每小时人数统计格式化 ===
    def format_hourly_person_stats(hourly_set_counter):
        res = {}
        total = 0
        for period_str, name_set in sorted(hourly_set_counter.items()):
            count = len(name_set)
            res[period_str] = {'人员名称':list(name_set),'个数':count}
            total += count
        res["总人次"] = str(total)
        return res

    stats["每小时人数统计/人"] = format_hourly_person_stats(stats["每小时人数统计/人"])

    # keep_keys 过滤
    if keep_keys and "all" not in keep_keys:
        keep_set = set(keep_keys)
        stats = {k: v for k, v in stats.items() if k in keep_set}

    return stats