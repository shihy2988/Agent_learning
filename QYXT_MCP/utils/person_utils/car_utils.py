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
from fuzzywuzzy import fuzz, process
from datetime import datetime
from collections import defaultdict
from collections import defaultdict, Counter
import requests
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
    if not t_str:
        return None
    t_str = str(t_str).strip()

    # 支持多种格式
    formats = [
        "%Y-%m-%dT%H:%M:%S",  # 2026-07-01T23:52:20
        "%Y-%m-%d %H:%M:%S",  # 2026-07-02 00:04:05  ← 新增
        "%Y-%m-%d %H:%M:%S.%f",  # 带毫秒
        "%Y-%m-%dT%H:%M:%S.%f",
    ]

    for fmt in formats:
        try:
            return datetime.strptime(t_str, fmt)
        except ValueError:
            continue
    # 尝试ISO格式（带时区等）
    try:
        return datetime.fromisoformat(t_str.replace("Z", "+00:00"))
    except:
        return None

def check_numeric_condition(value, condition: dict, field_name: str = "") -> bool:
    """增强版：支持数值 + 多种时间格式"""
    if value is None or not condition:
        return True

    op = condition.get("op")
    target = condition.get("value")

    # ==================== 判断是否为时间字段 ====================
    time_keywords = ["时间", "入井", "出井", "轨迹开始", "轨迹结束"]
    is_time_field = any(kw in field_name for kw in time_keywords)

    parsed_value = parse_time(value)

    if is_time_field or parsed_value is not None:
        try:
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

            return True
        except Exception:
            pass  # 解析失败则继续走数值/字符串逻辑

    # ==================== 数值处理（保持不变） ====================
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
        return True
    except (ValueError, TypeError):
        # 字符串兜底
        return str(value) == str(target)


def parse_time(t_str):
    if not t_str: return None
    t_str = str(t_str).strip()
    formats = ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M:%S.%f"]
    for fmt in formats:
        try:
            return datetime.strptime(t_str, fmt)
        except:
            continue
    try:
        return datetime.fromisoformat(t_str.replace("Z", "+00:00"))
    except:
        return None

def sort_key(x):
    # x like "0-1小时"
    try:
        return int(x.split('-')[0])
    except:
        return 0


from datetime import timezone

UTC = timezone.utc


def ensure_utc(dt):
    if dt is None:
        return None

    # 没有时区，认为是 UTC
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)

    # 有时区，转 UTC
    return dt.replace(tzinfo=UTC)



def generate_car_statistics(filtered_cars, keep_keys=["all"]):
    """
    统计指定车辆数据，增加keep_keys功能：只保留keep_keys中出现的key，其他不返回。
    并增加按照 部门、车辆类型、主站、分站、区域等分布统计。
    新增：每小时车辆数统计，轨迹与某小时有交集即计入，同一车同小时只计一次。
    :param filtered_cars: 车辆dict
    :param keep_keys: 仅保留的key集合或list，若为None则全部保留
    :return: stats统计字典
    """
    from collections import defaultdict, Counter
    from datetime import timedelta

    stats = {
        "总车辆数": len(filtered_cars),
        "车辆总览": [],
        "车辆列表_名称_编号_出入井次数": [],
        "出入井时长分布/辆次": defaultdict(int),
        "入井时间段分布/辆次": defaultdict(int),
        "出井时间段分布/辆次": defaultdict(int),
        "入井地点分布/辆次": defaultdict(int),
        "出井地点分布/辆次": defaultdict(int),
        "区域分布/条": defaultdict(lambda: defaultdict(set)),
        "主站分布/条": defaultdict(set),
        "分站分布/条": defaultdict(set),
        "站点停留时长分布/条": defaultdict(int),
        "所属部门分布/辆": Counter(),
        "车辆类型分布/辆": Counter(),
        # === 新增：每小时车辆数统计 ===
        "每小时车辆数统计/辆": defaultdict(set),  # {period_str: set(车辆名称)}
    }


    car_set = set()
    current_underground_count = 0
    for car_name, carinfo in filtered_cars.items():
        car_id = carinfo.get("车辆编号", "")
        car_name_val = carinfo.get("车辆名称", "")
       
            
        if car_id in car_set:
            continue
        car_set.add(car_id)

        department = carinfo.get("所属部门")
        classtype = carinfo.get("车辆类型")

        if department:
            stats["所属部门分布/辆"][department] += 1
        if classtype:
            stats["车辆类型分布/辆"][classtype] += 1

        stats["车辆总览"].append([
            car_name_val,
            car_id, department, classtype
        ])
        stats["车辆列表_名称_编号_出入井次数"].append([
            car_name_val,
            car_id,
            len(carinfo.get("出入井记录", []))
        ])

      
        for segment in carinfo.get("出入井记录", []):
            # 出入井时长分布
            try:
                ts = segment.get("入井时长(秒)")
                if ts is not None:
                    hours = max(0, int(float(ts)) // 3600)
                    key = f"{hours}-{hours + 1}小时"
                    stats["出入井时长分布/辆次"][key] += 1
            except Exception:
                pass

            in_time = parse_time(segment.get("入井时间"))
            out_time = parse_time(segment.get("出井时间"))

            if in_time:
                hour = in_time.hour
                next_hour = (hour + 1) % 24
                period = f"{in_time.strftime('%Y-%m-%d')} {hour:02d}-{next_hour:02d}点"
                stats["入井时间段分布/辆次"][period] += 1

            if out_time:
                hour = out_time.hour
                next_hour = (hour + 1) % 24
                period = f"{out_time.strftime('%Y-%m-%d')} {hour:02d}-{next_hour:02d}点"
                stats["出井时间段分布/辆次"][period] += 1

            stats["入井地点分布/辆次"][segment.get("入井地点") if segment.get("入井地点") is not None else "未知"] += 1
            stats["出井地点分布/辆次"][segment.get("出井地点") if segment.get("出井地点") is not None else "未出井"] += 1

            area_seen = set()
            main_seen = set()
            sub_seen = set()
            for record in segment.get("具体轨迹变化", []):
                area = record.get("区域名称")
                main = record.get("主站名称")
                sub = record.get("分站名称")

                # ========== 新增：每小时车辆数统计核心逻辑 ==========
                track_start = parse_time(record.get("轨迹开始时间"))
                time_end = record.get("轨迹结束时间")
                if not time_end:
                    track_end = datetime.now()
                else:
                    track_end = parse_time(time_end)
                
                track_start = ensure_utc(track_start)
                track_end = ensure_utc(track_end)

                if track_start and track_end and track_start <= track_end:
                    start_hour_dt = track_start.replace(minute=0, second=0, microsecond=0)
                    end_hour_dt = track_end.replace(minute=0, second=0, microsecond=0)
                    # 结束时间恰好为整点时，不回退（车辆轨迹整点结束仍视为在该时段内活动过）
                    # 如需与人员统计保持一致（整点结束不计入该小时），取消下面注释即可：
                    # if track_end == end_hour_dt and track_end > track_start:
                    #     effective_end_hour_dt = end_hour_dt - timedelta(hours=1)
                    # else:
                    #     effective_end_hour_dt = end_hour_dt
                    effective_end_hour_dt = end_hour_dt

                    current = start_hour_dt
                    while current <= effective_end_hour_dt:
                        h = current.hour
                        next_h = (h + 1) % 24
                        period_str = f"{current.strftime('%Y-%m-%d')} {h:02d}-{next_h:02d}点"
                        stats["每小时车辆数统计/辆"][period_str].add(car_name_val)
                        current += timedelta(hours=1)

                elif track_start:
                    h = track_start.hour
                    next_h = (h + 1) % 24
                    period_str = f"{track_start.strftime('%Y-%m-%d')} {h:02d}-{next_h:02d}点"
                    stats["每小时车辆数统计/辆"][period_str].add(car_name_val)

                elif track_end:
                    h = track_end.hour
                    next_h = (h + 1) % 24
                    period_str = f"{track_end.strftime('%Y-%m-%d')} {h:02d}-{next_h:02d}点"
                    stats["每小时车辆数统计/辆"][period_str].add(car_name_val)
                # ===================================================

                # 区域分布/条（保持原有逻辑）
                period = None
                time_str = record.get("时间") or record.get("起始时间")
                dt = parse_time(time_str)
                if dt:
                    period = f"{dt.hour:02d}-{dt.hour + 1:02d}点"
                else:
                    period = "未知"

                if area:
                    if (area, period) not in area_seen:
                        stats["区域分布/条"][period][area].add(car_name_val)
                        area_seen.add((area, period))

                if main and main not in main_seen:
                    stats["主站分布/条"][main].add(car_name_val)
                    main_seen.add(main)

                if sub and sub not in sub_seen:
                    stats["分站分布/条"][sub].add(car_name_val)
                    sub_seen.add(sub)

                # 站点停留时长分布
                stay_time = record.get("停留时长/s")
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
                    stats["站点停留时长分布/条"][key] += 1
                except Exception:
                    pass
    
    # ==================== 格式化输出 ====================
    def format_with_unit(counter_dict):
        return {k: f"{v}" for k, v in sorted(counter_dict.items())}

    def format_with_tiaoshu(counter_dict):
        return {k: f"{v}" for k, v in sorted(counter_dict.items())}

    def format_with_renming(counter_dict):
        return {k: f"{v}" for k, v in sorted(counter_dict.items())}

    stats["入井时间段分布/辆次"] = format_with_unit(stats["入井时间段分布/辆次"])
    stats["出井时间段分布/辆次"] = format_with_unit(stats["出井时间段分布/辆次"])
    stats["入井地点分布/辆次"] = format_with_unit(stats["入井地点分布/辆次"])
    stats["出井地点分布/辆次"] = format_with_unit(stats["出井地点分布/辆次"])

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
            res[period]['总车辆数'] = str(total)
        return res

    def format_set_counter(set_counter):
        return {k: str(len(v)) for k, v in sorted(set_counter.items())}

    stats["区域分布/条"] = format_nested_set_counter_with_total(stats["区域分布/条"])
    stats["主站分布/条"] = format_set_counter(stats["主站分布/条"])
    stats["分站分布/条"] = format_set_counter(stats["分站分布/条"])

    stats["站点停留时长分布/条"] = format_with_tiaoshu(stats["站点停留时长分布/条"])
    stats["所属部门分布/辆"] = format_with_renming(stats["所属部门分布/辆"])
    stats["车辆类型分布/辆"] = format_with_renming(stats["车辆类型分布/辆"])

    stats["出入井时长分布/辆次"] = {
        k: f"{v}" for k, v in sorted(
            stats["出入井时长分布/辆次"].items(),
            key=lambda x: sort_key(x)
        )
    }

    # === 新增：每小时车辆数统计格式化 ===
    def format_hourly_car_stats(hourly_set_counter):
        res = {}
        total = 0
        for period_str, name_set in sorted(hourly_set_counter.items()):
            count = len(name_set)
            res[period_str] = {'车辆名称':list(name_set),'个数':count}
            total += count
        res["总辆次"] = str(total)
        return res

    stats["每小时车辆数统计/辆"] = format_hourly_car_stats(stats["每小时车辆数统计/辆"])

    # keep_keys 过滤
    if keep_keys and "all" not in keep_keys:
        keep_set = set(keep_keys)
        stats = {k: v for k, v in stats.items() if k in keep_set}

    return stats