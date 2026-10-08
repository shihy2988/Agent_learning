#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件名:\tbase_utils.py
作者:\tshihongyu
创建日期:\t2026-09-09
描述:\t淖尔壕煤矿 安全监控(AQJC) 数据处理与工具函数。
      提供时间解析、数值/时间条件判断、模糊匹配、测点数据统计等通用能力,
      适配 MCP 服务安全监控业务功能需求。
      设计参考 HJL_agent/utils/person_utils/base_utils.py。
"""
import traceback
from datetime import datetime
from collections import defaultdict, Counter
from typing import Dict, Any, Optional, Union



# ==================== 时间解析函数 ====================
def parse_time(t_str):
    """
    解析时间字符串, 支持:
    - "%Y-%m-%dT%H:%M:%S" / "%Y-%m-%d %H:%M:%S" (含 .%f)
    - "%Y-%m-%d"
    - "%H:%M:%S"  纯时分秒 -> 返回秒数(int), 支持 HH>23
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

    # 纯 "%H:%M:%S" -> 秒数
    try:
        t = datetime.strptime(t_str, "%H:%M:%S")
        return t.hour * 3600 + t.minute * 60 + t.second
    except ValueError:
        pass

    parts = t_str.split(":")
    if len(parts) == 3:
        try:
            h, m, s = int(parts[0]), int(parts[1]), int(parts[2])
            if 0 <= m < 60 and 0 <= s < 60:
                return h * 3600 + m * 60 + s
        except ValueError:
            pass

    try:
        return datetime.fromisoformat(t_str.replace("Z", "+00:00"))
    except Exception:
        print(t_str, traceback.format_exc())
        return None


def check_numeric_condition(value, condition: dict, field_name: str = "") -> bool:
    """
    支持数值与多种时间格式的过滤。判断条件:
    - ">", ">=" ; "<", "<=" ; "==", "=" ; "!="
    - "between"(闭区间) ; "not_between" ; "in"
    - "after"/"later"/"since" ; "before"/"earlier"/"until"
    同时支持字符串/数值/时间字段的匹配判断。
    时间字段判定: field_name 含 时间/时刻 关键字时走时间逻辑。
    """
    if value is None or not condition:
        return True

    if isinstance(condition, tuple):
        condition = condition[0]

    op = condition.get("op")
    target = condition.get("value")

    # ==================== 判断是否为时间字段 ====================
    time_keywords = ["时间", "时刻"]
    is_time_field = any(kw in field_name for kw in time_keywords)

    if is_time_field:
        try:
            dt_value = parse_time(value)
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

    # ==================== 数值处理 ====================
    try:
        num_value = float(value)
        if op == ">":
            return num_value > float(target)
        elif op == ">=":
            return num_value >= float(target)
        elif op == "<":
            return num_value < float(target)
        elif op == "<=":
            return num_value <= float(target)
        elif op in ["==", "="]:
            return abs(num_value - float(target)) < 1e-6
        elif op == "between":
            if isinstance(target, (list, tuple)) and len(target) == 2:
                return float(target[0]) <= num_value <= float(target[1])
        elif op == "not_between":
            if isinstance(target, (list, tuple)) and len(target) == 2:
                return not (float(target[0]) <= num_value <= float(target[1]))
        elif op == "in":
            if isinstance(target, (list, tuple, set)):
                return num_value in [float(t) for t in target]
            else:
                return abs(num_value - float(target)) < 1e-6
        elif op == "!=":
            return str(value) != str(target)
        return True
    except (ValueError, TypeError):
        # 字符串兜底
        if op == "in":
            if isinstance(target, (list, tuple, set)):
                return str(value) in [str(t) for t in target]
            else:
                return str(value) == str(target)
        elif op == "!=":
            return str(value) != str(target)
        return str(value) == str(target)


# ==================== 模糊匹配 ====================
def normalize(v):
    """None->None, str->[str], 其余原样返回(便于统一按列表处理)"""
    if v is None:
        return None
    if isinstance(v, str):
        return [v]
    return v


def fuzzy_match(value, filters, threshold=50):
    """
    模糊匹配: filters 中任意元素与 value 相似度超过 threshold, 或 value 包含任一 filter, 即视为匹配。
    无 fuzzywuzzy 依赖时退化为子串匹配。
    """
    if not filters:
        return False
    value = str(value) if value is not None else ""
    if filters is not None and not isinstance(filters, (list, tuple)):
        filters = [filters]

    # 完全包含直接 True
    for f in filters:
        if str(f) in value:
            return True

    try:
        from fuzzywuzzy import process
        all_matches = process.extract(value, [str(f) for f in filters])
        if all_matches:
            _, max_score = max(all_matches, key=lambda x: x[1])
            return max_score >= threshold
    except Exception:
        # 无 fuzzywuzzy, 退化: 任一边是另一边的子串
        for f in filters:
            fs = str(f)
            if fs and (fs in value or value in fs):
                return True
    return False


def sort_key(x):
    # x like "0-1小时"
    try:
        return int(str(x).split('-')[0])
    except Exception:
        return 0


# ==================== 测点数据统计 ====================
from collections import Counter
from typing import Dict

def generate_statistics(points: Dict[str, Dict], keep_keys=None) -> Dict:
    stats = {
        "总测点数": len(points),
        "测点列表": [],
        "传感器类型分布/个": Counter(),
        "测点状态分布/条": Counter(),
        
    }

    for code, point in points.items():
        data = point.get("稳定阶段", [])
        if not data:
            continue

        sensor_name = point.get("传感器类型名称", "")
        location = point.get("测点安装位置", "")
        unit = point.get("数值单位", "")
        stats["测点列表"].append([code, sensor_name, location, len(data)])
        if sensor_name:
            stats["传感器类型分布/个"][sensor_name] += 1

        statuses = point.get("测点状态")
        if statuses is not None:
            for status in statuses:
                st = status['状态']
                stats["测点状态分布/条"][str(st)] += 1

    stats["传感器类型分布/个"] = {k: str(v) for k, v in sorted(stats["传感器类型分布/个"].items())}
    stats["测点状态分布/条"] = {k: str(v) for k, v in sorted(stats["测点状态分布/条"].items())}

    if keep_keys and "all" not in keep_keys:
        keep_set = set(keep_keys)
        stats = {k: v for k, v in stats.items() if k in keep_set}

    return stats
