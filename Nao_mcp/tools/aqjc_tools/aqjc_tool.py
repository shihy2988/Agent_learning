#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件名:    aqjc_tools.py
作者:      shihy（改造自 person_tools.py）
创建日期:  2026-09-10
描述:      矿井安全监控(AQJC)测点相关 MCP 工具服务。
           提供今日/当前测点数据查询、多测点综合查询与统计聚合、测点异常预警、
           分站运行/供电状态排查、基础档案(测点/分站)查询等能力。
           数据源: ClickHouse, database = PS_NAO
           底层依赖: qyjc_bases.QyjcBase
"""

import os
import sys
import json
import asyncio
import logging
import traceback
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from logging.handlers import RotatingFileHandler
from typing import Optional, List, Dict, Any, Union

import urllib3
import clickhouse_connect
from fastmcp import FastMCP
from fuzzywuzzy import fuzz
import re
import copy

# ==================== 依赖路径 ====================
_THIS_FILE = os.path.abspath(__file__)
sys.path.append(os.path.dirname(os.path.dirname(_THIS_FILE)))
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(_THIS_FILE))) + '/utils/aqjc_utils'
)

from aqjc_bases import AQjcBase
from base_utils import (
    generate_statistics,
    normalize,
    fuzzy_match,
    check_numeric_condition,
)

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


# ==================== 日志 ====================
LOG_DIR = 'logs'
LOG_FILE = os.path.join(LOG_DIR, 'AQJC_service.log')

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=5 * 1024 * 1024,   # 50 MB
    backupCount=5,
    encoding='utf-8',
)
_handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)

_root_logger = logging.getLogger()
_root_logger.setLevel(logging.INFO)

# 防止日志重复记录：移除已有 StreamHandler
for h in list(_root_logger.handlers):
    if isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler):
        _root_logger.removeHandler(h)

_root_logger.addHandler(_handler)
logger = logging.getLogger("MineAQJCService")


# ==================== 常量 ====================
ABNORMAL_TYPE_MAP = {
    "001": "超限报警", "002": "断电报警", "003": "馈电异常",
    "004": "传感器断线", "005": "基站断电", "006": "基站不通",
    "007": "标校",     "008": "超量程",   "009": "超上限预警",
    "010": "超下限预警",
}

# 可用于 numeric_filters 的字段（供大响应压缩时提示用户）
FILTERABLE_FIELDS = [
    "高量程", "低量程", "上限报警门限", "上限解报门限",
    "下限报警门限", "下限解报门限", "上限断电门限", "上限复电门限",
    "下限断电门限", "下限复电门限",
    "平均值", "中位数", "标准差", "最小值", "最大值", "趋势",
    "持续秒数", "持续小时", "起始时间", "结束时间", "状态",
]

# 大响应压缩时的默认统计项
DEFAULT_STATS_TODAY = ["总测点数", "测点列表", "传感器类型分布/个", "测点状态分布/条"]
DEFAULT_STATS_MULTI = ["总测点数", "传感器类型分布/个", "测点状态分布/条"]

# 单次返回 JSON 的软上限（超出触发压缩）
MAX_JSON_SIZE = 30000

# query_station_point_tree 允许的字段
ALLOWED_POINT_FIELDS = {"传感器类型列表", "测点安装位置列表", "测点编码列表", "测点位置——编码对应关系"}
ALLOWED_FZ_FIELDS = {"分站安装位置列表", "分站编码列表","分站编码-安装位置对应关系","分站数量","分站安装位置数量","传感器类型数量","测点安装位置数量","测点数量"}
                


# ==================== 通用工具函数 ====================

def _extract_number(v):
    """从值中提取数字，失败返回 None。"""
    if isinstance(v, bool):
        return None
    if isinstance(v, (int, float)):
        return float(v)
    if isinstance(v, str):
        m = re.search(r'-?\d+\.?\d*', v)
        if m:
            try:
                return float(m.group())
            except ValueError:
                return None
    return None


def _merge_values(a, b):
    """合并两个统计值：字典递归合并，数值字符串相加，其余保留后者。"""
    if isinstance(a, dict) and isinstance(b, dict):
        result = {}
        keys = list(a.keys()) + [k for k in b.keys() if k not in a]
        for k in keys:
            if k in a and k in b:
                result[k] = _merge_values(a[k], b[k])
            elif k in a:
                result[k] = a[k]
            else:
                result[k] = b[k]
        return result
    na, nb = _extract_number(a), _extract_number(b)
    if na is not None and nb is not None:
        total = na + nb
        if isinstance(a, str) and a.rstrip().endswith("个"):
            return f"{int(total)}个"
        if isinstance(a, str) and a.rstrip().endswith("条"):
            return f"{int(total)}条"
        if isinstance(a, str) and a.rstrip().endswith("人"):
            return f"{int(total)}人"
        if isinstance(a, str) and a.rstrip().endswith("次"):
            return f"{int(total)}次"
        if isinstance(a, str):
            return str(int(total)) if total == int(total) else str(total)
        return total
    return b


def _merge_dicts(values):
    """按顺序合并一组字典/数值。"""
    result = None
    for v in values:
        if v is None:
            continue
        if result is None:
            result = copy.deepcopy(v)
        else:
            result = _merge_values(result, v)
    return result


def _truncate_ranking(ranking_data, top_n: int = 5):
    """
    通用排名截断：
      支持 [[名称,类型,位置,采样条数], ...] / [{"采样条数":n},...] / {key: count}
      排序键优先识别 采样条数/条数/count/次数/数量。
    """
    if not ranking_data:
        return ranking_data
    try:
        def _extract_count(value):
            if isinstance(value, dict):
                for kk in ("采样条数", "条数", "count", "次数", "数量", "测点数"):
                    if kk in value:
                        try:
                            return float(value[kk])
                        except (ValueError, TypeError):
                            return 0.0
                return 0.0
            if isinstance(value, (int, float)):
                return float(value)
            if isinstance(value, str):
                try:
                    return float(value)
                except ValueError:
                    return 0.0
            if isinstance(value, (list, tuple)):
                for v in reversed(value):
                    try:
                        return float(v)
                    except (ValueError, TypeError):
                        continue
            return 0.0

        if isinstance(ranking_data, dict):
            sorted_items = sorted(
                ranking_data.items(),
                key=lambda kv: _extract_count(kv[1]),
                reverse=True,
            )
            return dict(sorted_items[:top_n])
        elif isinstance(ranking_data, list):
            sorted_items = sorted(
                ranking_data,
                key=lambda item: _extract_count(item),
                reverse=True,
            )
            return sorted_items[:top_n]
    except Exception as e:
        logger.warning(f"_truncate_ranking 处理失败: {e}")
    return ranking_data


def _aggregate_multi_day_stats(per_day_stats, days, top_list_key=None, top_n=5, total_key="总测点数"):
    """
    多日统计聚合：
      - total_key：给每日明细 + 合计
      - top_list_key：按 测点编码 汇总采样条数，取 TOP N
      - 其他分布：跨天递归累加合并
    """
    if not per_day_stats:
        return {}
    all_keys = set()
    for day in days:
        all_keys.update(per_day_stats.get(day, {}).keys())

    result = {}
    for key in all_keys:
        if key == total_key:
            daily = {}
            total = 0
            for day in days:
                v = per_day_stats.get(day, {}).get(key)
                try:
                    n = int(v)
                    daily[day] = n
                    total += n
                except (TypeError, ValueError):
                    continue
            result[total_key] = {"每日": daily, "合计": total}

        elif top_list_key and key == top_list_key:
            merged = {}
            for day in days:
                v = per_day_stats.get(day, {}).get(key)
                items = []
                if isinstance(v, list):
                    items = v
                elif isinstance(v, dict):
                    items = list(v.items())
                for item in items:
                    if isinstance(item, (list, tuple)) and len(item) >= 4:
                        code, sensor, loc, cnt = item[0], item[1], item[2], item[3]
                        kk = (code, sensor, loc)
                        try:
                            c = int(cnt)
                        except (TypeError, ValueError):
                            c = 0
                        merged[kk] = merged.get(kk, 0) + c
                    elif isinstance(item, (list, tuple)) and len(item) == 2:
                        kk, val = item
                        try:
                            c = int(val)
                        except (TypeError, ValueError):
                            c = 0
                        merged[kk] = merged.get(kk, 0) + c
            sorted_items = sorted(merged.items(), key=lambda kv: kv[1], reverse=True)[:top_n]
            result[key] = [[k[0], k[1], k[2], v] for k, v in sorted_items]

        else:
            values = [per_day_stats.get(day, {}).get(key) for day in days]
            values = [v for v in values if v is not None]
            if values:
                result[key] = _merge_dicts(values)
    return result


def _summarize_yb_info(yb_result):
    """
    汇总测点异常：
      - 异常总数
      - 异常类型分布（按类型描述）
      - 测点异常列表（按异常条数降序）
    """
    if not yb_result:
        return "无异常信息"

    point_summary = {}
    type_summary = {}
    total = 0

    for point in yb_result:
        if not isinstance(point, dict):
            continue
        point_code = point.get("测点编码", "")
        sensor_type = point.get("传感器类型名称", "")
        location = point.get("测点安装位置", "")
        events = point.get("异常事件列表", [])

        if not isinstance(events, list):
            continue

        for evt in events:
            if not isinstance(evt, dict):
                continue
            total += 1

            type_code = str(evt.get("异常类型编码", "")).zfill(3)
            type_desc = evt.get("异常类型描述") or ABNORMAL_TYPE_MAP.get(type_code, type_code)

            kp = (point_code, sensor_type, location)
            if kp not in point_summary:
                point_summary[kp] = {
                    "测点编码": point_code,
                    "传感器类型名称": sensor_type,
                    "测点安装位置": location,
                    "异常条数": 0,
                    "异常类型": {},
                }
            point_summary[kp]["异常条数"] += 1
            point_summary[kp]["异常类型"][type_desc] = (
                point_summary[kp]["异常类型"].get(type_desc, 0) + 1
            )

            type_summary[type_desc] = type_summary.get(type_desc, 0) + 1

    point_list = list(point_summary.values())
    point_list.sort(key=lambda x: x["异常条数"], reverse=True)

    return {
        "异常总数": total,
        "异常类型分布": type_summary,
        "测点异常列表": point_list,
    }


def _summarize_station_status(station_result):
    """
    汇总分站状态：
      - 运行状态统计：按最终运行状态计数
      - 供电状态统计：按最终供电状态计数
      - 状态变化分站：仅收录运行/供电状态发生变化的分站，给出其变化过程（无时间）
    """
    if not station_result:
        return "无分站状态记录"

    run_state_summary = {}
    power_state_summary = {}
    changed_stations = {}

    for fz_code, fz_info in station_result.items():
        if not isinstance(fz_info, dict):
            continue
        periods = fz_info.get("时段", []) or []
        if not periods:
            continue

        try:
            periods_sorted = sorted(periods, key=lambda p: p.get("起始时间", "") if isinstance(p, dict) else "")
        except Exception:
            periods_sorted = periods

        changes = []
        prev_key = None
        for p in periods_sorted:
            if not isinstance(p, dict):
                continue
            run = p.get("分站运行状态", "")
            power = p.get("分站供电状态", "")
            cur_key = (run, power)
            if cur_key != prev_key:
                changes.append({"运行状态": run, "供电状态": power})
                prev_key = cur_key

        if changes:
            final = changes[-1]
            if final.get("运行状态"):
                rs = final["运行状态"]
                run_state_summary[rs] = run_state_summary.get(rs, 0) + 1
            if final.get("供电状态"):
                ps = final["供电状态"]
                power_state_summary[ps] = power_state_summary.get(ps, 0) + 1

        if len(changes) > 1:
            changed_stations[fz_code] = f"{len(changes)}次"

    return {
        "运行状态统计": run_state_summary,
        "供电状态统计": power_state_summary,
        "状态变化分站": changed_stations if changed_stations else "无状态变化分站",
    }
    
def convert_sets_to_lists(obj):
    """递归地将对象中的 set 转换为 list。"""
    if isinstance(obj, set):
        return list(obj)
    if isinstance(obj, dict):
        return {k: convert_sets_to_lists(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [convert_sets_to_lists(item) for item in obj]
    return obj


def _normalize_filters(x):
    """把 str / list / None 归一化为 list[str] / None。"""
    if x is None:
        return None
    if isinstance(x, str):
        s = x.strip()
        return [s] if s else None
    if isinstance(x, list):
        out = [str(i).strip() for i in x if isinstance(i, (str, int)) and str(i).strip()]
        return out or None
    return None


# ==================== MCP 服务 ====================
class AQJCMcpService:

    def __init__(
            self,
            mcp: FastMCP,
            host: str,
            port: int,
            user: str,
            password: str,
            database: str,
    ):
        self.mcp = mcp
        self.db_config = {
            "host": host,
            "port": port,
            "username": user,
            "password": password,
            "database": database,
            "secure": False,
            "verify": False,
            "connect_timeout": 10,
            'autogenerate_session_id': False,
        }

        try:
            self.client = clickhouse_connect.get_client(**self.db_config)
            logger.info("Successfully connected to ClickHouse.")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

        # 底层基类
        self.qyjc_base = AQjcBase(self.client, logger)

        self._register_resources()
        self._register_prompts()
        self._register_tools()

    # ==================== 1. Resource: 数据字典 ====================
    def _register_resources(self):
        @self.mcp.resource("docs://aqjc/data-dictionary")
        def get_data_dictionary() -> str:
            """获取安全监控(AQJC)测点系统的数据字典、字段说明及查询指南。"""
            return """
                # 矿井安全监控(AQJC)系统数据字典与查询指南

                ## 1. 测点元信息字段（可在 numeric_filters 中筛选）
                | 字段名 | 类型 | 说明 | 示例 |
                | :--- | :--- | :--- | :--- |
                | **高量程** | Float | 传感器高量程 | 100 |
                | **低量程** | Float | 传感器低量程 | 0 |
                | **上限报警门限** | Float | 上限报警门限 | 8.0 |
                | **上限解报门限** | Float | 上限解报门限 | 7.5 |
                | **下限报警门限** | Float | 下限报警门限 | 0.5 |
                | **下限解报门限** | Float | 下限解报门限 | 0.8 |
                | **上限断电门限** | Float | 上限断电门限 | — |
                | **上限复电门限** | Float | 上限复电门限 | — |
                | **下限断电门限** | Float | 下限断电门限 | — |
                | **下限复电门限** | Float | 下限复电门限 | — |
                | **平均值** | Float | 当天测点均值 | 0.0303 |
                | **中位数** | Float | 当天测点中位数 | 0.0 |
                | **标准差** | Float | 当天测点标准差 | 0.1714 |
                | **最小值** | Float | 当天最小值 | 0 |
                | **最大值** | Float | 当天最大值 | 1 |
                | **起始时间** | DateTime | 稳定阶段/状态记录起始时间 | "2026-09-09 22:10:40" |
                | **结束时间** | DateTime | 稳定阶段/状态记录结束时间 | "2026-09-10 01:21:59" |
                | **持续秒数** | Int | 持续秒数 | 3600 |
                | **持续小时** | Float | 持续小时 | 1.0 |
                | **状态** | String | 测点状态（正常/标校/故障/断线等） | "标校" |

                ## 2. 支持的操作符 (op)
                - `>`, `>=`: 大于 / 大于等于
                - `<`, `<=`: 小于 / 小于等于
                - `==`, `=`: 等于
                - `!=`: 不等于
                - `between`: 闭区间，value 为 [start, end]
                - `not_between`: 不在区间内
                - `in`: 包含于列表
                - `after` / `since`: 等同 `>=`
                - `before` / `until`: 等同 `<=`

                ## 3. 统计聚合项 (statistics_filters)
                通过 `statistics_filters` 指定需要返回的统计维度。可选：
                1. **总测点数**: 符合条件的测点总数
                2. **测点列表**: [测点编码, 传感器类型名称, 安装位置, 采样条数]
                3. **传感器类型分布/个**: 按传感器类型统计的测点数量
                4. **测点状态分布/条**: 按状态统计的条数

                ## 4. 基础档案类型 (get_infos)
                - **cddy**: 测点档案（测点编码、类型、安装位置、量程、门限等）
                - **fzdy**: 分站档案（分站编码、名称、位置、状态等）
                """

    # ==================== 2. Prompt: 全局行为准则 ====================
        # ==================== 2. Prompt: 全局行为准则 ====================
    def _register_prompts(self):
        @self.mcp.prompt()
        def analysis_guide() -> str:
            """获取矿井安全监控(AQJC)数据分析的专业操作指南。"""
            return """
            你是一名资深的【矿井安全监控数据分析专家】。你的核心任务是根据用户的自然语言描述，精准拆解需求，组合调用系统提供的工具，返回结构化、可执行的分析结果。

            ### 🔄 核心工作流 (Workflow)
            1. **时间对齐**：用户提及相对时间（"今天"、"最近2小时"、"本周"），**必须首先**调用 `get_system_time` 获取服务器当前时间，再推算准确的时间范围（格式: YYYY-MM-DD HH:MM:SS）。
            2. **意图路由**：根据用户需求，从下方【工具路由矩阵】选择最匹配的工具。
            3. **参数构建**：优先使用结构化过滤条件（如 `numeric_filters` / `statistics_filters`），避免获取全量数据后再做 Python 过滤。
            4. **结果解释**：对返回的 JSON 做专业解读。若数据量过大被压缩，主动建议用户增加筛选条件。

            ---

            ### 🛠️ 工具路由矩阵

            #### 1. `get_system_time`（时间基准）
            - **何时使用**：任何涉及时间范围的查询前。
            - **注意**：无参数。

            #### 2. `query_todayornow_points`（今日/当前测点数据）
            - **何时使用**：查询今日或当前时刻的测点数据、实时状态。
            - **关键参数技巧**：
              - **today_or_now**: True 返回今日(00:00 - now)的数据；False 返回近10分钟数据（即"当前"）。
              - **模糊匹配**：`point_code_filters`, `sensor_type_filters`, `location_filters`, `substation_code_filters` 支持字符串或列表。
              - **高级数值/时间过滤（numeric_filters）**：如"上限报警门限 < 5"、"状态 == 标校"。
              - **统计聚合（statistics_filters）**：当用户询问"分布"、"汇总"、"多少"时，传入 Key 列表以避免返回海量明细。

            #### 3. `query_points_list`（综合查询）
            - **何时使用**：查询任意时间段的测点数据、多维度统计、复杂条件筛选。
            - **关键参数**：`start_date` / `end_date` 指定时间窗口（未指定时默认当天）。

            #### 4. `query_point_yb_info`（测点异常信息）
            - **何时使用**：用户询问"某测点异常"、"预警"、"标校记录"、"故障"。
            - **关键参数**：`point_code` 指定测点；`start_time` / `end_time` 时间窗口；`real_status=True` 时聚焦近12小时。

            #### 5. `query_station_status`（分站状态排查）
            - **何时使用**：用户询问"分站是否在线"、"通讯中断"、"供电故障"。
            - **关键参数**：`substation_code` 支持模糊匹配；`real_status=True` 时聚焦近12小时。

            #### 6. `query_station_point_tree`（分站 ↔ 测点关联树）
            - **何时使用**：用户需要"列出所有测点"、"查找某测点"、"列出所有分站"。
            - **关键参数**：`fields` 必须使用精确全称（如 `测点编码列表`、`传感器类型列表`），否则静默返回空。

            #### 7. `generate_aqjc_report`（日报 / 多日报告）
            - **何时使用**：用户明确说"出一份报告"、"生成日报"、"汇总汇报"、"周报"、"近N天情况报告"等。
            - **入参**：只需 `start_date` 和 `end_date`（"YYYY-MM-DD HH:MM:SS" 或 "YYYY-MM-DD"）。
              - 同一天 → **单日报告**；跨天 → **多日报告**（多日已做跨天聚合，不按天罗列）。
            - **固定输出三块**（一次调用完整返回，无需再拼装）：
              1. **统计内容**：总测点数 / 测点列表(采样条数 TOP5) / 传感器类型分布 / 测点状态分布
              2. **异常信息**：异常总数 / 异常类型分布 / 测点异常列表
              3. **分站状态**：运行状态统计 / 供电状态统计 / 状态变化分站（仅列有变化的分站及其变化过程，不含时间）
            - **与其它工具的区别**：
              - 需要**报告结构**（统计+异常+分站三块）→ 用本工具；
              - 只需要**单项明细/单维度统计** → 用 `query_todayornow_points` / `query_points_list`；
              - 只要**异常明细** → 用 `query_point_yb_info`；
              - 只要**分站状态** → 用 `query_station_status`。
            - **不要用**"`query_todayornow_points` + `query_point_yb_info` + `query_station_status`"手工拼装日报——三块内容已内聚到本工具，一次调用即可。

            ---

            ### 📋 报告场景决策

            当用户提出以下表达时，**优先路由到 `generate_aqjc_report`**：

            | 用户表达 | 路由 |
            | :--- | :--- |
            | "出一份今天的报告" | `generate_aqjc_report`（start_date=end_date=今天） |
            | "汇总今天测点情况" | `generate_aqjc_report` |
            | "生成近3天/本周/本月报告" | `generate_aqjc_report`（跨天→多日报告） |
            | "有没有报警/异常汇总" | 只关心异常 → `query_point_yb_info`；若还要统计和分站 → `generate_aqjc_report` |
            | "分站状态怎么样" | 只关心分站 → `query_station_status`；若还要统计和异常 → `generate_aqjc_report` |
            | "某测点当前值" | `query_todayornow_points`（point_code_filters=...） |
            | "有哪些测点/分站" | `query_station_point_tree`（fields 单字段投影） |

            ---

            ### ⚠️ 异常处理与兜底
            1. **无数据**：明确告知"指定条件下未找到相关记录"，并建议缩小/扩大范围。
            2. **参数错误**：检查时间格式 `YYYY-MM-DD HH:MM:SS` 或 op 合法性。
            3. **数据截断**：若响应包含 message 提示，向用户解释原因并引导使用 `statistics_filters` 下钻。
            4. **实时数据延迟**：本系统实时数据存在较大延迟（实测可达数小时），回答"今日/当前值"时**主动说明数据截止时刻**，避免用户误以为是实时。
            5. **报告日期判定**：`generate_aqjc_report` 的单日/多日由内部数据覆盖天数自动决定。若用户口述"今天"但时间窗跨天，将输出多日报告。

            请保持回答专业、客观、条理清晰。优先输出核心结论，再附带详细数据支撑。
            """

    # ==================== 3. 压缩辅助函数 ====================
    @staticmethod
    def _compact_station_status(res_new: Dict, max_size: int = MAX_JSON_SIZE) -> str:
        """
        对 query_station_status 的结果做两阶段压缩：
          1) 摘要结构：按分站聚合 时段数 / 运行状态分布 / 供电状态分布 / 总持续小时 / 最新时段
          2) 极简结构：仅编码 / 位置 / 时段数 / 总持续小时
        返回 JSON 字符串。
        """

        def build_summary(data: Dict) -> Dict:
            out = {}
            for fz_code, info in data.items():
                periods = info.get("时段", []) if isinstance(info, dict) else []
                run_counter = Counter()
                power_counter = Counter()
                total_hours = 0.0
                last_period = None
                last_end = None

                for p in periods:
                    if not isinstance(p, dict):
                        continue
                    run_counter[p.get("分站运行状态", "未知")] += 1
                    power_counter[p.get("分站供电状态", "未知")] += 1
                    try:
                        total_hours += float(p.get("持续小时", 0) or 0)
                    except (TypeError, ValueError):
                        pass

                    end_t = str(p.get("结束时间", "") or "")
                    if end_t and (last_end is None or end_t > last_end):
                        last_end = end_t
                        last_period = p

                out[fz_code] = {
                    "分站编码": info.get("分站编码", fz_code) if isinstance(info, dict) else fz_code,
                    "分站安装位置": info.get("分站安装位置", "") if isinstance(info, dict) else "",
                    "时段数": len(periods),
                    "总持续小时": round(total_hours, 2),
                    "运行状态分布": dict(run_counter),
                    "供电状态分布": dict(power_counter),
                    "最新时段": last_period,
                }
            return out

        def build_minimal(summary: Dict) -> Dict:
            return {
                k: {
                    "分站编码": v.get("分站编码", k),
                    "分站安装位置": v.get("分站安装位置", ""),
                    "时段数": v.get("时段数", 0),
                    "总持续小时": v.get("总持续小时", 0),
                }
                for k, v in summary.items()
            }

        # 阶段 1：摘要
        summary = build_summary(res_new)
        summary["message"] = (
            "由于数据体量过大，已压缩为分站级状态摘要（含状态分布与总时长）。"
            "如需时段明细，请指定 substation_code / 时间窗 / run_state / power_state 进一步缩小范围。"
        )
        json_summary = json.dumps(
            convert_sets_to_lists(summary),
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )
        if len(json_summary) <= max_size:
            return json_summary

        # 阶段 2：极简
        minimal = build_minimal(summary)
        minimal["message"] = (
            "数据体量极大，仅返回分站级汇总（时段数与总时长）。"
            "请指定 substation_code / 时间窗 / run_state / power_state 缩小范围。"
        )
        return json.dumps(
            convert_sets_to_lists(minimal),
            ensure_ascii=False,
            separators=(",", ":"),
            default=str,
        )

    @staticmethod
    def _compact_yb_info(records: List[Dict[str, Any]], max_size: int = 20000) -> str:
        """
        对测点异常数据进行压缩，异常事件只保留四个字段：
            异常类型描述、异常开始时间、异常结束时间、持续时长(分钟)
        阶段1：完整精简（测点信息 + 精简事件列表）
        阶段2：极简汇总（测点信息 + 异常条数，不含事件明细）
        阶段3：截断（保留部分测点，极端情况兜底）
        返回 JSON 字符串。
        """

        def _simplify_event(evt: Dict) -> Dict:
            type_desc = evt.get("异常类型描述") or ABNORMAL_TYPE_MAP.get(
                str(evt.get("异常类型编码", "")).zfill(3),
                evt.get("异常类型编码", ""),
            )
            return {
                "异常类型描述": type_desc,
                "异常开始时间": evt.get("异常开始时间", ""),
                "异常结束时间": evt.get("异常结束时间", ""),
                "持续时长(分钟)": evt.get("持续时长(分钟)", 0),
            }

        # ---------- 阶段1：精简后的完整列表 ----------
        simplified_points = []
        total_events = 0

        for point in records:
            if not isinstance(point, dict):
                continue

            events = point.get("异常事件列表", [])
            simplified_events = [
                _simplify_event(evt)
                for evt in (events if isinstance(events, list) else [])
                if isinstance(evt, dict)
            ]

            event_count = len(simplified_events)
            total_events += event_count

            simplified_points.append({
                "测点编码": point.get("测点编码", ""),
                "传感器类型名称": point.get("传感器类型名称", ""),
                "测点安装位置": point.get("测点安装位置", ""),
                "数值单位": point.get("数值单位", ""),
                "异常事件列表": simplified_events,
            })

        result_stage1 = {
            "total_nums": {
                "测点数": len(simplified_points),
                "异常总条数": total_events,
            },
            "message": "已精简异常事件字段，仅保留类型描述、开始时间、结束时间、持续时长。",
            "data": simplified_points,
        }

        json_stage1 = json.dumps(result_stage1, ensure_ascii=False, separators=(",", ":"), default=str)
        if len(json_stage1) <= max_size:
            return json_stage1

        # ---------- 阶段2：极简汇总 ----------
        minimal_points = [
            {
                "测点编码": p["测点编码"],
                "传感器类型名称": p["传感器类型名称"],
                "测点安装位置": p["测点安装位置"],
                "异常条数": len(p["异常事件列表"]),
            }
            for p in simplified_points
        ]

        result_stage2 = {
            "total_nums": result_stage1["total_nums"],
            "message": "数据量较大，仅返回测点异常条数汇总。请指定测点或时间范围缩小查询。",
            "data": minimal_points,
        }

        json_stage2 = json.dumps(result_stage2, ensure_ascii=False, separators=(",", ":"), default=str)
        if len(json_stage2) <= max_size:
            return json_stage2

        # ---------- 阶段3：截断（极端情况） ----------
        limit = max(10, max_size // 200)
        truncated_points = minimal_points[:limit]

        result_stage3 = {
            "total_nums": result_stage1["total_nums"],
            "message": f"数据极度庞大，已截断，仅保留前 {len(truncated_points)} 个测点。请务必缩小查询范围！",
            "data": truncated_points,
        }
        return json.dumps(result_stage3, ensure_ascii=False, separators=(",", ":"), default=str)

    # ==================== 4. Tools 注册入口 ====================
    def _register_tools(self):
        self._register_system_time_tool()
        self._register_todayornow_points_tool()
        self._register_points_list_tool()
        self._register_point_yb_info_tool()
        self._register_station_status_tool()
        self._register_station_point_tree_tool()
        self._register_aqjc_report_tool()    # ← 新增

    # ---------- 4.1 系统时间 ----------
    def _register_system_time_tool(self):
        @self.mcp.tool()
        def get_system_time() -> str:
            """
            【核心基础工具】获取服务器当前的系统时间。

            【使用场景】
            所有涉及时间范围查询的**第一步**。用户提及"今天"、"昨天"、"最近2小时"等相对时间时，必须先调用此工具获取基准时间。

            【返回值】
            JSON 字符串，包含:
            - current_time: 当前日期时间 (格式: "YYYY-MM-DD HH:MM:SS")
            - weekday: 当前星期 (如: "Monday")
            """
            now = datetime.now()
            return json.dumps(
                {
                    "current_time": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "weekday": now.strftime("%A"),
                },
                ensure_ascii=False,
            )

    # ---------- 4.2 今日/当前测点数据 ----------
    def _register_todayornow_points_tool(self):
        @self.mcp.tool()
        def query_todayornow_points(
                point_code_filters: Union[List[str], str, None] = None,
                sensor_type_filters: Union[List[str], str, None] = None,
                location_filters: Union[List[str], str, None] = None,
                substation_code_filters: Union[List[str], str, None] = None,
                numeric_filters: Optional[Dict[str, Dict]] = None,
                statistics_filters: Union[List[str], str, None] = None,
                today_or_now: bool = False,
        ) -> str:
            """
            【核心查询工具】查询今日或当前时刻的测点数据。

            【使用场景】
            查询今日或当前时刻的测点明细、实时状态、报警/门限异常，或需要按传感器类型/位置/分站统计时。

            【参数说明】
            - point_code_filters:     测点编码 (str 或 list，模糊匹配，相似度阈值 95)
            - sensor_type_filters:    传感器类型名称 (str 或 list，模糊匹配，阈值 70)
            - location_filters:       测点安装位置 (str 或 list，模糊匹配，阈值 70)
            - substation_code_filters:分站编码 (str 或 list，模糊匹配)
            - numeric_filters:        高级数值/时间过滤字典。格式: {"字段名": {"op": "操作符", "value": 值}}。
            - statistics_filters:     统计聚合项列表。可选: "总测点数", "测点列表",
                                      "传感器类型分布/个", "测点状态分布/条"
            - today_or_now:           True=今日 00:00~now；False=近10分钟（当前）。

            【返回值】
            JSON 字符串。结构: {"每日数据": {day: {point_code|statistics: {...}}}, "总共天数": N}
            """
            logger.info(
                f"query_todayornow_points called: point_code_filters={point_code_filters}, "
                f"sensor_type_filters={sensor_type_filters}, location_filters={location_filters}, "
                f"substation_code_filters={substation_code_filters}, "
                f"numeric_filters={numeric_filters}, statistics_filters={statistics_filters}, "
                f"today_or_now={today_or_now}"
            )
            step = 0
            try:
                # --- 1. 参数适配 ---
                step += 1
                point_codes = _normalize_filters(point_code_filters)
                sensor_types = _normalize_filters(sensor_type_filters)
                locations = _normalize_filters(location_filters)
                substations = _normalize_filters(substation_code_filters)

                # --- 2. 时间窗口 ---
                step += 1
                now = datetime.now()
                if today_or_now:
                    start_time = f"{now.date()} 00:00:00"
                    end_time = now.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    start_time = (now - timedelta(minutes=10)).strftime("%Y-%m-%d %H:%M:%S")
                    end_time = now.strftime("%Y-%m-%d %H:%M:%S")
                logger.info(f"step {step}: 时间窗口 start={start_time}, end={end_time}, "
                            f"today_or_now={today_or_now}")

                # --- 3. 过滤条件（拷贝以避免修改调用方字典） ---
                step += 1
                num_filters = dict(numeric_filters) if numeric_filters else None
                stat_filter = statistics_filters if statistics_filters else None

                if not today_or_now:
                    num_filters = num_filters or {}
                    num_filters.setdefault("结束时间", {"op": ">=", "value": start_time})
                    num_filters.setdefault("起始时间", {"op": "<=", "value": end_time})

                # --- 4. 查询 ---
                step += 1
                logger.info(f"step {step}: 调用 QyjcBase.get_points_daytype_with_cache 下发请求")
                points_records = self.qyjc_base.get_points_daytype_with_cache(
                    point_code_filters=point_codes,
                    sensor_type_filters=sensor_types,
                    location_filters=locations,
                    substation_code_filters=substations,
                    numeric_filters=num_filters,
                    statistics_filter=stat_filter,
                    start_date=start_time,
                    end_date=end_time,
                )

                if points_records:
                    with open("aqjc_history_data.txt", "w", encoding="utf-8") as f:
                        f.write(json.dumps(points_records, ensure_ascii=False, indent=2, default=str))
                    logger.debug("全部数据已写入 aqjc_history_data.txt")

                step += 1
                if not points_records or not points_records.get("每日数据"):
                    return json.dumps({"message": "未找到符合条件的测点记录"}, ensure_ascii=False)

                # --- 5. 序列化 ---
                step += 1
                json_full = json.dumps(
                    convert_sets_to_lists(points_records),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                )
                logger.info(f"step {step}: 序列化完成, 长度={len(json_full)}")

                # --- 6. 大响应压缩 ---
                if len(json_full) > MAX_JSON_SIZE:
                    step += 1
                    logger.info(f"step {step}: json_full > {MAX_JSON_SIZE}，压缩为统计摘要")
                    day_datas = points_records.get("每日数据", {})
                    simplified = {"每日数据": {}, "总共天数": points_records.get("总共天数", 0)}

                    for day, day_dict in day_datas.items():
                        try:
                            simplified["每日数据"][day] = generate_statistics(day_dict, DEFAULT_STATS_TODAY)
                        except Exception as e:
                            logger.error(f"step {step}: 压缩 {day} 失败: {e}")
                            simplified["每日数据"][day] = {}

                    simplified["message"] = (
                        "由于数据体量过大，仅返回每日统计摘要。"
                        "如需其他分布/明细，请指定 单个测点或进一步缩小查询范围。"
                        f"支持统计项: {DEFAULT_STATS_TODAY} 支持数据筛选项 {FILTERABLE_FIELDS}"
                    )
                    json_full = json.dumps(
                        convert_sets_to_lists(simplified),
                        ensure_ascii=False,
                        separators=(",", ":"),
                        default=str,
                    )
                    logger.info(f"step {step}: 压缩后 json 长度={len(json_full)}")

                step += 1
                logger.info(f"step {step}: 返回结果 长度={len(json_full)}")
                return json_full

            except Exception as e:
                logger.error(f"step {step}: traceback:\n{traceback.format_exc()}")
                return json.dumps({"error": "查询失败", "message": str(e)}, ensure_ascii=False)

    # ---------- 4.3 综合查询 ----------
    def _register_points_list_tool(self):
        @self.mcp.tool()
        def query_points_list(
                point_code_filters: Union[List[str], str, None] = None,
                sensor_type_filters: Union[List[str], str, None] = None,
                location_filters: Union[List[str], str, None] = None,
                substation_code_filters: Union[List[str], str, None] = None,
                numeric_filters: Optional[Dict[str, Dict]] = None,
                statistics_filters: Union[List[str], str, None] = None,
                start_date: Union[str, datetime, None] = None,
                end_date: Union[str, datetime, None] = None,
        ) -> str:
            """
            【核心查询工具】综合多条件测点数据查询与统计聚合。

            【使用场景】
            查询特定时间段内的测点明细、报警记录，或按传感器类型/位置/分站统计。

            【参数说明】
            - start_date: 起始时间 (格式: "YYYY-MM-DD HH:MM:SS"，默认当天 00:00:00)。
            - end_date:   截止时间 (格式: "YYYY-MM-DD HH:MM:SS"，默认当天 23:59:59)。

            【返回值】
            JSON 字符串。结构: {"每日数据": {day: {...}}, "总共天数": N}
            """
            logger.info(
                f"query_points_list called: point_code_filters={point_code_filters}, "
                f"sensor_type_filters={sensor_type_filters}, location_filters={location_filters}, "
                f"substation_code_filters={substation_code_filters}, "
                f"numeric_filters={numeric_filters}, statistics_filters={statistics_filters}, "
                f"start_date={start_date}, end_date={end_date}"
            )
            step = 0
            try:
                # --- 1. 参数适配 ---
                step += 1
                point_codes = _normalize_filters(point_code_filters)
                sensor_types = _normalize_filters(sensor_type_filters)
                locations = _normalize_filters(location_filters)
                substations = _normalize_filters(substation_code_filters)

                # --- 2. 时间窗口 ---
                step += 1
                today = datetime.now().date()
                start_time = start_date or f"{today} 00:00:00"
                end_time = end_date or f"{today} 23:59:59"
                logger.info(f"step {step}: 时间窗口 start={start_time}, end={end_time}")

                num_filters = numeric_filters if numeric_filters else None
                # 注意: 与 query_todayornow_points 不同，此处不向底层传 statistics_filter，
                #       由后续压缩阶段统一调用 point_filter 处理。

                # --- 3. 查询 ---
                step += 1
                points_records = self.qyjc_base.get_points_daytype_with_cache(
                    point_code_filters=point_codes,
                    sensor_type_filters=sensor_types,
                    location_filters=locations,
                    substation_code_filters=substations,
                    numeric_filters=num_filters,
                    start_date=start_time,
                    end_date=end_time,
                )

                if points_records:
                    with open("aqjc_history_data_multi.txt", "w", encoding="utf-8") as f:
                        f.write(json.dumps(points_records, ensure_ascii=False, indent=2, default=str))
                    logger.debug("全部数据已写入 aqjc_history_data_multi.txt")

                step += 1
                if not points_records or not points_records.get("每日数据"):
                    return json.dumps(
                        {"message": "未找到符合条件的测点记录，请从其他角度进行查询。"},
                        ensure_ascii=False,
                    )

                # --- 4. 序列化 ---
                step += 1
                json_full = json.dumps(
                    convert_sets_to_lists(points_records),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                )
                logger.info(f"step {step}: 序列化完成, 长度={len(json_full)}")

                # --- 5. 大响应压缩 ---
                if len(json_full) > MAX_JSON_SIZE:
                    step += 1
                    logger.info(f"step {step}: json_full > {MAX_JSON_SIZE}，压缩为统计摘要")
                    day_datas = points_records.get("每日数据", {})
                    simplified = {"每日数据": {}, "总共天数": points_records.get("总共天数", 0)}

                    for day, day_dict in day_datas.items():
                        try:
                            simplified["每日数据"][day] = self.qyjc_base.point_filter(
                                day_dict if isinstance(day_dict, dict) else {},
                                statistics_filter=DEFAULT_STATS_MULTI,
                            )
                        except Exception as e:
                            logger.error(f"step {step}: 压缩 {day} 失败: {e}")
                            simplified["每日数据"][day] = {}

                    simplified["message"] = (
                        "由于数据体量过大，仅返回每日统计摘要。"
                        "如需其他分布/明细，请指定 statistics_filters 或进一步缩小查询范围。"
                        f"支持统计项: {DEFAULT_STATS_MULTI} {FILTERABLE_FIELDS}"
                    )
                    json_full = json.dumps(
                        convert_sets_to_lists(simplified),
                        ensure_ascii=False,
                        separators=(",", ":"),
                        default=str,
                    )
                    logger.info(f"step {step}: 压缩后 json 长度={len(json_full)}")

                step += 1
                logger.info(f"step {step}: 返回结果  长度={len(json_full)}")
                return json_full

            except Exception:
                logger.error(f"step {step}: traceback:\n{traceback.format_exc()}")
                return json.dumps(
                    {"error": "当前查询失败，请尝试其他维度查询"},
                    ensure_ascii=False,
                )

    # ---------- 4.4 测点异常信息 ----------
    def _register_point_yb_info_tool(self):
        @self.mcp.tool()
        def query_point_yb_info(
                point_code: Optional[str] = None,
                start_time: Optional[str] = None,
                end_time: Optional[str] = None,
                real_status: bool = False,
        ) -> str:
            """
            【测点异常工具】查询测点异常/预警记录（历史或近12小时实时）。

            【使用场景】
            用户询问"某测点是否异常"、"有没有预警"、"标校/故障记录"等。

            【参数说明】
            - point_code: 测点编码（精确匹配，可选；不传则查询全部）。
            - start_time: 起始时间 (格式: "YYYY-MM-DD HH:MM:SS")。
            - end_time:   截止时间 (格式: "YYYY-MM-DD HH:MM:SS")。
            - real_status: 若为 True，自动忽略 start_time/end_time，强制查询近12小时。

            【返回值】
            JSON 字符串。
            - 结果 <= 30000 字符：返回完整异常记录列表。
            - 结果 >  30000 字符：自动压缩为按测点/异常类型聚合的摘要。
            - 无记录返回 {"message": "未查到异常信息"}。
            """
            logger.info(
                f"query_point_yb_info called: point_code={point_code}, "
                f"start_time={start_time}, end_time={end_time}, real_status={real_status}"
            )
            try:
                if real_status:
                    now = datetime.now()
                    start_time = (now - timedelta(hours=12)).strftime("%Y-%m-%d %H:%M:%S")
                    end_time = now.strftime("%Y-%m-%d %H:%M:%S")

                result = self.qyjc_base.get_yb_info(
                    point_code=point_code,
                    start_time=start_time,
                    end_time=end_time,
                )

                if result:
                    with open("query_point_yb_info.txt", "w", encoding="utf-8") as f:
                        f.write(json.dumps(result, ensure_ascii=False, indent=2, default=str))
                    logger.debug("全部数据已写入 query_point_yb_info.txt")

                if not result:
                    return json.dumps({"message": "未查到异常信息"}, ensure_ascii=False)

                # ==================== 序列化 ====================
                json_full = json.dumps(
                    convert_sets_to_lists(result),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                )
                logger.info(
                    f"query_point_yb_info: 原始结果 {len(result)} 条, json 长度 {len(json_full)}"
                )

                # ==================== 超长压缩 ====================
                if len(json_full) > MAX_JSON_SIZE:
                    json_full = self._compact_yb_info(result)
                    logger.info(f"query_point_yb_info: 触发压缩, 压缩后长度 {len(json_full)}")
                    with open("query_point_yb_info_yasuo.txt", "w", encoding="utf-8") as f:
                        f.write(json.dumps(json_full, ensure_ascii=False, indent=2, default=str))
                    logger.debug("压缩数据已写入 query_point_yb_info_yasuo.txt")

                return json_full

            except Exception as e:
                logger.error(
                    f"query_point_yb_info({point_code}, {start_time}, {end_time}) 异常: "
                    f"{e} {traceback.format_exc()}"
                )
                return json.dumps(
                    {"error": "当前查询失败，请尝试其他维度查询"},
                    ensure_ascii=False,
                )

    # ---------- 4.5 分站状态 ----------
    def _register_station_status_tool(self):
        @self.mcp.tool()
        def query_station_status(
                substation_code: Optional[str] = None,
                start_time: Optional[str] = None,
                end_time: Optional[str] = None,
                run_state: Optional[str] = None,
                power_state: Optional[str] = None,
                real_status: bool = False,
        ) -> str:
            """
            【设备状态工具】查询分站（分站/基站）的历史或实时运行与供电状态。

            【使用场景】
            用户询问"某分站是否在线"、"通讯是否中断"、"电源是否有故障"。

            【参数说明】
            - substation_code: 分站编码/名称（支持模糊匹配，相似度>90）。
            - start_time: 起始时间 (格式: "YYYY-MM-DD HH:MM:SS")。
            - end_time:   截止时间 (格式: "YYYY-MM-DD HH:MM:SS")。
            - run_state:  运行状态过滤（如: "通信正常", "通信中断", "故障", "未知"）。
            - power_state:供电状态过滤（如: "交流供电", "直流供电", "电源故障", "未知"）。
            - real_status: 若为 True，自动忽略 start_time/end_time，强制查询近12小时。

            【返回值】
            JSON 字符串。以分站编码为 Key 的分段状态记录字典。
            """
            logger.info(
                f"query_station_status called: substation_code={substation_code}, "
                f"start_time={start_time}, end_time={end_time}, "
                f"run_state={run_state}, power_state={power_state}, real_status={real_status}"
            )
            try:
                if real_status:
                    now = datetime.now()
                    start_time = (now - timedelta(hours=12)).strftime("%Y-%m-%d %H:%M:%S")
                    end_time = now.strftime("%Y-%m-%d %H:%M:%S")

                # 组装 where_clause（仅允许列名/值）
                where_clauses = []
                if start_time:
                    where_clauses.append(f"ENTRY_TIME >= '{start_time}'")
                if end_time:
                    where_clauses.append(f"ENTRY_TIME <= '{end_time}'")
                where_clause_sql = " AND ".join(where_clauses) if where_clauses else None

                raw = self.qyjc_base.get_fz_status(
                    realtime=real_status,
                    where_clause=where_clause_sql,
                )
                if raw:
                    with open("query_station_status.txt", "w", encoding="utf-8") as f:
                        f.write(json.dumps(raw, ensure_ascii=False, indent=2, default=str))
                    logger.debug("全部数据已写入 query_station_status.txt")

                # ==================== 解析：按 dict 结构 ====================
                res_new = {}
                for fz_code, fz_info in (raw or {}).items():
                    if not isinstance(fz_info, dict):
                        logger.warning(
                            f"query_station_status: 分站 {fz_code} 返回结构异常: {type(fz_info)}"
                        )
                        continue

                    fz_code_value = str(fz_info.get("分站编码", fz_code) or "").strip()
                    fz_location = str(fz_info.get("分站安装位置", "") or "").strip()
                    periods = fz_info.get("时段", []) or []

                    # ---- 分站模糊匹配 ----
                    if substation_code:
                        sc_str = str(substation_code).strip()
                        scores = [
                            fuzz.partial_ratio(sc_str, str(fz_code)),
                            fuzz.partial_ratio(sc_str, fz_code_value),
                        ]
                        if fz_location:
                            scores.append(fuzz.partial_ratio(sc_str, fz_location))
                        if max(scores) <= 90:
                            continue

                    if not isinstance(periods, list):
                        continue

                    # ---- 时段过滤 ----
                    filtered_periods = []
                    for period in periods:
                        if not isinstance(period, dict):
                            filtered_periods.append(period)
                            continue
                        if run_state and period.get("分站运行状态") != run_state:
                            continue
                        if power_state and period.get("分站供电状态") != power_state:
                            continue
                        filtered_periods.append(period)

                    if filtered_periods:
                        res_new[fz_code] = {
                            "分站编码": fz_code_value or str(fz_code),
                            "分站安装位置": fz_location,
                            "时段": filtered_periods,
                        }

                if not res_new:
                    info = (
                        f"未查到分站状态信息, substation_code={substation_code}, "
                        f"where_clause={where_clause_sql}"
                    )
                    logger.info(info)
                    return json.dumps({"message": info}, ensure_ascii=False)

                # ==================== 序列化 ====================
                json_full = json.dumps(
                    convert_sets_to_lists(res_new),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                )
                logger.info(
                    f"query_station_status: 原始结果 {len(res_new)} 个分站, "
                    f"json 长度 {len(json_full)}"
                )

                # ==================== 超长压缩 ====================
                if len(json_full) > MAX_JSON_SIZE:
                    json_full = self._compact_station_status(res_new)
                    logger.info(f"query_station_status: 触发压缩, 压缩后长度 {len(json_full)}")

                return json_full

            except Exception:
                logger.error("query_station_status traceback:\n%s", traceback.format_exc())
                return json.dumps(
                    {"error": "当前查询失败，请尝试其他维度查询"},
                    ensure_ascii=False,
                )

    # ---------- 4.6 分站 ↔ 测点关联树 ----------
    def _register_station_point_tree_tool(self):
        @self.mcp.tool()
        def query_station_point_tree(
                substation_code: Optional[str] = None,
                substation_location: Optional[str] = None,
                point_location: Optional[str] = None,
                point_code: Optional[str] = None,
                sensor_type: Union[List[str], str, None] = None,
                fields: Union[List[str], str, None] = None,
                with_points: bool = True,
        ) -> str:
            """
            【基础信息工具】查询分站与测点关联树，支持按传感器类型过滤和字段投影。

            【使用场景】
            - "有哪些分站 / 传感器类型 / 测点安装位置" → 不传任何过滤参数
            - "某分站下挂有哪些测点" → substation_code
            - "某传感器类型的测点分布" → sensor_type
            - "只返回测点编码和传感器类型" → fields="测点编码" / ["测点编码","传感器类型名称"]
            - "只拉取传感器类型清单" → fields="传感器类型名称"

            【参数说明】(全部可选)
            - substation_code:      分站编码（模糊，>60）
            - substation_location:  分站安装位置（模糊，>60）
            - point_location:       测点安装位置（模糊，>60）
            - point_code:           测点编码（模糊，>60）
            - sensor_type:          传感器类型名称（模糊，>60），支持 str 或 list，如 "风速" / ["甲烷","一氧化碳"]
            - fields:               字段投影。单个字符串 或 列表。当询问信息时可从下面字段中进行选择
                                    可选值: "传感器类型列表", "测点安装位置列表", "测点编码列表", "测点位置——编码对应关系","分站安装位置列表", "分站编码列表","分站编码-安装位置对应关系","分站数量","分站安装位置数量","传感器类型数量","测点安装位置数量","测点数量"
            - with_points:          是否返回测点明细，默认 True。

            【返回值】
            - 无任何参数：返回枚举清单  "传感器类型列表", "测点安装位置列表", "测点编码列表", "测点位置——编码对应关系","分站安装位置列表", "分站编码列表","分站编码-安装位置对应关系","分站数量","分站安装位置数量","传感器类型数量","测点安装位置数量","测点数量"
            - 仅传 fields：返回该字段的去重集合
            - 有其它过滤参数：按分站聚合并返回测点明细（受 fields 投影控制）
            """
            logger.info(
                f"query_station_point_tree called: substation_code={substation_code}, "
                f"substation_location={substation_location}, point_location={point_location}, "
                f"point_code={point_code}, sensor_type={sensor_type}, "
                f"fields={fields}, with_points={with_points}"
            )
            try:
                # ---------- 1. 拉取基础档案 ----------
                cddy = self.qyjc_base.get_cddy_info() or {}   # {测点编码: {...}}
                fzdy = self.qyjc_base.get_fzdy_info() or {}   # {分站编码: {...}}

                # ---------- 2. 参数预处理 ----------
                if fields is None:
                    field_list = None
                elif isinstance(fields, str):
                    field_list = [fields.strip()] if fields.strip() else None
                elif isinstance(fields, list):
                    field_list = [str(f).strip() for f in fields if str(f).strip()]
                else:
                    field_list = None

    
                if sensor_type is None:
                    sensor_type_list = None
                elif isinstance(sensor_type, str):
                    sensor_type_list = [sensor_type.strip()] if sensor_type.strip() else None
                elif isinstance(sensor_type, list):
                    sensor_type_list = [str(s).strip() for s in sensor_type if str(s).strip()]
                else:
                    sensor_type_list = None

                def project_point(pt: Dict) -> Dict:
                    """只保留 field_list 指定的测点级字段"""
                    if not field_list:
                        return pt
                    return {k: pt.get(k, "") for k in field_list if k in ALLOWED_POINT_FIELDS}

                # ---------- 3. 分支判断 ----------
                has_filter = any([
                    substation_code, substation_location,
                    point_location, point_code, sensor_type_list,
                ])
                has_fields = field_list is not None

                # 3a. 无过滤 + 无 fields → 枚举清单
                
                fz_codes = sorted({str(k) for k in fzdy.keys()})
                fz_locations = sorted({
                    str(v.get("分站安装位置", "")).strip()
                    for v in fzdy.values()
                    if isinstance(v, dict) and str(v.get("分站安装位置", "")).strip()
                })
                
                
                fz_code_to_location = {}
                for fz_code, fz_info in fzdy.items():
                    if not isinstance(fz_info, dict):
                        continue
                    loc = str(fz_info.get("分站安装位置", "")).strip()
                    fz_code_to_location[str(fz_code)] = loc

                # 只保留有编码的、排序输出
                fz_code_to_location = dict(sorted(fz_code_to_location.items()))
                
                sensor_types_all = sorted({
                    str(v.get("传感器类型名称", "")).strip()
                    for v in cddy.values()
                    if isinstance(v, dict) and str(v.get("传感器类型名称", "")).strip()
                })
                cd_locations = sorted({
                    str(v.get("测点安装位置", "")).strip()
                    for v in cddy.values()
                    if isinstance(v, dict) and str(v.get("测点安装位置", "")).strip()
                })
                cd_nums =    sorted({
                    str(v.get("测点编码", "")).strip()
                    for v in cddy.values()
                    if isinstance(v, dict) and str(v.get("测点编码", "")).strip()
                })
                
                loc_to_nums = defaultdict(set)
                for v in cddy.values():
                    if not isinstance(v, dict):
                        continue
                    loc = str(v.get("测点安装位置", "")).strip()
                    num = str(v.get("测点编码", "")).strip()
                    if loc and num:
                        loc_to_nums[loc].add(num)

                # 排序输出
                loc_to_nums = {
                    loc: sorted(nums) for loc, nums in sorted(loc_to_nums.items())
                }
                
                out = {
                    "分站编码列表": fz_codes,
                    "分站安装位置列表": fz_locations,
                    "分站编码-安装位置对应关系":fz_code_to_location,
                    "传感器类型列表": sensor_types_all,
                    "测点安装位置列表": cd_locations,
                    "测点编码列表":cd_nums,
                    "测点位置——编码对应关系":loc_to_nums,
                    "分站数量": len(fz_codes),
                    "分站安装位置数量": len(fz_locations),
                    "传感器类型数量": len(sensor_types_all),
                    "测点安装位置数量": len(cd_locations),
                    "测点数量":len(cd_nums)
                }
                
                
                if not has_filter and not has_fields:
                   
                    res_json = json.dumps(out, ensure_ascii=False, separators=(",", ":"), default=str)
                    logger.info(f"query_station_point_tree: 无参枚举返回, 长度={len(res_json)}")
                    return res_json

                # 3b. 只传 fields（无其它过滤）→ 拉取该字段的去重集合
                if not has_filter and has_fields:
                    
                    filtered_out = {k: out[k] for k in field_list if k in out}

                    res_json = json.dumps(
                        filtered_out,
                        ensure_ascii=False,
                        separators=(",", ":"),
                        default=str
                    )
                    logger.info(f"query_station_point_tree: 参枚举{field_list} 返回, 长度={len(res_json)}")
                    return res_json

                # ---------- 4. 筛选分站 ----------
                selected_fz = {}
                for fz_code, fz_info in fzdy.items():
                    if not isinstance(fz_info, dict):
                        continue
                    fz_loc = str(fz_info.get("分站安装位置", "")).strip()

                    if substation_code:
                        score = max(
                            fuzz.partial_ratio(str(substation_code), str(fz_code)),
                            fuzz.partial_ratio(str(substation_code), fz_loc),
                        )
                        if score <= 60:
                            continue
                    if substation_location:
                        if fuzz.partial_ratio(str(substation_location), fz_loc) <= 60:
                            continue
                    selected_fz[str(fz_code)] = fz_info

                # ---------- 5. 聚合测点（含 sensor_type 多值 OR 过滤） ----------
                def match_sensor_type(name: str, filters: List[str], threshold: int = 60) -> bool:
                    if not filters:
                        return True
                    return any(fuzz.partial_ratio(f, name) > threshold for f in filters)

                fz_to_points = defaultdict(list)
                for cd_code, cd_info in cddy.items():
                    if not isinstance(cd_info, dict):
                        continue
                    cd_loc = str(cd_info.get("测点安装位置", "")).strip()
                    sensor_name = str(cd_info.get("传感器类型名称", "")).strip()

                    if point_code and (
                        fuzz.partial_ratio(str(point_code), str(cd_code)) <= 60
                        and fuzz.partial_ratio(str(point_code), cd_loc) <= 60
                    ):
                        continue
                    if point_location and fuzz.partial_ratio(str(point_location), cd_loc) <= 60:
                        continue
                    if not match_sensor_type(sensor_name, sensor_type_list):
                        continue

                    fz_code = str(cd_info.get("分站编码", "")).strip()
                    fz_to_points[fz_code].append({
                        "测点编码": str(cd_code),
                        "传感器类型名称": sensor_name,
                        "测点安装位置": cd_loc,
                        "数值单位": cd_info.get("数值单位", ""),
                        "分站编码": fz_code,
                    })

                # ---------- 6. 组合结果 ----------
                target_fz_codes = (
                    set(selected_fz.keys())
                    if (substation_code or substation_location)
                    else set(fz_to_points.keys())
                )

                result = {}
                total_points = 0
                sensor_type_counter = Counter()

                for fz_code in sorted(target_fz_codes):
                    pts = fz_to_points.get(fz_code, [])
                    # 传了测点级过滤条件时，只保留有命中的分站
                    if (point_location or point_code or sensor_type_list) and not pts:
                        continue

                    fz_info = fzdy.get(fz_code) or {}
                    entry = {
                        "分站编码": fz_code,
                        "分站安装位置": (
                            fz_info.get("分站安装位置", "") if isinstance(fz_info, dict) else ""
                        ),
                        "测点数": len(pts),
                    }
                    if with_points:
                        entry["测点列表"] = [project_point(pt) for pt in pts]

                    sc = Counter(pt["传感器类型名称"] for pt in pts if pt["传感器类型名称"])
                    if sc:
                        entry["传感器类型分布"] = dict(sc)

                    result[fz_code] = entry
                    total_points += len(pts)
                    sensor_type_counter.update(
                        pt["传感器类型名称"] for pt in pts if pt["传感器类型名称"]
                    )

                if not result:
                    return json.dumps(
                        {"message": "未找到符合条件的分站/测点信息"},
                        ensure_ascii=False,
                    )

                # 若 fields 只含分站级字段，则去掉测点列表
                if field_list and not any(f in ALLOWED_POINT_FIELDS for f in field_list):
                    for fz_code in list(result.keys()):
                        if isinstance(result[fz_code], dict):
                            result[fz_code].pop("测点列表", None)

                result["total_nums"] = {"分站": len(result), "测点": total_points}
                result["传感器类型分布"] = dict(sensor_type_counter)

                res_json = json.dumps(
                    convert_sets_to_lists(result),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                )
                logger.info(
                    f"query_station_point_tree: 返回 {len(result) - 2} 个分站, "
                    f"{total_points} 个测点, 长度={len(res_json)}"
                )
                return res_json

            except Exception:
                logger.error(f"query_station_point_tree 异常: {traceback.format_exc()}")
                return json.dumps(
                    {"error": "当前查询失败，请尝试其他维度查询"},
                    ensure_ascii=False,
                )

        # ---------- 4.7 日报生成 ----------
    def _register_aqjc_report_tool(self):
        @self.mcp.tool()
        def generate_aqjc_report(
                start_date: Union[str, datetime, None] = None,
                end_date: Union[str, datetime, None] = None,
        ) -> str:
            """
            【报告生成工具】根据起止日期生成安全监控(AQJC)日报 / 周报/月报/多日报告，运行情况。

            【使用场景】
            用户说"出一份今天的报告"、"汇总近3天的测点情况"、"生成7月11日到7月14日的报告"等。

            【参数说明】
            - start_date: 起始日期 (格式: "YYYY-MM-DD HH:MM:SS" 或 "YYYY-MM-DD")，缺省为当天 00:00:00。
            - end_date:   截止日期 (格式: "YYYY-MM-DD HH:MM:SS" 或 "YYYY-MM-DD")，缺省为当天 23:59:59。

            【报告内容】
            一、统计内容
                1. 总测点数
                2. 测点列表（仅保留采样条数最多的前 5 个测点）
                3. 传感器类型分布/个
                4. 测点状态分布/条
            二、异常信息（按异常类型汇总 + 按测点聚合）
            三、分站状态（按运行状态/供电状态汇总；仅列出状态发生变化的分站及其变化过程）

            【返回值】
            JSON 字符串。
            - 单日报告:
                {"报告类型":"单日报告","日期":"...",
                 "统计内容":{...},"异常信息":{...},"分站状态":{...}}
            - 多日报告:
                {"报告类型":"多日报告","起始日期":"...","结束日期":"...","统计天数":N,
                 "统计内容":{...聚合...},"异常信息":{...},"分站状态":{...}}
              多日的分布类统计已跨天累加合并，测点列表按唯一测点合并后取 TOP5。
            """
            logger.info(
                f"generate_aqjc_report called with params: "
                f"start_date={start_date}, end_date={end_date}"
            )
            try:
                step = 0

                # ========== 时间处理 ==========
                def _normalize_time(t, is_end: bool):
                    if t is None:
                        return None
                    if isinstance(t, datetime):
                        return t.strftime("%Y-%m-%d %H:%M:%S")
                    t = str(t).strip()
                    if not t:
                        return None
                    if len(t) == 10:
                        return f"{t} 23:59:59" if is_end else f"{t} 00:00:00"
                    return t

                today = datetime.now().date()
                start_time = _normalize_time(start_date, is_end=False) or f"{today} 00:00:00"
                end_time = _normalize_time(end_date, is_end=True) or f"{today} 23:59:59"

                # ========== 拉取测点数据 ==========
                step += 1
                logger.info(f"step {step}: 拉取测点数据 [{start_time} ~ {end_time}]")
                points_records = self.qyjc_base.get_points_daytype_with_cache(
                    start_date=start_time,
                    end_date=end_time,
                )

                if not points_records or not points_records.get("每日数据"):
                    return json.dumps(
                        {"message": "未找到符合条件的测点记录，无法生成报告，请调整查询时间范围。"},
                        ensure_ascii=False,
                    )

                all_stats = [
                    "总测点数",
                    "测点列表",
                    "传感器类型分布/个",
                    "测点状态分布/条",
                ]

                day_datas = points_records.get("每日数据", {}) or {}
                days = sorted(list(day_datas.keys()))
                if not days:
                    return json.dumps(
                        {"message": "在指定时间范围内未找到任何每日数据，无法生成报告。"},
                        ensure_ascii=False,
                    )

                # ========== 逐日抽取统计 ==========
                step += 1
                logger.info(f"step {step}: 开始逐日统计, 天数={len(days)}")
                per_day_stats = {}
                for day in days:
                    try:
                        summary = self.qyjc_base.point_filter(
                            day_datas[day] if isinstance(day_datas[day], dict) else {},
                            statistics_filter=all_stats,
                        )
                    except Exception as e:
                        logger.warning(f"step {step}: point_filter 失败({e})，使用空统计")
                        summary = {}

                    # 兼容 {"statistics": {...}} 与直接返回统计字典两种结构
                    if isinstance(summary, dict) and "statistics" in summary:
                        stats = dict(summary.get("statistics", {}) or {})
                    elif isinstance(summary, dict):
                        stats = dict(summary)
                    else:
                        stats = {}

                    per_day_stats[day] = stats

                # ========== 单日 / 多日 报告结构 ==========
                if len(days) == 1:
                    step += 1
                    logger.info(f"step {step}: 生成单日报告")
                    stats = per_day_stats[days[0]]
                    if "测点列表" in stats:
                        stats["测点列表"] = _truncate_ranking(stats["测点列表"], top_n=5)
                    report = {
                        "报告类型": "单日报告",
                        "日期": days[0],
                        "统计内容": stats,
                    }
                else:
                    step += 1
                    logger.info(f"step {step}: 生成多日聚合报告, 天数={len(days)}")
                    agg_stats = _aggregate_multi_day_stats(
                        per_day_stats,
                        days,
                        top_list_key="测点列表",
                        top_n=5,
                        total_key="总测点数",
                    )
                    report = {
                        "报告类型": "多日报告",
                        "起始日期": days[0],
                        "结束日期": days[-1],
                        "统计天数": len(days),
                        "统计内容": agg_stats,
                    }

                # ========== 异常信息 ==========
                step += 1
                logger.info(f"step {step}: 查询测点异常 [{start_time} ~ {end_time}]")
                try:
                    yb_result = self.qyjc_base.get_yb_info(
                        point_code=None,
                        start_time=start_time,
                        end_time=end_time,
                    )
                    report["异常信息"] = (
                        _summarize_yb_info(yb_result) if yb_result else "无异常信息"
                    )
                except Exception as e:
                    logger.error(
                        f"generate_aqjc_report 查询异常信息失败: {traceback.format_exc()}"
                    )
                    report["异常信息"] = f"查询失败: {str(e)}"

                # ========== 分站状态 ==========
                step += 1
                logger.info(f"step {step}: 查询分站状态 [{start_time} ~ {end_time}]")
                try:
                    where_sql = (
                        f"ENTRY_TIME >= '{start_time}' AND ENTRY_TIME <= '{end_time}'"
                    )
                    fz_result = self.qyjc_base.get_fz_status(
                        realtime=False,
                        where_clause=where_sql,
                    )
                    report["分站状态"] = _summarize_station_status(fz_result)
                except Exception as e:
                    logger.error(
                        f"generate_aqjc_report 查询分站状态失败: {traceback.format_exc()}"
                    )
                    report["分站状态"] = f"查询失败: {str(e)}"

                step += 1
                json_full = json.dumps(
                    convert_sets_to_lists(report),
                    ensure_ascii=False,
                    separators=(",", ":"),
                    default=str,
                )
                logger.info(f"step {step}: 生成报告完成, 长度={len(json_full)}")
                return json_full

            except Exception as e:
                logger.error(f"generate_aqjc_report 异常: {traceback.format_exc()}")
                return json.dumps(
                    {
                        "error": "报告生成失败",
                        "message": str(e),
                        "traceback": traceback.format_exc(),
                    },
                    ensure_ascii=False,
                )

# ==================== 测试辅助 ====================
def _pretty(title: str, result):
    print("\n" + "=" * 70)
    print(f"🧪 {title}")
    print("=" * 70)
    try:
        if isinstance(result, str):
            data = json.loads(result)
        elif isinstance(result, list):
            texts = []
            for item in result:
                if isinstance(item, dict) and "text" in item:
                    texts.append(item["text"])
                else:
                    texts.append(str(item))
            data = json.loads("".join(texts)) if texts else {}
        else:
            data = result

        if isinstance(data, dict):
            keys = list(data.keys())
            if len(keys) > 12:
                print(f"✅ 返回 {len(keys)} 个顶层 key，前 12 个: {keys[:12]} ...")
            else:
                print(f"✅ 返回 keys: {keys}")

            if "total_nums" in data:
                print(f"   total_nums = {data['total_nums']}")
            if "传感器类型分布" in data:
                st = data["传感器类型分布"]
                print(f"   传感器类型分布(前5) = {dict(list(st.items())[:5])}")

            for k, v in data.items():
                if isinstance(v, list):
                    print(f"   {k} (list, {len(v)} 项): {v[:5]}{' ...' if len(v) > 5 else ''}")
                elif isinstance(v, dict) and k not in ("total_nums", "传感器类型分布"):
                    sample_key = list(v.keys())[0] if v else None
                    if sample_key:
                        print(f"   {k} → 样本: {sample_key} = "
                              f"{json.dumps(v[sample_key], ensure_ascii=False)[:200]}")
                    break
        else:
            print(f"✅ 返回: {str(data)[:500]}")
        return data
    except Exception as e:
        print(f"⚠️ 输出解析失败: {e}，原始返回: {str(result)[:500]}")
        return result


# ==================== 测试：query_station_point_tree ====================
async def test_query_station_point_tree(mcp_app: FastMCP):
    # 单日报告（只传日期也行）
    result = await mcp_app.call_tool("generate_aqjc_report", {
        "start_date": "2026-09-10",
        "end_date":   "2026-09-17",
    })
    print(result)
    # # 多日报告
    # result = await mcp_app.call_tool("generate_aqjc_report", {
    #     "start_date": "2026-09-07 00:00:00",
    #     "end_date":   "2026-09-10 23:59:59",
    # })

    # # 不传参数 → 默认当天
    # result = await mcp_app.call_tool("generate_aqjc_report", {})
    # print("\n" + "🔥" * 20)
    # print("  query_station_point_tree 全量测试")
    # print("🔥" * 20)

    # results_summary = []

    # async def run(title: str, params: dict):
    #     try:
    #         res = await mcp_app.call_tool("query_station_point_tree", params)
    #         data = _pretty(title, res)
    #         results_summary.append((title, "OK", data))
    #         return data
    #     except Exception as e:
    #         print("\n" + "=" * 70)
    #         print(f"❌ {title} 异常")
    #         print("=" * 70)
    #         print(traceback.format_exc())
    #         results_summary.append((title, "ERROR", str(e)))
    #         return None

    # # ---------- 一、枚举分支 ----------
    # print("\n\n########## 一、枚举分支（无参） ##########")
    # await run("1.1 无参调用 → 枚举清单", {})

    # # ---------- 二、fields 去重分支 ----------
    # print("\n\n########## 二、fields 去重分支 ##########")
    # await run("2.1 fields=传感器类型名称 (str)", {"fields": "传感器类型名称"})
    # await run("2.2 fields=分站编码 (str)",       {"fields": "分站编码"})
    # await run("2.3 fields=分站安装位置 (str)",   {"fields": "分站安装位置"})
    # await run("2.4 fields=测点安装位置 (str)",   {"fields": "测点安装位置"})
    # await run("2.5 fields=[分站编码, 分站安装位置] (list)",
    #           {"fields": ["分站编码", "分站安装位置"]})
    # await run("2.6 fields=[测点编码, 传感器类型名称, 测点安装位置] (list)",
    #           {"fields": ["测点编码", "传感器类型名称", "测点安装位置"]})
    # await run("2.7 fields=[] 空列表 → 等价无参", {"fields": []})
    # await run("2.8 fields=不存在字段 → 报错", {"fields": "不存在的字段"})
    # await run("2.9 fields=[合法, 非法] → 报错", {"fields": ["测点编码", "非法字段"]})

    # # ---------- 三、聚合分支 — sensor_type ----------
    # print("\n\n########## 三、聚合分支 — sensor_type ##########")
    # await run("3.1 sensor_type=甲烷 (单值)", {"sensor_type": "甲烷"})
    # await run("3.2 sensor_type=风速 (单值)", {"sensor_type": "风速"})
    # await run("3.3 sensor_type=[甲烷, 一氧化碳] (多值 OR)",
    #           {"sensor_type": ["甲烷", "一氧化碳"]})
    # await run("3.4 sensor_type=[风速, 温度, 粉尘] (多值 OR)",
    #           {"sensor_type": ["风速", "温度", "粉尘"]})
    # await run("3.5 sensor_type=风门 (可能匹配多个)", {"sensor_type": "风门"})
    # await run("3.6 sensor_type=不存在的类型 → 空结果",
    #           {"sensor_type": "不存在的类型XYZ"})

    # # ---------- 四、聚合分支 — 分站过滤 ----------
    # print("\n\n########## 四、聚合分支 — 分站过滤 ##########")
    # await run("4.1 substation_code=1100001", {"substation_code": "1100001"})
    # await run("4.2 substation_code=1100002", {"substation_code": "1100002"})
    # await run("4.3 substation_code=110 (模糊→多个分站)", {"substation_code": "110"})
    # await run("4.4 substation_location=主排水泵房", {"substation_location": "主排水泵房"})
    # await run("4.5 substation_location=变电所 (模糊)", {"substation_location": "变电所"})
    # await run("4.6 substation_location=通风机房", {"substation_location": "通风机房"})
    # await run("4.7 substation_location=不存在的分站位置 → 空结果",
    #           {"substation_location": "不存在的分站位置XYZ"})

    # # ---------- 五、聚合分支 — 测点过滤 ----------
    # print("\n\n########## 五、聚合分支 — 测点过滤 ##########")
    # await run("5.1 point_location=皮带机头 (模糊)", {"point_location": "皮带机头"})
    # await run("5.2 point_location=主排水泵房 (精确)", {"point_location": "主排水泵房"})
    # await run("5.3 point_location=回风大巷 (模糊)", {"point_location": "回风大巷"})
    # await run("5.4 point_code=1101MN000300000273 (精确)",
    #           {"point_code": "1101MN000300000273"})
    # await run("5.5 point_code=1101MN0003 (前缀模糊)", {"point_code": "1101MN0003"})
    # await run("5.6 point_code=不存在 → 空结果", {"point_code": "ZZZZ_NOT_EXIST"})

    # # ---------- 六、fields + 过滤 组合 ----------
    # print("\n\n########## 六、fields + 过滤 组合 ##########")
    # await run("6.1 sensor_type=甲烷 + fields=[测点编码]",
    #           {"sensor_type": "甲烷", "fields": "测点编码"})
    # await run("6.2 sensor_type=甲烷 + fields=[测点编码, 测点安装位置]",
    #           {"sensor_type": "甲烷", "fields": ["测点编码", "测点安装位置"]})
    # await run("6.3 sensor_type=[甲烷,风速] + fields=[测点编码, 传感器类型名称, 测点安装位置]",
    #           {"sensor_type": ["甲烷", "风速"],
    #            "fields": ["测点编码", "传感器类型名称", "测点安装位置"]})
    # await run("6.4 substation_code=1100001 + fields=[测点编码, 数值单位]",
    #           {"substation_code": "1100001", "fields": ["测点编码", "数值单位"]})
    # await run("6.5 substation_location=变电所 + fields=[测点安装位置]",
    #           {"substation_location": "变电所", "fields": "测点安装位置"})
    # await run("6.6 point_location=皮带机头 + fields=[测点编码, 传感器类型名称]",
    #           {"point_location": "皮带机头", "fields": ["测点编码", "传感器类型名称"]})
    # await run("6.7 substation_location=变电所 + fields=[分站安装位置] → 应无测点列表",
    #           {"substation_location": "变电所", "fields": "分站安装位置"})
    # await run("6.8 substation_location=变电所 + fields=[分站编码, 分站安装位置]",
    #           {"substation_location": "变电所", "fields": ["分站编码", "分站安装位置"]})

    # # ---------- 七、with_points=False ----------
    # print("\n\n########## 七、with_points=False ##########")
    # await run("7.1 substation_code=1100001 + with_points=False",
    #           {"substation_code": "1100001", "with_points": False})
    # await run("7.2 substation_location=变电所 + with_points=False",
    #           {"substation_location": "变电所", "with_points": False})
    # await run("7.3 sensor_type=甲烷 + with_points=False",
    #           {"sensor_type": "甲烷", "with_points": False})

    # # ---------- 八、复合过滤 ----------
    # print("\n\n########## 八、复合过滤 ##########")
    # await run("8.1 substation_location=变电所 + sensor_type=甲烷",
    #           {"substation_location": "变电所", "sensor_type": "甲烷"})
    # await run("8.2 point_location=皮带机头 + sensor_type=烟雾",
    #           {"point_location": "皮带机头", "sensor_type": "烟雾"})
    # await run("8.3 substation_code=1100002 + sensor_type=[甲烷,温度]",
    #           {"substation_code": "1100002", "sensor_type": ["甲烷", "温度"]})
    # await run("8.4 substation_location=避难硐室 + sensor_type=氧气 + fields=[测点编码,测点安装位置]",
    #           {"substation_location": "避难硐室", "sensor_type": "氧气",
    #            "fields": ["测点编码", "测点安装位置"]})
    # await run("8.5 全参数叠加 + with_points=False",
    #           {"substation_code": "1100002", "sensor_type": "甲烷",
    #            "fields": ["测点编码", "传感器类型名称"], "with_points": False})

    # # ---------- 九、边界与异常 ----------
    # print("\n\n########## 九、边界与异常 ##########")
    # await run("9.1 全部参数为 None → 枚举",
    #           {"substation_code": None, "substation_location": None,
    #            "point_location": None, "point_code": None,
    #            "sensor_type": None, "fields": None})
    # await run("9.2 空字符串过滤 → 视为无过滤",
    #           {"substation_code": "", "sensor_type": "", "fields": ""})
    # await run("9.3 sensor_type=[] 空列表 → 无过滤", {"sensor_type": []})
    # await run("9.4 fields=[测点编码, 测点编码] 重复 key",
    #           {"fields": ["测点编码", "测点编码"]})
    # await run("9.5 极模糊匹配 (单字 '甲')", {"sensor_type": "甲"})
    # await run("9.6 极模糊匹配 (单字 '风')", {"sensor_type": "风"})

    # ---------- 汇总 ----------
    # print("\n\n" + "=" * 70)
    # print("📊 测试汇总")
    # print("=" * 70)
    # total = len(results_summary)
    # ok_count = sum(1 for _, s, _ in results_summary if s == "OK")
    # err_count = total - ok_count

    # print(f"总用例: {total}  成功: {ok_count}  异常: {err_count}")
    # if err_count:
    #     print("\n异常用例列表:")
    #     for title, status, detail in results_summary:
    #         if status == "ERROR":
    #             print(f"  ❌ {title}")
    #             print(f"      {str(detail)[:200]}")
    # else:
    #     print("\n🎉 全部用例执行完毕，无未捕获异常。")


# ==================== 执行入口 ====================
if __name__ == "__main__":
    mcp_app = FastMCP("MineAQJCService")

    AQJCMcpService(
        mcp=mcp_app,
        host="10.11.3.210",
        port=8123,
        database="PS_NAO",
        user="default",
        password="xt123456",
    )

    asyncio.run(test_query_station_point_tree(mcp_app))