#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文件名:	person_tools.py
作者:	shihy
创建日期:	2026-04-22
描述:	矿井人员定位相关工具方法，提供人员最新入井记录查询、多人员状态筛选、分段轨迹分析、今日名单等能力。依赖 ClickHouse 实时/历史数据与接口服务，支持多维过滤与分析，适用于 MCP 对接的人员定位服务场景。
"""

import json
import logging
import re
import urllib3
import requests
import clickhouse_connect
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from fastmcp import FastMCP
import traceback
import time

from typing import List, Dict, Optional
import sys
import os
from pprint import pprint
from collections import defaultdict
import copy
from fuzzywuzzy import fuzz, process
from tqdm import tqdm
from typing import Union

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

from sqls.persons_sqls import (
    query_person_info,
    query_area_info,
    query_person_history,
    query_base_station_info,
    query_jizhan_history,
    query_warning_history,

)
from utils.person_utils import check_numeric_condition, PersonBase

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置日志，日志存储在文件中
from logging.handlers import RotatingFileHandler

LOG_DIR = 'logs'
LOG_FILE = os.path.join(LOG_DIR, 'HJL_person_service.log')

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


def match_state(item, run_state, power_state):
    # item: dict like {"基站运行状态": "...", "基站供电状态": "...", ...}
    if run_state and item.get("基站运行状态") != run_state:
        return False
    if power_state and item.get("基站供电状态") != power_state:
        return False
    return True


def convert_sets_to_lists(obj):
    """递归地将对象中的 set 转换为 list"""
    if isinstance(obj, set):
        return list(obj)
    elif isinstance(obj, dict):
        return {k: convert_sets_to_lists(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_sets_to_lists(item) for item in obj]
    else:
        return obj


# 在序列化之前转换


class PersonnelMCPService():
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
            "secure": True,
            "verify": False,
            "connect_timeout": 10,
            'autogenerate_session_id': False
        }

        try:
            self.client = clickhouse_connect.get_client(**self.db_config)
            logger.info("Successfully connected to ClickHouse.")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
        self.person_base = PersonBase(self.client, logger)

        self._register_resources()
        self._register_prompts()
        self._register_tools()

    # ==================== 1. Resource: 静态文档与数据字典 ====================
    def _register_resources(self):
        @self.mcp.resource("docs://personnel/data-dictionary")
        def get_data_dictionary() -> str:
            """
            获取矿井人员定位系统的数据字典、字段说明及查询指南。
            """
            return """
                # 矿井人员定位系统数据字典与查询指南

                ## 1. 数值与时间过滤条件 (numeric_filters)
                在调用 `query_personnel_list` 时，`numeric_filters` 参数支持对以下字段进行精细筛选。
                **注意**：时间格式统一为 "YYYY-MM-DD HH:MM:SS"，时长格式为 "HH:MM:SS"。

                ### 支持的操作符 (op):
                - `>`, `>=`: 大于 / 大于等于
                - `<`, `<=`: 小于 / 小于等于
                - `==`, `=`: 等于
                - `!=`: 不等于
                - `between`: 区间内 (value 为 [start, end] 列表，闭区间)
                - `not_between`: 不在区间内
                - `in`: 包含于列表 (value 为 list)
                - `after`, `since`: 等同于 `>=` (时间/数值)
                - `before`, `until`: 等同于 `<=` (时间/数值)

                ### 常用过滤字段示例:
                | 字段名 | 类型 | 说明 | 示例值 |
                | :--- | :--- | :--- | :--- |
                | **学历** | String | 人员学历 | ["小学", "初中", "高中", "专科", "中专", "大专", "本科", "硕士", "博士"] |
                | **是否矿领导** | String | 是否为矿级领导 | "是", "否" |
                | **是否特种人员** | String | 是否持有特种作业证 | "是", "否" |
                | **出生年月** | Date | 出生日期 | "1990-01-01" |
                | **工作状态** | String | 当前状态 | "正常", "求救" |
                | **当日是否已出井** | String | 当日最终状态 | "已出井", "井下", "井口" |
                | **入井时间** | DateTime | 最近一次入井时间 | "2026-07-14 08:00:00" |
                | **出井时间** | DateTime | 最近一次出井时间 | "2026-07-14 18:00:00" |
                | **入井时长** | Duration | 累计在井时长 | "05:00:00"|
                | **轨迹持续时间** | Duration | 轨迹记录总时长 | "01:00:00" |
                | **轨迹距离变化** | Int/Float | 轨迹移动距离(m) | 40 |
                | **轨迹开始时间** | DateTime | 轨迹起始点时间 | ["2026-07-14 14:00:00", "2026-07-14 18:00:00"] (配合 between) |
                | **轨迹结束时间** | DateTime | 轨迹结束点时间 | "2026-07-14 15:00:00" |

                ## 2. 统计聚合项 (statistics_filters)
                在 `query_personnel_list` 中，通过 `statistics_filters` 指定需要返回的统计维度。若不指定，默认返回精简摘要。

                ### 支持的统计 Key:
                1. **总人数**: 符合条件的人员总数。
                2. **人员列表_姓名_卡号_入井次数**: 详细人员名单及其基础出入井统计。
                3. **入井时长分布/人次**: 按不同时长区间统计的人数分布。
                4. **入井时间段分布/人次**: 按一天中不同时段入井的人数分布。
                5. **出井时间段分布/人次**: 按一天中不同时段出井的人数分布。
                6. **区域分布/条**: 人员在各个区域的分布情况。
                7. **基站分布/条**: 人员关联基站的分布情况。
                8. **基站停留时长分布/条**: 在不同基站停留时长的分布。
                9. **部门分布/人**: 按部门/队组统计的人数。
                10. **职位分布/人**: 按职务/职位统计的人数。
                11. **工种分布/人**: 按工种统计的人数。
                12. **每小时人数统计/人**: 按工种统计的人数。

                ## 3. 其他基础信息类型 (get_infos)
                - **person**: 人员基础信息（姓名、卡号、部门、工种等）。
                - **area_limit**: 区域基础信息（区域名称、编码、限制类型等）。
                - **station**: 基站基础信息（基站名称、编码、位置、状态等）。
                """

    # ==================== 2. Prompt: 全局行为准则 ====================
    def _register_prompts(self):
        @self.mcp.prompt()
        def analysis_guide() -> str:
            """
            获取矿井人员定位与设备状态分析的专业操作指南。
            模型在处理用户请求前，必须内化此指南，以确保工具调用的准确性、高效性和专业性。
            """
            return """
            你是一名资深的【矿井安全生产调度与数据分析专家】。你的核心任务是根据用户的自然语言描述，精准拆解需求，并组合调用系统提供的工具，返回结构化、可执行的分析结果。

            请严格遵循以下【核心工作流】与【工具使用规范】：

            ### 🔄 核心工作流 (Workflow)
            1. **时间对齐 (Time Alignment)**：只要用户提及相对时间（如“今天”、“昨天”、“最近2小时”、“本周”），**必须首先**调用 `get_system_time` 获取服务器当前时间，以此推算准确的 `start_date` 和 `end_date` (格式: YYYY-MM-DD HH:MM:SS)。
            2. **意图路由 (Intent Routing)**：根据用户需求，从下方的【工具路由矩阵】中选择最匹配的工具。
            3. **参数构建 (Parameter Construction)**：优先使用结构化过滤条件（如 `numeric_filters`），避免在获取全量数据后用 Python 逻辑二次过滤。
            4. **结果解释 (Result Interpretation)**：对返回的 JSON 数据进行专业解读。若数据量过大被系统压缩，应主动建议用户增加筛选条件（如指定部门、区域或缩短时间范围）。

            ---

            ### 🛠️ 工具路由矩阵与使用规范

            #### 1. `get_system_time` (时间基准)
            - **何时使用**：任何涉及时间范围的查询前。
            - **注意**：无参数。返回结果将作为后续所有工具时间参数的计算基准。

            #### 2. `query_todayornow_personlist` (核心：查询今日或者当前的井下人员数据)
            - **何时使用**：查询今日或者当前的人员名单、出入井记录、多维度统计分布、复杂条件筛选。
            - **关键参数技巧**：
              - **today_or_now**: 如果为True 返回今日井下数据。 如果为False 则返回近10分钟的数据即为目前井下数据。
              - **模糊匹配**：`person_name_filters`, `department_filters`, `area_filters` 等支持传入字符串或列表，系统会自动进行模糊匹配。
              - **高级数值/时间过滤 (`numeric_filters`)**：当用户提出“时长超过”、“属于某类”、“时间在区间内”时，**必须**使用此参数。
                *示例*：查询“入井时长超过8小时且非矿领导的本科员工”
                ```json
                {
                  "学历": {"op": "in", "value": ["本科", "硕士", "博士"]},
                  "是否矿领导": {"op": "==", "value": "否"},
                  "入井时长": {"op": ">", "value": "08:00:00"}
                }
                ```
              - **统计聚合 (`statistics_filters`)**：当用户询问“分布”、“汇总”、“多少人”时，传入统计 Key 列表（如 `["总人数", "区域分布/条", "部门分布/人"]`），系统将直接返回聚合结果，避免返回海量明细导致超时。

            #### 3. `query_personnel_list` (核心：人员综合查询与统计)
            - **何时使用**：查询多日或者不是今日/当前的人员名单、出入井记录、多维度统计分布、复杂条件筛选。
            - **关键参数技巧**：
              - **模糊匹配**：`person_name_filters`, `department_filters`, `area_filters` 等支持传入字符串或列表，系统会自动进行模糊匹配。
              - **高级数值/时间过滤 (`numeric_filters`)**：当用户提出“时长超过”、“属于某类”、“时间在区间内”时，**必须**使用此参数。
                *示例*：查询“入井时长超过8小时且非矿领导的本科员工”
                ```json
                {
                  "学历": {"op": "in", "value": ["本科", "硕士", "博士"]},
                  "是否矿领导": {"op": "==", "value": "否"},
                  "入井时长": {"op": ">", "value": "08:00:00"}
                }
                ```
              - **统计聚合 (`statistics_filters`)**：当用户询问“分布”、“汇总”、“多少人”时，传入统计 Key 列表（如 `["总人数", "区域分布/条", "部门分布/人"]`），系统将直接返回聚合结果，避免返回海量明细导致超时。

            #### 4. `query_warning_info` (安全预警查询)
            - **何时使用**：用户询问“报警”、“求救”、“超员”、“超时”、“限制区违规”等安全事件。
            - **关键参数**：`types` (可选: "超时报警", "超员报警", "求救报警", "限制区报警")。若设为 `real_status=True`，则自动查询近12小时的实时报警状态。

            #### 5. `query_station_status` (基站/设备状态排查)
            - **何时使用**：用户询问“基站是否在线”、“通讯中断”、“供电故障”、“某区域设备状态”。
            - **关键参数**：`base_station_name` 支持**模糊匹配**（无需精确编码）。`run_state` 和 `power_state` 可直接传入中文（如 "通讯中断", "电源故障"）。若 `real_status=True`，自动聚焦近12小时状态。

            #### 6. `get_infos` (基础档案与名录字典)
            - **何时使用**：用户需要“查找某人的卡号”、“列出所有区域”、“查看基站列表”或进行基础数据核对。
            - **关键参数**：`type` 必须是 "person", "area_limit", 或 "station" (支持列表如 `["person", "station"]`)。`name` 参数支持对姓名、区域名、基站名的**模糊匹配** (相似度>60即返回)。

            ---

            ### ⚠️ 异常处理与兜底策略
            1. **无数据**：若工具返回空或 "未查到..."，请明确告知用户“在指定条件下未找到相关记录”，并**主动提供缩小范围的建议**（例如：“是否扩大时间范围？”或“请确认姓名/区域名称是否准确？”）。
            2. **参数错误**：若工具返回参数校验错误，请检查时间格式是否为 `YYYY-MM-DD HH:MM:SS`，或 `numeric_filters` 的 `op` 是否为合法操作符 (`>`, `>=`, `<`, `<=`, `==`, `!=`, `between`, `in`)。
            3. **数据截断**：若返回结果包含 "由于数据体量过大..." 的 message，请向用户解释原因，并引导其使用 `statistics_filters` 或增加 `department_filters`/`area_filters` 进行下钻分析。

            请保持回答专业、客观、条理清晰。优先输出核心结论，再附带详细数据支撑。
            """

    def _register_tools(self):
        @self.mcp.tool()
        def get_system_time() -> str:
            """
            【核心基础工具】获取服务器当前的系统时间。

            【使用场景】
            这是所有涉及时间范围查询的**第一步**。当用户提及“今天”、“昨天”、“最近2小时”、“本周”等相对时间时，必须先调用此工具获取基准时间，再推算出准确的绝对时间。

            【参数】
            无。

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

        @self.mcp.tool()
        def query_todayornow_personlist(
                person_name_filters: Union[List[str], str, None] = None,
                department_filters: Union[List[str], str, None] = None,
                worktype_filters: Union[List[str], str, None] = None,
                duty_filters: Union[List[str], str, None] = None,
                area_filters: Union[List[str], str, None] = None,
                station_filters: Union[List[str], str, None] = None,
                numeric_filters: Optional[Dict[str, Dict]] = None,
                statistics_filters: Union[List[str], str, None] = None,
                today_or_now: bool = False,
        ) -> str:
            """
            【核心查询工具】查询今日或者目前井下人员数据。

            【使用场景】
            查询特定条件下的人员明细、出入井记录，或需要按区域/部门/工种等进行人数分布统计时。如果查询当日则将start_date传为当天0点时间,end_date为当前时间。
            【参数说明】
            - person_name_filters: 人员姓名 (支持字符串或列表，系统自动模糊匹配)。
            - department_filters: 部门/队组/班组 (支持字符串或列表，模糊匹配)。
            - worktype_filters: 工种 (支持字符串或列表，模糊匹配)。
            - duty_filters: 职务/职位 (支持字符串或列表，模糊匹配)。
            - area_filters: 区域名称 (支持字符串或列表，模糊匹配)。
            - station_filters: 基站名称 (支持字符串或列表，模糊匹配)。

            - numeric_filters: 高级数值/时间过滤字典。格式: {"字段名": {"op": "操作符", "value": 值}}。
              支持字段名: "学历", "是否矿领导", "是否特种人员", "出生年月", "工作状态", "当日是否已出井", "入井时间", "出井时间", "入井时长", "轨迹持续时间", "轨迹距离变化", "轨迹开始时间", "轨迹结束时间"。
              支持操作符: ">", ">=", "<", "<=", "==", "!=", "between", "in"。

              示例: {"入井时长": {"op": ">", "value": "08:00:00"}, "学历": {"op": "in", "value": ["本科", "硕士"]}}
            - statistics_filters: 统计聚合项列表。若提供，系统将优先返回聚合统计而非海量明细。
              可选值: "总人数", "人员列表_姓名_卡号_入井次数", "入井时长分布/人次", "入井时间段分布/人次", "出井时间段分布/人次", "区域分布/条", "基站分布/条", "基站停留时长分布/条", "部门分布/人", "职位分布/人", "工种分布/人","每小时人数统计/人"。
            - today_or_now: 如果为True 返回今日井下数据。 如果为False 则返回近10分钟的数据即为目前井下数据(当前、现在等时间都为False)。

            【返回值】
            JSON 字符串。包含符合条件的每日人员明细或统计聚合结果。若数据量过大，系统将自动压缩并返回摘要及 message 提示。
            """

            logger.info(
                f"query_todayornow_personlist called with params: "
                f"person_name_filters={person_name_filters}, department_filters={department_filters},"
                f"worktype_filters={worktype_filters}, duty_filters={duty_filters}, area_filters={area_filters}, "
                f"station_filters={station_filters}"
                f"numeric_filters={numeric_filters}, statistics_filter={statistics_filters}, today_or_now {today_or_now}"

            )

            # 参数适配
            try:
                step = 0
                logger.info(f"step {step}: 参数适配 - 处理姓名/部门/工种/职位/区域/站点/时间等条件")
                # 姓名
                names = None
                if person_name_filters is not None:
                    if isinstance(person_name_filters, str):
                        names = [person_name_filters.strip()]
                    elif isinstance(person_name_filters, list):
                        names = [str(n).strip() for n in person_name_filters if isinstance(n, str)]
                # 部门
                departments = None
                if department_filters is not None:
                    departments = [department_filters] if isinstance(department_filters, str) else department_filters
                # 工种
                work_types = None
                if worktype_filters is not None:
                    work_types = [worktype_filters] if isinstance(worktype_filters, str) else worktype_filters
                # 职务
                duties = None
                if duty_filters is not None:
                    duties = [duty_filters] if isinstance(duty_filters, str) else duty_filters
                # 区域
                areas = None
                if area_filters is not None:
                    areas = [area_filters] if isinstance(area_filters, str) else area_filters
                # 站点
                stations = None
                if station_filters is not None:
                    stations = [station_filters] if isinstance(station_filters, str) else station_filters

                # 时间
                step += 1;
                logger.info(f"step {step}: 检查和处理时间参数")

                now = datetime.now()

                # today为True: 开始时间为0点，结束时间为当前时间
                start_time = f"{now.date()} 00:00:00"
                end_time = now.strftime("%Y-%m-%d %H:%M:%S")

                # numeric_filters & statistics_filter (直接透传，验证见person_bases.py)
                step += 1;
                logger.info(f"step {step}: 处理 numeric_filters 和 statistics_filter")
                num_filters = numeric_filters if numeric_filters else None
                stat_filter = statistics_filters if statistics_filters else None

                # 查询
                step += 1;
                logger.info(
                    f"step {step}: 调用 get_person_infos_daytype_with_cache 下发筛选请求 参数: person_name_filters={names}, department_filters={departments}, worktype_filters={work_types}, duty_filters={duties}, area_filters={areas}, station_filters={stations}, start_date={start_time}, end_time={end_time}, numeric_filters={num_filters},today_or_now = {today_or_now} , statistics_filter={stat_filter}")
                if num_filters is None:
                    num_filters = {}

                if not today_or_now:
                    target_time = datetime.now() - timedelta(minutes=10)

                    # 格式化为指定的字符串格式
                    result = target_time.strftime("%Y-%m-%d %H:%M:%S")
                    now_time = datetime.now() + timedelta(minutes=1)
                    now_time = now_time.strftime("%Y-%m-%d %H:%M:%S")

                    num_filters[ "当日是否已出井"] = {
                        "op": "==",
                        "value": "井下"  # 已出井 / 井下 / 井口
                    },
                    # num_filters[ "轨迹开始时间"]  = {
                    #     "op": ">",
                    #     "value": result
                    # },
                    num_filters["轨迹结束时间"]={
                        "op": "between",
                        "value": [result, now_time]
                    },
                    logger.info(f"添加numeric_filters {num_filters} 成功")

                person_records = self.person_base.get_person_infos_daytype_with_cache(
                    person_name_filters=names,
                    department_filters=departments,
                    worktype_filters=work_types,
                    duty_filters=duties,
                    area_filters=areas,
                    station_filters=stations,
                    start_date=start_time,
                    end_date=end_time,
                    numeric_filters=num_filters,
                    now_or_today=True,
                    # statistics_filter=stat_filter,
                )
                if person_records:
                    with open("history_data.txt", "w", encoding="utf-8") as f:
                        f.write(json.dumps(person_records, ensure_ascii=False, indent=2))
                    print("全部数据已成功写入 history_data.txt")

                step += 1;
                logger.info(f"step {step}: 检查查询结果有效性")
                if not person_records:
                    return json.dumps({"message": "未找到符合条件的人员记录"}, ensure_ascii=False)

                step += 1;
                logger.info(f"step {step}: 序列化查询到的数据")
                json_full = json.dumps(person_records, ensure_ascii=False, separators=(",", ":"))
                logger.info(f"step {step}: 已经序列化查询结果, 序列化长度: {len(json_full)}")

                statistics_filters_values = [
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
                len_json_full = len(json_full)

                step += 1;
                logger.info(f"step {step}: 判断是否需要进行统计项过滤")
                statistics_filter_passed = statistics_filters is not None and len(statistics_filters) > 0
                logger.info(f"step {step}: statistics_filter_passed={statistics_filter_passed}")

                if statistics_filter_passed:
                    step += 1;
                    logger.info(f"step {step}: statistics_filter 不为空，进行精准统计项过滤抽取")
                    day_datas = person_records.get('每日数据')
                    filtered_record = {'每日数据': {}}
                    key_length_info = {}
                    oversize_keys = []
                    for key, day_data in day_datas.items():
                        logger.info(f"step {step}: 处理 {key} 的单日数据")
                        record = self.person_base.person_filter(
                            pdata=day_data,
                            statistics_filter=statistics_filters
                        )
                        filtered_stats = {}
                        messages = []
                        for stat_key in statistics_filters:
                            value = record['statistics'].get(stat_key)
                            single_stat_dict = {stat_key: value}
                            stat_json = json.dumps(single_stat_dict, ensure_ascii=False, separators=(",", ":"))
                            stat_len = len(stat_json)
                            key_length_info[(key, stat_key)] = stat_len
                            if stat_len > 10000:
                                message_warn = f"'{stat_key}' 统计项在 {key} 的数据体量过大，请进一步缩小查询范围或细化分布参数."
                                messages.append(message_warn)
                                logger.warning(f"step {step}: {message_warn}")
                            filtered_stats[stat_key] = value
                        filtered_record['每日数据'][key] = {"statistics": filtered_stats}
                        if messages:
                            filtered_record['每日数据'][key]['message'] = "; ".join(messages)
                            logger.info(f"step {step}: 超大统计项产生 message: {'; '.join(messages)}")
                    json_full = json.dumps(filtered_record, ensure_ascii=False, separators=(",", ":"))
                    len_json_full = len(json_full)
                    logger.info(f"step {step}: statistics_filter抽取后json长度: {len_json_full}")

                else:
                    if len_json_full > 30000:
                        step += 1;
                        logger.info(f"step {step}: json_full超过30k，对每日数据做第一次筛选压缩")
                        new_outs_record = {'每日数据': []}
                        day_datas = person_records.get('每日数据')
                        day_record = {}
                        for key, day_data in day_datas.items():
                            logger.info(f"step {step}: 进行person_filter筛选 key={key}")
                            record = self.person_base.person_filter(
                                pdata=day_data,
                                statistics_filter=statistics_filters_values
                            )
                            day_record[key] = [record]
                        new_outs_record['每日数据'] = day_record
                        new_outs_record_converted = convert_sets_to_lists(new_outs_record)
                        json_full = json.dumps(new_outs_record_converted, ensure_ascii=False, separators=(",", ":"))
                        len_json_full = len(json_full)
                        logger.info(f"step {step}: 首轮压缩后json长度: {len_json_full}")

                        if len_json_full > 30000:
                            step += 1;
                            logger.info(f"step {step}: 首轮压缩仍超30k，仅保留总人数与入井次数")
                            all_days_agg = {}
                            day_datas = person_records.get('每日数据')
                            for k in ["总人数", "人员列表_姓名_卡号_入井次数"]:
                                all_days_agg[k] = {}
                            for day, day_data in day_datas.items():
                                logger.info(f"step {step}: 处理 day={day} 的核心统计")
                                summary = self.person_base.person_filter(
                                    pdata=day_data,
                                    statistics_filter=["总人数", "人员列表_姓名_卡号_入井次数"]
                                )
                                for k in ["总人数", "人员列表_姓名_卡号_入井次数"]:
                                    if k in summary["statistics"]:
                                        all_days_agg[k][day] = summary["statistics"][k]
                                        logger.info(f"step {step}: 保存 {k} 统计 day={day}")
                            result_json = {
                                "出入井总人数": all_days_agg.get("总人数", {}),
                                "人员列表_姓名_卡号_入井次数": all_days_agg.get("人员列表_姓名_卡号_入井次数", {}),
                                "message": (
                                    f"由于数据体量过大，仅返回总人数与人员卡号入井汇总。如需其他分布/统计，请指定分布参数（如 [入井时长分布/人次]），支持的分布有{statistics_filters_values}"
                                    "或进一步缩小查询范围（如指定姓名、部门、时间段等）以获取更详细内容。"
                                )
                            }
                            json_full = json.dumps(result_json, ensure_ascii=False, separators=(",", ":"))
                            len_json_full = len(json_full)
                            logger.info(f"step {step}: 最精简核心统计后json长度: {len_json_full}")

                logger.info(
                    f"step {step}: 压缩人员 records, 当前json长度: len_json_full={len(json_full)}"
                )

                step += 1;
                logger.info(f"step {step}: 函数执行完成, 返回json_full")
                return json_full

            except Exception as e:
                logger.error(f"step {step}: traceback: %s", traceback.format_exc())
                return json.dumps({
                    "error": "查询失败",
                    "message": str(e),
                    "traceback": traceback.format_exc()
                }, ensure_ascii=False)

        @self.mcp.tool()
        def query_personnel_list(
                person_name_filters: Union[List[str], str, None] = None,
                department_filters: Union[List[str], str, None] = None,
                worktype_filters: Union[List[str], str, None] = None,
                duty_filters: Union[List[str], str, None] = None,
                area_filters: Union[List[str], str, None] = None,
                station_filters: Union[List[str], str, None] = None,
                numeric_filters: Optional[Dict[str, Dict]] = None,
                statistics_filters: Union[List[str], str, None] = None,
                start_date: Union[str, datetime, None] = None,
                end_date: Union[str, datetime, None] = None,
        ) -> str:
            """
            【核心查询工具】综合多条件人员名单查询与统计聚合。支持模糊匹配、数值/时间区间过滤及多维度统计。

            【使用场景】
            查询特定条件下的人员明细、出入井记录，或需要按区域/部门/工种等进行人数分布统计时。如果查询当日则将start_date传为当天0点时间,end_date为当前时间。
            【参数说明】
            - person_name_filters: 人员姓名 (支持字符串或列表，系统自动模糊匹配)。
            - department_filters: 部门/队组/班组 (支持字符串或列表，模糊匹配)。
            - worktype_filters: 工种 (支持字符串或列表，模糊匹配)。
            - duty_filters: 职务/职位 (支持字符串或列表，模糊匹配)。
            - area_filters: 区域名称 (支持字符串或列表，模糊匹配)。
            - station_filters: 基站名称 (支持字符串或列表，模糊匹配)。
            - start_date: 起始时间 (格式: "YYYY-MM-DD HH:MM:SS"，为空默认当天 00:00:00)。
            - end_date: 截止时间 (格式: "YYYY-MM-DD HH:MM:SS"，为空默认当天 23:59:59)。
            - numeric_filters: 高级数值/时间过滤字典。格式: {"字段名": {"op": "操作符", "value": 值}}。
              支持字段名: "学历", "是否矿领导", "是否特种人员", "出生年月", "工作状态", "当日是否已出井", "入井时间", "出井时间", "入井时长", "轨迹持续时间", "轨迹距离变化", "轨迹开始时间", "轨迹结束时间"。
              支持操作符: ">", ">=", "<", "<=", "==", "!=", "between", "in"。

              示例: {"入井时长": {"op": ">", "value": "08:00:00"}, "学历": {"op": "in", "value": ["本科", "硕士"]}}
            - statistics_filters: 统计聚合项列表。若提供，系统将优先返回聚合统计而非海量明细。
              可选值: "总人数", "人员列表_姓名_卡号_入井次数", "入井时长分布/人次", "入井时间段分布/人次", "出井时间段分布/人次", "区域分布/条", "基站分布/条", "基站停留时长分布/条", "部门分布/人", "职位分布/人", "工种分布/人"。

            【返回值】
            JSON 字符串。包含符合条件的每日人员明细或统计聚合结果。若数据量过大，系统将自动压缩并返回摘要及 message 提示。
            """

            logger.info(
                f"query_personnel_list called with params: "
                f"person_name_filters={person_name_filters}, department_filters={department_filters},"
                f"worktype_filters={worktype_filters}, duty_filters={duty_filters}, area_filters={area_filters}, "
                f"station_filters={station_filters}"
                f"numeric_filters={numeric_filters}, statistics_filter={statistics_filters}, "
                f"start_date={start_date}, end_date={end_date}"
            )

            # 参数适配
            try:
                step = 0
                logger.info(f"step {step}: 参数适配 - 处理姓名/部门/工种/职位/区域/站点/时间等条件")
                # 姓名
                names = None
                if person_name_filters is not None:
                    if isinstance(person_name_filters, str):
                        names = [person_name_filters.strip()]
                    elif isinstance(person_name_filters, list):
                        names = [str(n).strip() for n in person_name_filters if isinstance(n, str)]
                # 部门
                departments = None
                if department_filters is not None:
                    departments = [department_filters] if isinstance(department_filters, str) else department_filters
                # 工种
                work_types = None
                if worktype_filters is not None:
                    work_types = [worktype_filters] if isinstance(worktype_filters, str) else worktype_filters
                # 职务
                duties = None
                if duty_filters is not None:
                    duties = [duty_filters] if isinstance(duty_filters, str) else duty_filters
                # 区域
                areas = None
                if area_filters is not None:
                    areas = [area_filters] if isinstance(area_filters, str) else area_filters
                # 站点
                stations = None
                if station_filters is not None:
                    stations = [station_filters] if isinstance(station_filters, str) else station_filters

                # 时间
                step += 1;
                logger.info(f"step {step}: 检查和处理时间参数")
                start_time, end_time = start_date, end_date
                if not start_time or not end_time:
                    today = datetime.now().date()
                    if not start_time:
                        start_time = f"{today} 00:00:00"
                    if not end_time:
                        end_time = f"{today} 23:59:59"

                # numeric_filters & statistics_filter (直接透传，验证见person_bases.py)
                step += 1;
                logger.info(f"step {step}: 处理 numeric_filters 和 statistics_filter")
                num_filters = numeric_filters if numeric_filters else None
                stat_filter = statistics_filters if statistics_filters else None

                # 查询
                step += 1;
                logger.info(f"step {step}: 调用 get_person_infos_daytype_with_cache 下发筛选请求")
                person_records = self.person_base.get_person_infos_daytype_with_cache(
                    person_name_filters=names,
                    department_filters=departments,
                    worktype_filters=work_types,
                    duty_filters=duties,
                    area_filters=areas,
                    station_filters=stations,
                    start_date=start_time,
                    end_date=end_time,
                    numeric_filters=num_filters,
                    # statistics_filter=stat_filter,
                )

                if person_records:
                    with open("history_data_multi.txt", "w", encoding="utf-8") as f:
                        f.write(json.dumps(person_records, ensure_ascii=False, indent=2))
                    print("全部数据已成功写入 history_data_multi.txt")

                step += 1;
                logger.info(f"step {step}: 检查查询结果有效性")
                if not person_records:
                    return json.dumps({"message": "未找到符合条件的人员记录，请从其他角度进行查询。"}, ensure_ascii=False)

                step += 1;
                logger.info(f"step {step}: 序列化查询到的数据")
                json_full = json.dumps(person_records, ensure_ascii=False, separators=(",", ":"))
                logger.info(f"step {step}: 已经序列化查询结果, 序列化长度: {len(json_full)}")

                statistics_filters_values = [
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
                len_json_full = len(json_full)

                step += 1;
                logger.info(f"step {step}: 判断是否需要进行统计项过滤")
                statistics_filter_passed = statistics_filters is not None and len(statistics_filters) > 0
                logger.info(f"step {step}: statistics_filter_passed={statistics_filter_passed}")

                if statistics_filter_passed:
                    step += 1;
                    logger.info(f"step {step}: statistics_filter 不为空，进行精准统计项过滤抽取")
                    day_datas = person_records.get('每日数据')
                    filtered_record = {'每日数据': {}}
                    key_length_info = {}
                    oversize_keys = []
                    for key, day_data in day_datas.items():
                        logger.info(f"step {step}: 处理 {key} 的单日数据")
                        record = self.person_base.person_filter(
                            pdata=day_data,
                            statistics_filter=statistics_filters
                        )
                        filtered_stats = {}
                        messages = []
                        for stat_key in statistics_filters:
                            value = record['statistics'].get(stat_key)
                            single_stat_dict = {stat_key: value}
                            stat_json = json.dumps(single_stat_dict, ensure_ascii=False, separators=(",", ":"))
                            stat_len = len(stat_json)
                            key_length_info[(key, stat_key)] = stat_len
                            if stat_len > 10000:
                                message_warn = f"'{stat_key}' 统计项在 {key} 的数据体量过大，请进一步缩小查询范围或细化分布参数."
                                messages.append(message_warn)
                                logger.warning(f"step {step}: {message_warn}")
                            filtered_stats[stat_key] = value
                        filtered_record['每日数据'][key] = {"statistics": filtered_stats}
                        if messages:
                            filtered_record['每日数据'][key]['message'] = "; ".join(messages)
                            logger.info(f"step {step}: 超大统计项产生 message: {'; '.join(messages)}")
                    json_full = json.dumps(filtered_record, ensure_ascii=False, separators=(",", ":"))
                    len_json_full = len(json_full)
                    logger.info(f"step {step}: statistics_filter抽取后json长度: {len_json_full}")

                else:
                    if len_json_full > 30000:
                        step += 1;
                        logger.info(f"step {step}: json_full超过30k，对每日数据做第一次筛选压缩")
                        new_outs_record = {'每日数据': []}
                        day_datas = person_records.get('每日数据')
                        day_record = {}
                        for key, day_data in day_datas.items():
                            logger.info(f"step {step}: 进行person_filter筛选 key={key}")
                            record = self.person_base.person_filter(
                                pdata=day_data,
                                statistics_filter=statistics_filters_values
                            )
                            day_record[key] = [record]
                        new_outs_record['每日数据'] = day_record
                        new_outs_record_converted = convert_sets_to_lists(new_outs_record)
                        json_full = json.dumps(new_outs_record_converted, ensure_ascii=False, separators=(",", ":"))
                        len_json_full = len(json_full)
                        logger.info(f"step {step}: 首轮压缩后json长度: {len_json_full}")

                        if len_json_full > 30000:
                            step += 1;
                            logger.info(f"step {step}: 首轮压缩仍超30k，仅保留总人数与入井次数")
                            all_days_agg = {}
                            day_datas = person_records.get('每日数据')
                            for k in ["总人数", "人员列表_姓名_卡号_入井次数"]:
                                all_days_agg[k] = {}
                            for day, day_data in day_datas.items():
                                logger.info(f"step {step}: 处理 day={day} 的核心统计")
                                summary = self.person_base.person_filter(
                                    pdata=day_data,
                                    statistics_filter=["总人数", "人员列表_姓名_卡号_入井次数"]
                                )
                                for k in ["总人数", "人员列表_姓名_卡号_入井次数"]:
                                    if k in summary["statistics"]:
                                        all_days_agg[k][day] = summary["statistics"][k]
                                        logger.info(f"step {step}: 保存 {k} 统计 day={day}")
                            result_json = {
                                "出入井总人数": all_days_agg.get("总人数", {}),
                                "人员列表_姓名_卡号_入井次数": all_days_agg.get("人员列表_姓名_卡号_入井次数", {}),
                                "message": (
                                    f"由于数据体量过大，仅返回总人数与人员卡号入井汇总。如需其他分布/统计，请指定分布参数（如 [入井时长分布/人次]），支持的分布有{statistics_filters_values}"
                                    "或进一步缩小查询范围（如指定姓名、部门、时间段等）以获取更详细内容。"
                                )
                            }
                            json_full = json.dumps(result_json, ensure_ascii=False, separators=(",", ":"))
                            len_json_full = len(json_full)
                            logger.info(f"step {step}: 最精简核心统计后json长度: {len_json_full}")

                logger.info(
                    f"step {step}: 压缩人员 records, 当前json长度: len_json_full={len(json_full)}"
                )

                step += 1;
                logger.info(f"step {step}: 函数执行完成, 返回json_full")
                return json_full

            except Exception as e:
                logger.error(f"step {step}: traceback: %s", traceback.format_exc())
                return json.dumps({
                    "error": "当前查询失败，请尝试其他维度查询",
                    # "message": str(e),
                    # "traceback": traceback.format_exc()
                }, ensure_ascii=False)

        # INSERT_YOUR_CODE

        @self.mcp.tool()
        def query_warning_info(
                types: Optional[List[str]] = None,
                start_time: Optional[str] = None,
                end_time: Optional[str] = None,
                real_status: bool = False,
        ) -> str:
            """
           【安全预警工具】查询矿井报警信息（历史或实时）。

           【使用场景】
           用户询问“有没有报警”、“求救记录”、“超员/超时/限制区违规”等安全事件时。

           【参数说明】
           - types: 报警类型列表。可选值: ["超时报警", "超员报警", "求救报警", "限制区报警"]。若为 None 或空列表，则返回所有类型的报警。
           - start_time: 起始时间 (格式: "YYYY-MM-DD HH:MM:SS")。
           - end_time: 截止时间 (格式: "YYYY-MM-DD HH:MM:SS")。
           - real_status: 布尔值。若设为 True，将自动忽略 start_time/end_time，强制查询**近12小时**的实时报警状态。

           【返回值】
           JSON 字符串。按报警类型分组的字典，如 {"超时报警": [...], "求救报警": [...]}。若无记录，返回 {"message": "未查到报警信息"}。
           """
            logger.info(
                f"query_warning_info called: types={types}, start_time={start_time}, end_time={end_time}"
            )
            try:

                where_clauses = []

                # 注意：只有基站“编码”允许 SQL 精确过滤，基站名称走后期 Python 模糊匹配
                if real_status:
                    now = datetime.now()
                    start_time = (now - timedelta(hours=12)).strftime("%Y-%m-%d %H:%M:%S")
                    end_time = now.strftime("%Y-%m-%d %H:%M:%S")

                if start_time:
                    where_clauses.append(f"TIME >= '{start_time}'")
                if end_time:
                    where_clauses.append(f"TIME <= '{end_time}'")
                where_clause_sql = " AND ".join(where_clauses) if where_clauses else None

                result = query_warning_history(
                    self.client, where_clause=where_clause_sql
                )

                # 可用报警类型
                valid_types = {"超时报警", "超员报警", "求救报警", "限制区报警"}
                # 筛选类型
                out = {}
                # 若 types 参数为空或None，默认全查
                if not types:
                    types = list(valid_types)
                for k in types:
                    if k in result:
                        out[k] = result[k]
                # 检查是否有有效报警
                if any(isinstance(v, list) and len(v) > 0 for v in out.values()):
                    return json.dumps(out, ensure_ascii=False, separators=(",", ":"))
                else:
                    return json.dumps({"message": "未查到报警信息"}, ensure_ascii=False)
            except Exception as e:
                logger.error(
                    f"query_warning_info({types}, {start_time}, {end_time}) 异常: {e} {traceback.format_exc()}")
                return json.dumps(
                    {"error": "当前查询失败，请尝试其他维度查询"},
                    ensure_ascii=False
                )

        @self.mcp.tool()
        def query_station_status(
                base_station_name: Optional[str] = None,
                start_time: Optional[str] = None,
                end_time: Optional[str] = None,
                run_state: Optional[str] = None,  # 新增：基站运行状态过滤，可为int或str（'0', '1', '2', '9'等或对应数字）
                power_state: Optional[str] = None,  # 新增：供电方式过滤，可为int或str（'0', '1', '2', '9'等或对应数字）
                real_status: bool = False,
        ) -> str:
            """
            【设备状态工具】查询基站（分站）的历史或实时运行与供电状态分段记录。

            【使用场景】
            用户询问“某基站是否在线”、“通讯是否中断”、“电源是否有故障”或排查特定区域设备状态时。

            【参数说明】
            - base_station_name: 基站名称 (支持**模糊匹配**，无需精确编码，如传 "井口" 即可匹配所有含井口的基站)。
            - start_time: 起始时间 (格式: "YYYY-MM-DD HH:MM:SS")。
            - end_time: 截止时间 (格式: "YYYY-MM-DD HH:MM:SS")。
            - run_state: 运行状态过滤 (支持中文，如: "通讯正常", "通讯中断", "故障", "未知")。
            - power_state: 供电状态过滤 (支持中文，如: "直流供电", "交流供电", "电源故障", "未知")。
            - real_status: 布尔值。若设为 True，将自动忽略 start_time/end_time，强制查询**近12小时**的实时状态。

            【返回值】
            JSON 字符串。以基站名称为 Key，值为该基站状态分段记录列表的字典。
            示例: {"基站A": [{"基站运行状态": "通讯正常", "基站供电状态": "直流供电", "起始时间": "...", "结束时间": "..."}]}
            """
            logger.info(
                f"query_station_status_history called: base_station_code={base_station_name}, start_time={start_time},      end_time={end_time}, run_state={run_state}, power_state={power_state} ,real_status {real_status}"
            )
            try:
                # 重新拼接 where_clauses，兼容 base_station_name、start_time、end_time、run_state、power_state
                where_clauses = []

                # 注意：只有基站“编码”允许 SQL 精确过滤，基站名称走后期 Python 模糊匹配
                if real_status:
                    now = datetime.now()
                    start_time = (now - timedelta(hours=12)).strftime("%Y-%m-%d %H:%M:%S")
                    end_time = now.strftime("%Y-%m-%d %H:%M:%S")

                if start_time:
                    where_clauses.append(f"TIME >= '{start_time}'")
                if end_time:
                    where_clauses.append(f"TIME <= '{end_time}'")
                where_clause_sql = " AND ".join(where_clauses) if where_clauses else None

                # 修复：query_jizhan_history 拼接 where 子句需使用 kwarg where_clause，且只能拼接列名，不能拼接中文名称
                result = query_jizhan_history(self.person_base.client, where_clause=where_clause_sql)

                # 基站名称支持模糊匹配，同时对运行状态(run_state)和供电状态(power_state)增加后置过滤
                res_new = {}

                if base_station_name:
                    for station_name, periods in result.items():
                        score = fuzz.partial_ratio(base_station_name, station_name)
                        if score > 60:
                            # 对每条periods里的记录加 run/power 状态过滤
                            filtered_periods = [
                                period for period in periods
                                if match_state(period, run_state, power_state)
                            ]
                            if filtered_periods:
                                res_new[station_name] = filtered_periods
                else:
                    for station_name, periods in result.items():
                        filtered_periods = [
                            period for period in periods
                            if match_state(period, run_state, power_state)
                        ]
                        if filtered_periods:
                            res_new[station_name] = filtered_periods

                if not res_new or len(res_new) == 0:
                    info = f"未查到基站历史状态信息，base_station_name={base_station_name}, where_clause={where_clause_sql}"
                    logger.info(info)
                    return json.dumps({"message": f"{info}"}, ensure_ascii=False)

                json_res = json.dumps(res_new, ensure_ascii=False, separators=(",", ":"))
                logger.info(f"查到基站历史状态信息，返回 {len(res_new)} 个基站记录 返回长度{len(json_res)}", )
                return json_res
            except Exception as e:
                logger.error("traceback-----------------\n%s", traceback.format_exc())
                return json.dumps({"error": "当前查询失败，请尝试其他维度查询"}, ensure_ascii=False)

        @self.mcp.tool()
        def get_infos(
                type: Union[str, list] = "",
                name: str = ""
        ) -> str:
            """
            【基础档案工具】获取矿井基础数据名录（人员、区域、基站）及模糊检索。

            【使用场景】
            用户需要“查找某人的卡号”、“列出所有区域名称”、“查看基站列表”或进行基础数据核对时。

            【参数说明】
            - type: 查询类型，支持单个字符串或字符串列表。合法值: "person" (人员), "area_limit" (区域), "station" (基站)。
            - name: 名称过滤参数 (可选)。
              * 若 type 包含 "person"：支持对“姓名”、“人员卡编码”的**模糊匹配** (相似度>60即返回)。
              * 若 type 包含 "area_limit"：支持对“区域名称”的**模糊匹配**。
              * 若 type 包含 "station"：支持对“基站名称”的**模糊匹配**。
              * 若 name 为空字符串，则返回该类型的全量名录。

            【返回值】
            JSON 字符串。包含查询类型的字典数据及 total_nums (各类别匹配到的总数)。
            示例: {"person": {"张三_1001": {"姓名": "张三", "人员卡编码": "1001", ...}}, "total_nums": {"person": 1}}
            """
            try:

                if not type:
                    return json.dumps({"error": "type参数不能为空"}, ensure_ascii=False)

                if isinstance(type, str):
                    if type.strip() == "":
                        return json.dumps({"error": "type参数不能为空"}, ensure_ascii=False)
                    type_list = [type]
                else:
                    type_list = list(type)

                type_allowed = {"person", "area_limit", "station"}
                result_all = {}
                total_nums = {}
                invalid_types = [t for t in type_list if t not in type_allowed]
                if invalid_types:
                    return json.dumps({"error": f"不支持的type类型: {invalid_types}"}, ensure_ascii=False)

                # 支持多类型循环
                for t in type_list:
                    result_dict = {}
                    if t == "person":
                        persons = query_person_info(self.client)
                        for key, zh_map in persons.items():
                            key_name = key
                            if not name or (
                                    fuzz.partial_ratio(name, zh_map.get('姓名', '')) > 60 or
                                    fuzz.partial_ratio(name, zh_map.get('人员卡编码', '')) > 60 or
                                    fuzz.partial_ratio(name, key_name) > 60
                            ):
                                result_dict[key_name] = zh_map
                    elif t == "area_limit":
                        areas = query_area_info(self.client)
                        for area_code, zh_map in areas.items():
                            area_name = zh_map.get("区域名称", "")
                            if not name or fuzz.partial_ratio(name, area_name) > 60:
                                result_dict[area_code] = zh_map
                    elif t == "station":
                        stations = query_base_station_info(self.client)
                        for station_code, zh_map in stations.items():
                            station_name = zh_map.get("基站名称", "")
                            if not name or fuzz.partial_ratio(name, station_name) > 60:
                                result_dict[station_code] = zh_map

                    # 非法类型已提前拦截
                    result_all[t] = result_dict
                    total_nums[t] = len(result_dict)

                output = result_all
                output["total_nums"] = total_nums

                # 如果只查一个类型，为兼容原有代码，也输出扁平对象:
                if len(type_list) == 1:
                    single_t = type_list[0]
                    output = {single_t: result_all[single_t], "total_nums": {single_t: total_nums[single_t]}}

                res_json = json.dumps(output, ensure_ascii=False, separators=(",", ":"))
                logger.info(
                    f"get_infos 查询成功: type={type}, total_nums={total_nums}, name_filter={name}, 返回字段长度: {len(res_json)}"
                )
                return res_json

            except Exception as e:
                logger.error(f"get_infos({type},{name}) 异常: {e} {traceback.format_exc()}")
                return json.dumps({"error": "当前查询失败，请尝试其他维度查询"}, ensure_ascii=False)


import asyncio
import json


async def test_all_tools():
    print("🔥 开始测试 MinePersonnelService 的所有 MCP Tools\n" + "=" * 60)

    try:
        # print("\n1️⃣ get_system_time")
        # result = await mcp_app.call_tool("get_system_time", {})
        # print(result)

        # INSERT_YOUR_CODE

        print("\n--- 测试 query_station_status ---")

        # # 1. 历史区间筛查
        params_station_history = {
            # "start_date": "2026-07-11 00:00:00",
            # "end_date": "2026-07-18 00:00:00",
            # "person_name_filters": "石",  # 姓名
            # "department_filters": ["综掘队", "车队"],  # 队组班组/部门
            # "worktype_filters": None,  # 工种
            # "duty_filters": "普工",  # 职位
            # "area_filters": "30108",  # 区域筛选
            # "station_filters": "30110",  # 基站筛选
            "today_or_now": False
        }

        result_station_history = await mcp_app.call_tool("query_todayornow_personlist", params_station_history)
        print("query_warning_info | 历史区间】:\n", result_station_history)

        # # 5. 只传入base_station_name, 历史区间
        # params_station_name_history = {
        #     "base_station_name": "井口",
        #     "start_time": "2024-07-01 00:00:00",
        #     "end_time": "2024-08-01 23:59:59",
        #     "real_status": False
        # }
        # result_station_name_history = await mcp_app.call_tool("query_station_status", params_station_name_history)
        # print("【query_station_status | base_station_name=井口, 历史区间】:\n", result_station_name_history)
        # # 2️⃣ 覆盖测试 personnel_list 和人员统计（person_bases.py 1025+ 的场景）
        # print("\n2️⃣ query_cars_list 基础筛选（全部默认参数）")    # person  area_limit    station
        # params2 = {
        #     "type": ["station","area_limit","person"],
        #     "name": "张海"
        # }
        # result = await mcp_app.call_tool("get_infos", params2)
        # pprint(result)

        # # 2️⃣-1 测试基础筛选只查一个类型 station
        # print("\n2️⃣-1 查询 station（基站）")
        # params_station = {"type": ["station"]}
        # result_station = await mcp_app.call_tool("get_infos", params_station)
        # pprint(result_station)

        # # 2️⃣-2 测试带模糊名称筛选 基站
        # print("\n2️⃣-2 查询 station（基站），模糊名称='地面'")
        # params_station_fuzzy = {"type": ["station"], "name": "地面"}
        # result_station_fuzzy = await mcp_app.call_tool("get_infos", params_station_fuzzy)
        # pprint(result_station_fuzzy)

        # # 2️⃣-3 测试基础筛选只查一个类型 area_limit
        # print("\n2️⃣-3 查询 area_limit（区域）")
        # params_area = {"type": ["area_limit"]}
        # result_area = await mcp_app.call_tool("get_infos", params_area)
        # pprint(result_area)


    except Exception as e:
        print(f"\n❌ 测试过程中发生错误: {e}\n{traceback.format_exc()}")
    finally:
        print("\n" + "=" * 60)
        print("🎉 所有 Tool 测试执行完毕！")


# ====================== 执行测试 ======================
if __name__ == "__main__":
    # 注意：请确保 mcp_app 已经在上面被创建
    import json

    mcp_app = FastMCP("MinePersonnelService")

    PersonnelMCPService(
        mcp=mcp_app,
        host="10.11.3.210",
        port=8123,
        database="PS",
        user="default",
        password="xt123456"
    )

    asyncio.run(test_all_tools())
