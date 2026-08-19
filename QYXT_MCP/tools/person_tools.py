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
from base_tool import Base_tool

            
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))))

from sqls.person_sqls import (
    GET_PERSON_LATEST_SQL,
    GET_PERSON_TRAJECTORY_SQL,
    GET_TODAY_PERSONS_SQL,
    GET_REALTIME_STATION_HEAD_INFO_SQL,
    GET_AREA_LIMITS_SQL, GET_TODAY_CARS_SQL
)

from utils.person_utils import (
    get_type_data_from_redis, set_type_data_to_redis, fetch_and_process_car_history,
    PersonBase,CarBase
)

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置日志，日志存储在文件中
from logging.handlers import RotatingFileHandler

LOG_DIR = 'logs'
LOG_FILE = os.path.join(LOG_DIR, 'person_and_car_service.log')

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


class PersonnelMCPService(Base_tool):
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
        self.person_base = PersonBase(self.client,logger)
        self.car_base = CarBase(self.client,logger)
        self.station_names = {}

        self.table = "PS.HISTORY_PERSONNEL_LOCATION"
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

        # pprint(self._fetch_car_realtime_api())
        self.station_names_time = 0
        self.last_query_time = 0
        self._register_resources()
        self._register_prompts()
        self._register_tools()

    
    def fetch_person_realtime_api(self) -> List[Dict]:
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
        
    # ==================== 1. Resource: 静态文档与数据字典 ====================
    def _register_resources(self):
        @self.mcp.resource("docs://personnel/data-dictionary")
        def get_data_dictionary() -> str:
            """
            获取矿井人员定位系统的数据字典和字段说明。
            当模型不确定字段含义（如 CLASSTIMENAME 或 DUTYNAME）时应查阅。
            """
            return """
            # 矿井人员定位系统数据字典
            - NAME: 员工姓名/人员姓名
            - DEPARTMENT: 所属部门/部门（如：综采一队、机电班）
            - AREANAME: 监测区域名称/所在区域名称（如：43204回风顺槽、副井口）
            - ENTERTIME: 进入该区域的具体时刻（ISO 格式）/进入井下时间
            - UPDATE_TIME: 数据同步到数据库的最后时间（用于轨迹排序）
            - CLASSTIMENAME: 班次名称（早班、中班、晚班）
            - DUTYNAME: 职务名称（如：矿长、组长）
            - WORKTYPE: 工种
            - MAINSTATIONTIME: 主站时间
            - MAINSTATIONDISTANCE: 主站距离（米）
            - SUBSTATIONTIME: 分站时间
            - SUBSTATIONDISTANCE: 分站距离（米）
            - AREATIME: 区域停留时间
            - CARD_ID: 车辆唯一编号/卡号，用于区分具体的车辆实体
            - CAR_CODE: 车辆编码/标签，通常为厂商或内部识别号
            - CAR_NAME: 车辆名称，如“9号车”、“运煤车”等
            - DEPARTMENT: 所属部门/车队，例如“综采队”、“运输班”
            - CAR_TYPE_NAME: 车辆类型名称，如“运输车”、“检修车”、“防爆车”等
            - ELECTRICITY: 当前车辆定位卡电量百分比或描述（如“90%”或“低电量”）
            - MAIN_STATION_ID: 主基站ID，车辆信号采集基站的唯一标识
            - MAIN_STATION_TIME: 主基站接收车辆信号的时间
            - MAIN_STATION_DISTANCE: 车辆到主基站的距离（单位：米）
            - SUB_STATION_ID: 辅助基站ID（如有）
            - SUB_STATION_TIME: 辅助基站信号时间
            - SUB_STATION_DISTANCE: 车辆到辅助基站的距离（单位：米）
            - AREA_ID: 区域ID/分区主键，关联车辆所在的业务区域
            - ENTER_TIME: 进入当前区域/巷道的时间
            - ENTRY_TIME: 车辆进场时间/首次出现在系统中的时间
            
            ## 超时定义 入井超时8小时为超时
            

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
            | **入井时间** | DateTime | 最近一次入井时间 | "2026-07-14 08:00:00" |
            | **出井时间** | DateTime | 最近一次出井时间 | "2026-07-14 18:00:00" |
            | **入井时长(秒)** | Int | 累计在井时长(秒) | 18000 |
            | **轨迹开始时间** | DateTime | 轨迹起始点时间 | ["2026-07-14 14:00:00", "2026-07-14 18:00:00"] (配合 between) |
            | **轨迹结束时间** | DateTime | 轨迹结束点时间 | "2026-07-14 15:00:00" |
            | **距离主站距离/m** | Int/Float | 距离主站距离(米) | 150 |
            | **距离分站距离/m** | Int/Float | 距离分站距离(米) | 50 |
            | **变化次数** | Int | 变化次数 | 5 |
            | **停留时长/s** | Int | 停留时长(秒) | [300, 1800] (配合 between) |
            | **电量** | Str | 定位卡电量 | 正常/低电量 |
       

            ## 2. 统计聚合项 (statistics_filters)
            在 `query_personnel_list` 中，通过 `statistics_filters` 指定需要返回的统计维度。若不指定，默认返回精简摘要。

            ### 支持的统计 Key:
            1. **总人数**: 符合条件的人员总数。
            2. **人员列表_姓名_卡号_入井次数**: 详细人员名单及其基础出入井统计。
            3. **入井时长分布/人次**: 按不同时长区间统计的人数分布。
            4. **入井时间段分布/人次**: 按一天中不同时段入井的人数分布。
            5. **出井时间段分布/人次**: 按一天中不同时段出井的人数分布。
            6. **入井地点分布/人次**: 按入井地点统计的人次分布。
            7. **出井地点分布/人次**: 按出井地点统计的人次分布。
            8. **区域分布/条**: 人员在各个区域的分布情况。
            9. **主站分布/条**: 人员关联主站的分布情况。
            10. **分站分布/条**: 人员关联分站的分布情况。
            11. **站点停留时长分布/条**: 在不同站点停留时长的分布。
            12. **部门分布/人**: 按部门/队组统计的人数。
            13. **职位分布/人**: 按职务/职位统计的人数。
            14. **工种分布/人**: 按工种统计的人数。
            15. **班次分布/人**: 按班次统计的人数。
       

            ## 3. 其他基础信息类型 (get_infos)
            - **department**: 部门基础信息（部门名称、部门ID、上级部门ID等）。
            - **person**: 人员基础信息（姓名、卡号、部门、工种等）。
            - **car**: 车辆基础信息（车辆名称、卡号、类型、部门等）。
            - **worktype**: 工种基础信息（工种名称、工种ID等）。
            - **area_limit**: 区域基础信息（区域名称、编码、限制类型等）。
            - **station**: 基站基础信息（基站名称、编码、位置、状态等）。
 
            """

    # ==================== 2. Prompt: 全局行为准则 ====================
    def _register_prompts(self):

        @self.mcp.prompt()
        def analysis_guide() -> str:
            """
            获取矿井人员定位与车辆状态分析的专业操作指南。
            模型在处理用户请求前，必须内化此指南，以确保工具调用的准确性、高效性和专业性。
            """
            return """
            你是一名资深的【矿井安全生产调度与数据分析专家】。你的核心任务是根据用户的自然语言描述，精准拆解需求，并组合调用系统提供的工具，返回结构化、可执行的分析结果。

            请严格遵循以下【核心工作流】与【工具使用规范】：

            ### 🔄 核心工作流 (Workflow)
            1. **时间对齐 (Time Alignment)**：只要用户提及相对时间（如“今天”、“昨天”、“最近2小时”、“本周”），**必须首先**调用 `get_system_time` 获取服务器当前时间，以此推算准确的 `start_time` 和 `end_time` (格式: YYYY-MM-DD HH:MM:SS)。
            2. **意图路由 (Intent Routing)**：根据用户需求，从下方的【工具路由矩阵】中选择最匹配的工具。
            3. **参数构建 (Parameter Construction)**：优先使用结构化过滤条件（如姓名、区域、部门、工种等），避免在获取全量数据后用 Python 逻辑二次过滤。
            4. **结果解释 (Result Interpretation)**：对返回的 JSON 数据进行专业解读。若数据量过大被系统压缩，应主动建议用户增加筛选条件（如指定部门、区域或缩短时间范围）。

            ---

            ### 🛠️ 工具路由矩阵与使用规范

            #### 1. `get_system_time` (时间基准)
            - **何时使用**：任何涉及时间范围的查询前。
            - **注意**：无参数。返回结果将作为后续所有工具时间参数的计算基准。

            #### 2. `query_person_underground_status` (核心：今日/当前井下人员分布)
            - **何时使用**：查询实时井下总人数、各区域分布及今日出入井情况。
            - **关键参数技巧**：
              - **now_only=True**：返回当前实时井下人员分布。
              - **now_only=False**：返回今日全量井下记录（含出入、排班等统计）。

            #### 3. `query_person_trajectory` (人员轨迹与分段)
            - **何时使用**：查询某个人在一天或多个时间段的详细活动轨迹（进出井、轨迹分段、停留区域）。
            - **关键参数**：`name`, `start_time`, `end_time`。若轨迹结果较多，系统将自动精简，仅保留核心区段及时间范围。
            - **注意**：确定时间范围前，建议先调用 `get_system_time`。

            #### 4. `query_personnel_list` (核心：多条件人员综合查询与统计)
            - **何时使用**：按姓名、区域、工种、班次、部门、时间区间等多字段灵活组合筛选人员，获取完整信息与出入明细。
            - **关键参数技巧**：
              - 支持 `names`, `areas`, `work_types`, `class_names`, `departments`, `start_time`, `end_time` 等组合过滤。
              - **高级数值/时间过滤 (`numeric_filters`)**：当用户提出“时长超过”、“距离大于”、“时间在区间内”时，**必须**使用此参数。
                *示例*：查询“入井时长超过5小时且停留时长在5~30分钟”
                ```json
                {
                  "入井时长(秒)": {"op": ">", "value": 18000},
                  "停留时长/s": {"op": "between", "value": [300, 1800]}
                }
                ```
              - **统计聚合 (`statistics_filters`)**：当用户询问“分布”、“汇总”、“多少人”时，传入统计 Key 列表（如 `["总人数", "区域分布/条", "部门分布/人"]`），系统将直接返回聚合结果，避免返回海量明细导致超时。

            #### 5. `find_person_latest_entry` (最新入井记录)
            - **何时使用**：了解特定人员最新一次入井时间、班次、工种等基础信息。
            - **关键参数**：`name`（人员姓名）。

            #### 6. `get_data_dictionary` (数据字典/字段说明)
            - **何时使用**：对任何数据字段（如 CLASSTIMENAME、DUTYNAME、AREANAME 等）含义有疑惑时查阅说明。
            - **注意**：无参数或按需传入字段名。

            #### 7. `get_infos` (基础档案与名录字典)
            - **何时使用**：用户需要“查找某人的卡号”、“列出所有区域”、“查看基站/车辆/工种列表”或进行基础数据核对。
            - **关键参数**：`type` 必须是 "department", "person", "car", "worktype", "area_limit", 或 "station"（支持列表）。`name` 参数支持对姓名、区域名、基站名等的**模糊匹配**。

            #### 8. `query_car_underground_status` (井下车辆分布)
            - **何时使用**：查询矿井实时或今日内的车辆井下分布情况。
            - **关键参数技巧**：
              - **now_only=True**：获取实时在井下的车辆及其分布。
              - **now_only=False**：获取今日所有进出井下车辆的统计，包括区域明细。

            #### 9. `query_car_trajectory` (车辆轨迹)
            - **何时使用**：分析某车辆在一天内或特定时段的移动轨迹、分段详情。
            - **关键参数**：`cardID`, `start_time`, `end_time`。可追踪车辆进出井、行驶路线、区域变化等时空轨迹。

            #### 10. `query_cars_list` (批量车辆条件筛选)
            - **何时使用**：按车辆ID、车辆名称、车辆类型、部门、区域、定位卡电量等多维组合，批量查询车辆属性、进出井及出入明细。
            - **关键参数**：`cardids`, `car_names`, `car_types`, `departments`, `area_names`, `electricitys`, `start_time`, `end_time`。适用于车辆大盘分析与场景性筛查。

            ---

            ### ⚠️ 异常处理与兜底策略
            1. **无数据**：若工具返回空或 "未查到..."，请明确告知用户“在指定条件下未找到相关记录”，并**主动提供缩小范围的建议**（例如：“是否扩大时间范围？”或“请确认姓名/区域名称是否准确？”）。
            2. **参数错误**：若工具返回参数校验错误，请检查时间格式是否为 `YYYY-MM-DD HH:MM:SS`，或 `numeric_filters` 的 `op` 是否为合法操作符 (`>`, `>=`, `<`, `<=`, `==`, `!=`, `between`, `in`)。
            3. **数据截断**：若返回结果包含 "由于数据体量过大..." 的 message，请向用户解释原因，并引导其使用 `statistics_filters` 或增加部门/区域等筛选条件进行下钻分析。
            4. **默认时间**：若用户未说明时间区间，默认查询当日（或实时）数据。

            请保持回答专业、客观、条理清晰。优先输出核心结论，再附带详细数据支撑。
            """

    def _register_tools(self):

        @self.mcp.tool()
        def get_system_time() -> str:
            """
            功能描述: 获取服务器当前的系统时间。由于历史查询和轨迹查询依赖于准确的时间范围，该工具可作为大模型计算“昨天”、“上周”或“三小时前”等相对时间的基准参考。
            输入参数: 无。
            返回描述: 包含当前日期时间（格式：YYYY-MM-DD HH:MM:SS）及星期的 JSON 字符串。例如: {
                "current_time": "2024-06-05 17:23:42",
                "weekday": "Wednesday"
            }
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
        def query_person_underground_status(
                now_only: bool = False,
                info_needs: bool = False,
        ) -> str:
            """
            查询今日、当前实时井下总人数及各区域人员分布统计。

            功能说明：
            - 获取当前实时井下矿下人员数据,（通过now_only=True参数控制）
            - 获取今日矿下人员数据,包含人员基本信息、职务、工种、所在区域、进入时间等关键字段（通过now_only=False参数控制）
            - 如果info_needs为True，则返回人员基本信息，如果为False，则返回人员名称,默认返回人员名称。

            参数:
            now_only: 返回实时人员数据或者今日矿下人员数据。
            info_needs: 是否需要返回人员基本信息。如果为True，则返回人员基本信息，如果为False，则返回人员名称,默认返回人员名称。
            人员基本信息: {"姓名_卡号": <职位,工种,班次名,部门,区域名称>, ...},,
               
            返回格式: 
            - 返回json字符串
            
            """
            logger.info(f"query_person_underground_status called, now_only={now_only}")

            try:
                # 1. 获取实时井下人员
                real_time_list = self._fetch_person_realtime_api()

                # ==================== 实时模式 (now_only=True) ====================
                names = {}
                area_stats = {}
                self._init_station_names(force=False)
                person_names = []
                persons_info = {}
                for p in real_time_list:
                    name = p.get("name")
                    person_names.append(name)
                    cardid = p.get('cardID')
                    namenew = name + '_' + cardid
                    if not namenew:
                        continue
                    work_type = p.get("workType") or "未知"
                    class_time = p.get("classTimeName") or ""
                    department = p.get("department") or ""
                    main_area = p.get("areaName") or ""
                    dyty_name = p.get('dutyName') or "未知"
                    area_id = p.get("mainStationID", "未知区域")
                    try:
                        area = self.station_names.get(int(area_id), "未知区域").get("name", main_area)
                    except:
                        self._init_station_names(force=True)
                        area = self.station_names.get(int(area_id), area_id)

                    # 按注释要求格式：工种,班次名,部门
                    info = f"{dyty_name};{work_type};{class_time};{department};{area}".strip(",")
                    persons_info[namenew] = {"基本信息": info,"实时位置": None,"持续时间/s": 0,"进入时间": None,"离开时间": None}
                    
                    area_stats[area] = area_stats.get(area, 0) + 1

                print(persons_info)
                person_records = self.get_persons_by_filters(
                    names=person_names,
                )
                persons = person_records['persons']
                # 只保留需要的字段
                # 新keep_fields不再包含 "job"
                keep_fields = [
                    'mainStationId',
                    'duration',
                ]
                for person_key, info in persons.items():
                    print(person_key)
                    # 只保留每个人 records 最后一条（最新一条）记录的信息
                    if person_key in person_records and isinstance(info['records'], list) and len(info['records']) > 0:
                        latest_rec = info['records'][-1]
                        # job 字段保留
                        job_val = latest_rec.get('job', None)
                        info['job'] = job_val
                        # 只保留最新一条的当前位置和持续时间
                        curr_location = latest_rec.get('mainStationId', None)
                        duration = latest_rec.get('duration', None)
                        persons_info[person_key]['实时位置'] = curr_location
                        persons_info[person_key]['持续时间/s'] = duration
                        # 也可保留时间方便追踪 
                        persons_info[person_key]['进入时间'] = latest_rec.get('segmentStartTime', None)
                        persons_info[person_key]['离开时间'] = latest_rec.get('segmentEndTime', None)
                        # 清理 records 字段，防止混淆
                        info.pop('records', None)
                        
                print(persons_info)
                if now_only:
                    if info_needs:
                        json_str = json.dumps(
                            {
                                "实时井下人员数量": len(real_time_list),
                                "实时井下人员轨迹记录": persons_info,
                                "现有位置分布": dict(
                                    sorted(
                                        area_stats.items(), key=lambda x: x[1], reverse=True
                                    )
                                ),
                                "请求时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            },
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                    else:
                        json_str = json.dumps(
                            {
                                "实时井下人员数量": len(real_time_list),
                                "实时井下人员名称": person_names,
                                "现有位置分布": dict(
                                    sorted(
                                        area_stats.items(), key=lambda x: x[1], reverse=True
                                    )
                                ),
                                "请求时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            },
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                    logger.info(
                        f"query_person_underground_status(now_only=True) 查询成功："
                        f"实时井下人员数量={len(real_time_list)}, "
                        f"轨迹记录人数={len(person_records.get('persons', {})) if isinstance(person_records, dict) else '未知'}, "
                        f"区域分布数={len(area_stats)}, "
                        f"返回数据长度={len(json_str)}"
                    )

                    return json_str

                # ==================== 今日完整模式 (now_only=False) ====================
                underground_set = {
                    item.get("name") + '_' + item.get("cardID") for item in real_time_list if item.get("name")
                }

                today_persons = self.get_today_persons()

                # 2. 已出井人员
                out_names = {}
                out_person_names = []
                for person in today_persons:
                    out_person_names.append(person[0])
                    name = person[0] + '_' + person[1]
                    if name in underground_set:
                        continue
                    work_type = person[3]
                    class_time = person[4]
                    department = person[2]
                    status_text = "已出井"
                    name = person[0] + '_' + person[1]
                    info = f"{work_type};{class_time};{department};{status_text}".strip(",")
                    out_names[name] = info

                out_person_records = self.get_persons_by_filters(
                    names=out_person_names,
                )
                out_persons = out_person_records['persons']
                keep_fields = [
                    # 'segmentStartTime',
                    # 'segmentEndTime',
                    # 'electricity',
                    'mainStationId',
                    'duration',
                ]
                for person_key, info in out_persons.items():
                    # 将 job 字段单独存储在外部
                    if 'records' in info and isinstance(info['records'], list):
                        # 取第一个 record 的 job 值, 若不存在则为 None
                        job_val = None
                        for rec in info['records']:
                            if 'job' in rec:
                                job_val = rec['job']
                                break
                        # 赋予 info["job"] 字段
                        info['job'] = job_val
                        new_records = []
                        for rec in info['records']:
                            kept_values = [rec.get(k, None) for k in keep_fields]
                            new_records.append(kept_values)
                        # 替换为描述key和数据key的结构
                        # info['record_fields'] = ["主站ID", "持续时间/s"]
                        # info['record_values'] = new_records
                        # 移除原始 records 字段，防止混淆
                        info.pop('records', None)

                # 统计
                total_underground = len(underground_set)
                total_out_today = len(out_names)

                json_res = json.dumps(
                    {
                        "total_underground": total_underground,
                        "total_out_today": total_out_today,
                        "total_today": len(today_persons),
                        "井下人员": persons,
                        "出井人员": out_persons,
                        "area_distribution": dict(
                            sorted(
                                area_stats.items(), key=lambda x: x[1], reverse=True
                            )
                        ),

                        "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    },
                    ensure_ascii=False,
                    separators=(",", ":"),
                )

                logger.info(
                    f"query_underground_status 查询成功: "
                    f"total_underground={total_underground}, "
                    f"total_out_today={total_out_today}, "
                    f"total_today={len(today_persons)}, "
                    f"返回字段长度={len(json_res)}"
                )

                return json_res

            except Exception as e:
                logger.error(f"query_underground_status 异常: {traceback.format_exc()}")
                return json.dumps(
                    {"error": "查询失败", "message": traceback.format_exc()}, ensure_ascii=False,
                    separators=(",", ":"),
                )

        @self.mcp.tool()
        def query_person_trajectory(
                name: Optional[Union[str, int]] = None,
                start_time: Optional[str] = None,
                end_time: Optional[str] = None
                
        ) -> str:
            """
            查询某个人（或符合条件的人员）在指定时间段内的轨迹数据，如果start_time 和 end_time 都不传，则默认查询当天的轨迹记录。
            如果需要确定系统start_time 和 end_time 请先调用 get_system_time 函数

            功能说明：
            - 支持按姓名进行过滤
            - 支持指定时间范围（start_time ~ end_time）
            - 如果不传 start_time 和 end_time，则默认查询最新记录。

            参数:
                name: 姓名 (如 "陈玉岭") 这个字段必须有
                start_time: 开始时间，格式 "YYYY-MM-DD HH:MM:SS" 或 "YYYY-MM-DD"
                end_time:   结束时间，格式 "YYYY-MM-DD HH:MM:SS" 或 "YYYY-MM-DD"

            返回格式: 列如
                失败返回
                {
                        "message": f"未找到姓名 '{name}' 在指定时间范围内的记录"
                    }
                成功返回示例如下
                {
                    'name': '石天龙_0777',    # 人员姓名_卡号
                    'start': '2026-04-01 08:00:00',  # 查询起始时间
                    'end': '2026-04-02 18:00:00',    # 查询结束时间
                    'department': '领导干部',         # 部门
                    'workType': '生产矿长',           # 工种
                    'job': '矿领导',                 # 岗位/职位
                    'total_segments': 8,             # 分段数量
                    "leave": "出矿"/"矿下",                  # 是否已经出矿
                    'segments': [  # 每个分段（入井-出井一次）的明细
                        {
                            "inTime": "2026-04-23T12:32:40",       # 入井时间
                            "outTime": "2026-04-23T15:17:42",      # 出井时间
                            "inPlace": "副井口2号",                 # 入井地点
                            "outPlace": "副井口2号",                # 出井地点
                            "duration": "2时45分2秒",               # 在井下总时长（汉字格式，也可补充秒整数）
                            "segments_count": 19,                   # 此入井段内，小轨迹分段数
                            "segments": [  # 该入井段的所有分轨迹段明细
                                {
                                    "classTimeName": "早班",            # 班次名称
                                    "mainStationId": "5煤一连巷18号",   # 主站点ID或名称
                                    "segmentStartTime": "2026-04-23 12:35:31",       # 分段开始时间
                                    "segmentEndTime": "2026-04-23 12:36:35",         # 分段结束时间
                                    "segmentDurationSeconds": 64,    # 分段持续秒数
                                    "areaChanges": ["66.1"],         # 该分段中的距离变化明细
                                    "recordCount": 2                 # 该分段的数据点数量
                                },
                                {...}  # 其他分轨迹段，以同样结构表示
                            ]
                        },
                        {...} # 其他入井-出井大段，以同样结构表示
                    ]

                }
            """
            # INSERT_YOUR_CODE
            logger.info(f"query_person_trajectory called: name={name}, start_time={start_time}, end_time={end_time}")
            if name is not None:
                name = str(name)
           
            try:
                # INSERT_YOUR_CODE
                if name:
                    name = name.replace(' ', '')

                if not name:
                    # INSERT_YOUR_CODE
                    logger.warning("query_person_trajectory: name 参数为空")

                    return json.dumps({"error": "必须提供 name 参数"}, ensure_ascii=False)

                if not start_time or not end_time:

                    today = datetime.now().date()
                    if not start_time:
                        start_time = f"{today} 00:00:00"
                    if not end_time:
                        end_time = f"{today} 23:59:59"

                real_time_list = self._fetch_person_realtime_api()
                # ==================== 实时模式 (now_only=True) ====================
                names = {}
                self._init_station_names(force=False)
                for p in real_time_list:
                    name_new = p.get("name")
                    cardid = p.get('cardID')
                    if not name_new or not cardid:
                        continue
                    key = f"{name_new}_{cardid}"
                    work_type = p.get("workType") or "未知"
                    class_time = p.get("classTimeName") or ""
                    department = p.get("department") or ""
                    main_area = p.get("areaName") or ""
                    area_id = p.get("mainStationID", "未知区域")
                    try:
                        area = self.station_names.get(int(area_id), "未知区域").get("name", main_area)
                    except:
                        self._init_station_names(force=True)
                        area = self.station_names.get(int(area_id), area_id)
                    # 按注释要求格式：工种,班次名,部门
                    info = f"{work_type};{class_time};{department};{area}".strip(",")
                    names[key] = info

                # 支持多卡、同名不同卡处理（让 get_person_trajectory_with_stay 返回全部卡，需遍历cards）
                person_records = self.get_person_trajectory_with_stay(
                    name=name,
                    start_time=start_time,
                    end_time=end_time,
                )

                if not person_records or not person_records.get("cards"):
                    logger.info(f"未找到姓名 '{name}' 在指定时间范围({start_time} 至 {end_time})内的轨迹记录")
                    return json.dumps({
                        "message": f"未找到姓名 '{name}' 在指定时间范围内的记录"
                    }, ensure_ascii=False)

                inout_records = self.fetch_in_out_mine_records(start_time, end_time, '')
                # 将出入井记录聚合为 key = 姓名_卡号
                inout_recordsnew = {}
                for record in inout_records:
                    try:
                        uname = record.get('UserName', '')
                        uno = record.get('UserNo', 'unknown')
                        key = f"{uname}_{uno}"
                    except Exception:
                        continue
                    if key:
                        inout_recordsnew.setdefault(key, []).append(record)

                # 遍历每个card，补充分段和离井状态
                cards = person_records.get("cards", [])
                for card in cards:
                    cardid = card.get('cardId') or card.get('cardID')
                    # key为姓名_卡号
                    card_name = f"{name}_{cardid}"
                    inout_records_person = inout_recordsnew.get(card_name)
                    if inout_records_person is None and len(inout_recordsnew) > 1:
                        # fuzzy match, 取最大的score的best_match
                        try:
                            all_matches = process.extract(card_name, list(inout_recordsnew.keys()))
                            if all_matches:
                                best_match, max_score = max(all_matches, key=lambda x: x[1])
                            else:
                                best_match, max_score = None, 0
                        except Exception:
                            best_match, max_score = None, 0
                        if max_score is not None and max_score >= 40:
                            inout_records_person = inout_recordsnew[best_match]
                        else:
                            inout_records_person = []
                    segments = card.get('segments', [])
                    resnew = self.classify_segments_by_inout(segments, inout_records_person)
                    card["segments"] = resnew
                    card["total_segments"] = len(resnew)
                    card["in_or_out"] = "矿下" if card_name in names else "出矿"
           

                # 返回结构统一
                output = {
                    "name": person_records.get("name"),
                    "total_cards": person_records.get("total_cards"),
                    "cards": cards
                }

                json_str = json.dumps(output, ensure_ascii=False, separators=(",", ":"))
                len_json_str = len(json_str)
                if len_json_str > 80000:
                    # 返回精简内容
                    simple_cards = []
                    for card in cards:
                        simple_segments = []
                        for seg_s in card.get("segments", []):
                            prev_seg = {
                                "inTime": seg_s.get("inTime"),
                                "outTime": seg_s.get("outTime"),
                                "inPlace": seg_s.get("inPlace"),
                                "outPlace": seg_s.get("outPlace"),
                                "duration": seg_s.get("duration"),
                                "segments": []
                            }
                            segment_list = []
                            for seg in seg_s.get("segments", []):
                                station = seg.get("mainStationId", " ")
                                seg_start = seg.get("segmentStartTime")
                                seg_end = seg.get("segmentEndTime")
                                classname = seg.get('classTimeName')
                                segment_item = [classname, station, seg_start, seg_end]
                                segment_list.append(segment_item)
                            prev_seg["segments"] = segment_list
                            prev_seg["segments_desc"] = ['班次', '井下位置', '开始时间', '结束时间']
                            prev_seg["segments_count"] = len(segment_list)
                            simple_segments.append(prev_seg)
                        slim_card = {
                            "cardId": card.get("cardId") or card.get("cardID"),
                            "name": card.get("name"),
                            "department": card.get("department"),
                            "workType": card.get("workType"),
                            "job": card.get("job"),
                            "total_segments": card.get("total_segments"),
                            "in_or_out": card.get("in_or_out"),
                            "segments": simple_segments
                        }
                        simple_cards.append(slim_card)
                    slim_output = {
                        "name": person_records.get("name"),
                        "total_cards": len(simple_cards),
                        "cards": simple_cards,
                        "desc": '由于数据过多,精简回答,需要详细信息请给出更精准的限制信息'
                    }
                    str_json = json.dumps(slim_output, ensure_ascii=False, separators=(",", ":"))
                    logger.info(
                        f"find_status 查询成功: 返回精简JSON(多卡/同名支持), 字段长度: {len(str_json)}"
                    )
                    return str_json

                detailed_json = json.dumps(output, ensure_ascii=False, separators=(",", ":"))
                logger.info(
                    f"find_status 查询成功: 返回详细JSON(多卡/同名支持), 字段长度: {len(detailed_json)}"
                )
                return detailed_json


            except Exception as e:
                logger.error(f"find_status 异常: {e} {traceback.format_exc()}", exc_info=True)
                return json.dumps(
                    {"error": "查询失败", "message": str(e)},
                    ensure_ascii=False
                )

        @self.mcp.tool()
        def query_personnel_list(
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
            start_date: Union[str, datetime, None] = None,
            end_date: Union[str, datetime, None] = None,
            time_now: bool = False,
        ) -> str:
            """
            功能说明: 多条件综合查询人员名单。支持根据主站区域、分站区域、班次、工种、部门及指定时间段进行灵活组合筛选，常见应用如查询“指定区域在某时段的所有人员”或“今日某班次出勤人员名单”。
            如果需要确定系统start_time 和 end_time 请先调用 get_system_time 函数

            参数说明:
                start_date (string, 可选): 开始时间，格式为 "YYYY-MM-DD HH:MM:SS"，如未传则为当天的0点。
                end_date (string, 可选): 结束时间，格式为 "YYYY-MM-DD HH:MM:SS"，如未传则则为当天的24点。
                person_name_filters (list, 可选): 人员姓名筛选，如 ["张三", "李四"], 支持模糊匹配。
                department_filters (list, 可选): 部门名称筛选。
                classtype_filters (list, 可选): 班次筛选，如 ["早班", "中班","夜班"]。
                worktype_filters (list, 可选): 工种名称列表筛选。
                duty_filters (list, 可选): 职务名称筛选。
                electricity_filters (list, 可选): 定位卡电量状态筛选，仅支持 ["正常", "低电量"]。
                station_filters (list, 可选): 站点(主/分站)名称模糊匹配列表。
                area_filters (list, 可选): 区域名称筛选。
                in_places_filters (list, 可选): 入井位置名称筛选。
                out_places_filters (list, 可选): 出井位置名称筛选。
                numeric_filters (dict, 可选): 数值型筛选，比如区间等。
                statistics_filter (dict, 可选): 统计类高级筛选参数。
            - numeric_filters: 高级数值/时间过滤字典。格式: {"字段名": {"op": "操作符", "value": 值}}。
              支持字段名: "入井时间", "出井时间", "入井时长(秒)", "轨迹开始时间", "轨迹结束时间", "距离主站距离/m", "距离分站距离/m", "变化次数", "停留时长/s",电量。
              支持操作符: ">", ">=", "<", "<=", "==", "!=", "between", "not_between", "in" 等。
              
              示例: {"距离主站距离/m": {"op": "<=", "value": 150}, "入井时间": {"op": "between", "value": ["2026-07-01 04:00:00", "2026-07-01 08:00:00"]}
            - statistics_filter: 统计聚合项列表。若提供，系统将优先返回聚合统计而非海量明细。如不指定或为["all"]，则返回全部统计字段。
              可选值: "总人数", "人员列表_姓名_卡号_入井次数", "入井时长分布/人次", "入井时间段分布/人次", "出井时间段分布/人次", "入井地点分布/人次", "出井地点分布/人次", "区域分布/条", "主站分布/条", "分站分布/条", "站点停留时长分布/条", "部门分布/人", "职位分布/人", "工种分布/人", "班次分布/人"。
            - 如果筛选时 提示了当前、现在等时间  time_now 为True ，其余情况为False。
            返回值:
       

            返回一个包含所有符合条件的、去重的人员详细信息字典，其中包括姓名、部门、工种、班次及每人的详细记录。
            若压缩后数据仍然超过指定大小，将只返回如下三个字段：
            {
                "persons_inout_count": { ... },                  # 各人员的出入井/区域次数信息
                "_summary_area_persons_per_day": { ... },        # 全局区域人员数量（日维度统计）
                "_summary_person_unique_area_per_day": { ... }   # 各人员每日唯一出入区域次数（日维度统计）
            }
            """

            logger.info(
                f"query_personnel_list called with params: start_date={start_date}, end_date={end_date}, "
                f"person_name_filters={person_name_filters}, department_filters={department_filters}, "
                f"classtype_filters={classtype_filters}, worktype_filters={worktype_filters}, duty_filters={duty_filters}, "
                f"electricity_filters={electricity_filters}, station_filters={station_filters}, area_filters={area_filters}, "
                f"in_places_filters={in_places_filters}, out_places_filters={out_places_filters}, numeric_filters={numeric_filters}, "
                f"statistics_filter={statistics_filter}"
            )

            try:
                logger.info("Step 1: 适配名字")
                names = person_name_filters
                if names is not None:
                    if isinstance(names, str):
                        names = [names.strip()]
                        logger.info(f"Step 1.1: person_name_filters is str, converted to list: {names}")
                    elif isinstance(names, list):
                        names = [n.strip() for n in names if isinstance(n, str)]
                        logger.info(f"Step 1.2: person_name_filters is list, stripped values: {names}")
                    else:
                        names = None
                        logger.info("Step 1.3: person_name_filters is not str/list, set to None")

                logger.info("Step 2: 适配部门")
                departments = department_filters if department_filters is None or isinstance(department_filters, list) else [department_filters]
                logger.info(f"departments after adapt: {departments}")

                logger.info("Step 3: 适配班次")
                class_names = classtype_filters if classtype_filters is None or isinstance(classtype_filters, list) else [classtype_filters]
                logger.info(f"class_names after adapt: {class_names}")

                logger.info("Step 4: 适配工种")
                work_types = worktype_filters if worktype_filters is None or isinstance(worktype_filters, list) else [worktype_filters]
                logger.info(f"work_types after adapt: {work_types}")

                logger.info("Step 5: 适配 duty/职务")
                duties = duty_filters if duty_filters is None or isinstance(duty_filters, list) else [duty_filters]
                logger.info(f"duties after adapt: {duties}")

                logger.info("Step 6: 适配定位卡电量状态")
                electricitys = electricity_filters if electricity_filters is None or isinstance(electricity_filters, list) else [electricity_filters]
                logger.info(f"electricitys after adapt: {electricitys}")

                logger.info("Step 7: 适配站点")
                main_stations = []
                sub_stations = []
                if station_filters is not None:
                    if isinstance(station_filters, list):
                        main_stations = station_filters
                        logger.info(f"Step 7.1: station_filters as list: {main_stations}")
                    else:
                        main_stations = [station_filters]
                        logger.info(f"Step 7.2: station_filters not list, wrapped: {main_stations}")
                else:
                    logger.info("Step 7.3: station_filters is None")

                logger.info("Step 8: 适配区域")
                areas = area_filters if area_filters is None or isinstance(area_filters, list) else [area_filters]
                logger.info(f"areas after adapt: {areas}")

                logger.info("Step 9: 适配入井、出井位置")
                in_places = in_places_filters if in_places_filters is None or isinstance(in_places_filters, list) else [in_places_filters]
                out_places = out_places_filters if out_places_filters is None or isinstance(out_places_filters, list) else [out_places_filters]
                logger.info(f"in_places after adapt: {in_places}")
                logger.info(f"out_places after adapt: {out_places}")

                logger.info("Step 10: 适配起止时间")
                start_time = start_date
                end_time = end_date
                if not start_time or not end_time:
                    today = datetime.now().date()
                    if not start_time:
                        start_time = f"{today} 00:00:00"
                        logger.info(f"start_time not provided, set to today: {start_time}")
                    if not end_time:
                        end_time = f"{today} 23:59:59"
                        logger.info(f"end_time not provided, set to today: {end_time}")
                logger.info(f"Final time window: start_time={start_time}, end_time={end_time}")

                if numeric_filters is None:
                    numeric_filters = {}
                    
                if time_now:
                  
                    numeric_filters["轨迹结束时间"]={
                        "op": "==",
                        "value": ''
                    },
                    logger.info(f"添加numeric_filters {numeric_filters} 成功")
                    
                logger.info("Step 11: 查询底层数据")
                person_records = self.person_base.get_person_infos_daytype_with_cache(
                    person_name_filters=names,
                    area_filters=areas,
                    electricity_filters=electricitys,
                    worktype_filters=work_types,
                    classtype_filters=class_names,
                    department_filters=departments,
                    start_date=start_time,
                    end_date=end_time,
                    station_filters=main_stations,
                    # sub_stations is not directly mapped, only station_filters is defined in person_bases.py
                    in_places_filters=in_places,
                    out_places_filters=out_places,
                    duty_filters=duties,
                    numeric_filters=numeric_filters,
                )
                logger.info("person_base.get_person_infos_daytype_with_cache 已调用")

                if person_records:
                    with open("history_person_data.txt", "w", encoding="utf-8") as f:
                        f.write(json.dumps(person_records, ensure_ascii=False, indent=2))
                    print("全部数据已成功写入 history_person_data.txt")
                    
                if not person_records:
                    logger.info("query_personnel_list 未找到人员记录，适配参数：%s", json.dumps({
                        "names": names,
                        "departments": departments,
                        "areas": areas,
                        "electricitys": electricitys,
                        "work_types": work_types,
                        "class_names": class_names,
                        "start_time": start_time,
                        "end_time": end_time,
                        "main_stations": main_stations,
                        "sub_stations": sub_stations,
                        "in_places": in_places,
                        "out_places": out_places,
                        "duties": duties
                    }, ensure_ascii=False))
                    logger.info("Step 12: 未找到人员记录，返回 message")
                    return json.dumps({"message": "未找到符合条件的人员记录"}, ensure_ascii=False)

                logger.info("Step 13: 对查询到的数据进行序列化")
                json_full = json.dumps(person_records, ensure_ascii=False, separators=(",", ":"))
                logger.info(f"已经序列化查询结果, 序列化长度: {len(json_full)}")

                statistics_filter_values = [
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
                len_json_full = len(json_full)
                logger.info(f"Step 14: json_full长度: {len_json_full}")

                statistics_filter_passed = statistics_filter is not None and len(statistics_filter) > 0
                logger.info(f"Step 15: statistics_filter_passed={statistics_filter_passed}")

                if statistics_filter_passed:
                    logger.info("Step 16: statistics_filter 不为空，进行精准统计项过滤抽取")
                    day_datas = person_records.get('每日数据')
                    filtered_record = {'每日数据': {}}
                    key_length_info = {}
                    oversize_keys = []
                    for key, day_data in day_datas.items():
                        logger.info(f"Step 16.1: 处理 {key} 的单日数据")
                        record = self.person_base.person_filter(
                            pdata=day_data,
                            statistics_filter=statistics_filter
                        )
                        filtered_stats = {}
                        messages = []
                        for stat_key in statistics_filter:
                            value = record['statistics'].get(stat_key)
                            single_stat_dict = {stat_key: value}
                            stat_json = json.dumps(single_stat_dict, ensure_ascii=False, separators=(",", ":"))
                            stat_len = len(stat_json)
                            key_length_info[(key, stat_key)] = stat_len
                            if stat_len > 10000:
                                message_warn = f"'{stat_key}' 统计项在 {key} 的数据体量过大，请进一步缩小查询范围或细化分布参数."
                                messages.append(message_warn)
                                logger.warning(f"Step 16.2: {message_warn}")
                            filtered_stats[stat_key] = value
                        filtered_record['每日数据'][key] = {"statistics": filtered_stats}
                        if messages:
                            filtered_record['每日数据'][key]['message'] = "; ".join(messages)
                            logger.info(f"Step 16.3: 超大统计项产生 message: {'; '.join(messages)}")
                    json_full = json.dumps(filtered_record, ensure_ascii=False, separators=(",", ":"))
                    len_json_full = len(json_full)
                    logger.info(f"Step 16.4: statistics_filter抽取后json长度: {len_json_full}")

                else:
                    if len_json_full > 30000:
                        logger.info("Step 17: json_full超过30k，对每日数据做第一次筛选压缩")
                        new_outs_record = {'每日数据': []}
                        day_datas = person_records.get('每日数据')
                        day_record = {}
                        for key, day_data in day_datas.items():
                            logger.info(f"Step 17.1: 进行person_filter筛选 key={key}")
                            record = self.person_base.person_filter(
                                pdata=day_data,
                                statistics_filter=statistics_filter_values
                            )
                            day_record[key] = [record]
                        new_outs_record['每日数据'] = day_record
                        json_full = json.dumps(new_outs_record, ensure_ascii=False, separators=(",", ":"))
                        len_json_full = len(json_full)
                        logger.info(f"Step 17.2: 首轮压缩后json长度: {len_json_full}")

                        if len_json_full > 30000:
                            logger.info("Step 18: 首轮压缩仍超30k，仅保留总人数与入井次数")
                            all_days_agg = {}
                            day_datas = person_records.get('每日数据')
                            for k in ["总人数", "人员列表_姓名_卡号_入井次数"]:
                                all_days_agg[k] = {}
                            for day, day_data in day_datas.items():
                                logger.info(f"Step 18.1: 处理 day={day} 的核心统计")
                                summary = self.person_base.person_filter(
                                    pdata=day_data,
                                    statistics_filter=["总人数", "人员列表_姓名_卡号_入井次数"]
                                )
                                for k in ["总人数", "人员列表_姓名_卡号_入井次数"]:
                                    if k in summary["statistics"]:
                                        all_days_agg[k][day] = summary["statistics"][k]
                                        logger.info(f"Step 18.2: 保存 {k} 统计 day={day}")
                            result_json = {
                                "出入井总人数": all_days_agg.get("总人数", {}),
                                "人员列表_姓名_卡号_入井次数": all_days_agg.get("人员列表_姓名_卡号_入井次数", {}),
                                "message": (
                                    f"由于数据体量过大，仅返回总人数与人员卡号入井汇总。如需其他分布/统计，请指定分布参数（如 ['入井时间段分布']），支持的分布有{statistics_filter_values}"
                                    "或进一步缩小查询范围（如指定姓名、部门、时间段等）以获取更详细内容。"
                                )
                            }
                            json_full = json.dumps(result_json, ensure_ascii=False, separators=(",", ":"))
                            len_json_full = len(json_full)
                            logger.info(f"Step 18.3: 最精简核心统计后json长度: {len_json_full}")

                logger.info(
                    f"压缩人员 records, 当前json长度: len_json_full={len(json_full)}"
                )

                logger.info("Step 19: 函数执行完成, 返回json_full")
                return json_full

            except Exception as e:
                logger.error("traceback: %s", traceback.format_exc())
                logger.error(f"Step ERROR: 查询失败: {str(e)}")
                return json.dumps({
                    "error": "查询失败",
                    "message": str(e),
                    "traceback": traceback.format_exc()
                }, ensure_ascii=False)


        @self.mcp.tool()
        def find_person_latest_entry(
                name: Optional[Union[str, int]] = None,
                cardid: Optional[Union[str, int]] = None,
        ) -> str:
            """
            查找指定人员最近一次入井记录（即最新的进入井下的时间和相关信息）。

            功能说明：
            - 仅支持按姓名精确检索（入参必须传 name）
            - 返回该人员最近一次入井的基础信息，包括入井时间、部门、工种、班次、所在区域等

            参数:
                name: 姓名（如 "陈玉岭"），必填
                cardid: 工号（可选，用于唯一标识人员；如 "6285"）

            返回格式: 若找到
                {
                    "name": "陈玉岭",
                    "cardId": "6285",
                    "enterTime": "2024-06-06 09:12:13",
                    "areaName": "主井口",
                    "department": "机电队",
                    "workType": "维修工",
                    "classTimeName": "早班",
                    "other": {...}
                }
            若未找到该人
                {
                    "message": "未找到姓名 'xxx' 的最近入井记录"
                }
            """
            # INSERT_YOUR_CODE
            logger.info(f"find_person_latest_entry called: name={name}, cardid={cardid}")

            if name is not None:
                name = str(name)
            if cardid is not None:
                cardid = str(cardid)
            
            try:
                if not name:
                    return json.dumps({"error": "参数 name 不能为空"}, ensure_ascii=False)

                result = self.get_person_latest(name, cardid)
                # get_person_latest 要么返回 {"success": False, ...}，要么返回 {"success": True, ...}
                if not result or not result.get("success", False):
                    return json.dumps({"message": f"未找到姓名 '{name}' 的最近入井记录"}, ensure_ascii=False)
                r = result

                ret = {
                    "name": r.get("name", name),
                    "cardId": r.get("cardId", name),
                    "enterTime": r.get("enterTime", ""),
                    "areaName": r.get("areaName", ""),
                    "department": r.get("department", ""),
                    "workType": r.get("workType", ""),
                    "classTimeName": r.get("classTimeName", ""),
                    "other": {k: v for k, v in r.items() if k not in (
                        "name", "enterTime", "areaName", "department", "workType", "classTimeName", "success"
                    )}
                }

                res_json = json.dumps(ret, ensure_ascii=False, separators=(",", ":"))
                logger.info(
                    f"find_person_latest_entry 查询成功: "
                    f"返回字段长度: {len(res_json)}"
                )
                return res_json

            except Exception as e:
                logger.error("traceback-----------------\n%s", traceback.format_exc())

                return json.dumps({
                    "error": "查询失败",
                    "message": str(e),
                    "traceback": traceback.format_exc()
                }, ensure_ascii=False)

        @self.mcp.tool()
        def query_car_underground_status(
                now_only: bool = False,
                info_needs: bool = False,
        ) -> str:
            """
            查询今日、实时井下车辆及各区域车辆分布统计。

            功能说明：
            - 获取实时井下矿下车辆数据,（通过now_only=True参数控制）
            - 获取今日矿下车辆数据,包含车辆基本信息、部门、所在区域、入井时间等关键字段（通过now_only=False参数控制）
            - 如果info_needs为True，则返回车辆基本信息，如果为False，则返回车辆名称,默认返回车辆名称。

            参数:
            now_only: 返回实时车辆数据或者今日矿下车辆数据。
            info_needs: 是否需要返回车辆基本信息。如果为True，则返回车辆基本信息，如果为False，则返回车辆名称,默认返回车辆名称。
            车辆基本信息：
                {
                    "车牌ID": car.get("车牌ID", ""),
                    "部门": car.get("部门", ""),
                    "车辆类型": car.get("车辆类型", ""),
                    "定位卡电量": car.get("定位卡电量", ""),
                    "主站名称": car.get("主站名称", ""),
                    "主站时间": car.get("主站时间", ""),
                    "主站距离(米)": car.get("主站距离(米)", ""),
                    "入井时间": car.get("入井时间", ""),
                }

            返回格式: 返回json字符串
            """
            # INSERT_YOUR_CODE
            logger.info(f"query_car_underground_status called, now_only={now_only}")

            try:
                # === 实时模式 ===
                # API返回的每个car:
                # {'areaID', 'carCode', 'carName', 'carTypeID', 'carTypeName', 'cardId', 'department',
                #  'electricity', 'enterTime', 'mainStationDistance', 'mainStationID', 'mainStationTime', ... }
                car_realtime_list = self._fetch_car_realtime_api()  # [{...}, ...]

                cars = {}
                area_stats = {}
                electricity_stats = {}
                cars_names = []
                for car in car_realtime_list:
                    CARNAME = car.get("carName", "") or car.get("carCode") or car.get("cardId") or ""
                    if not CARNAME:
                        continue
                    # 填充标准化字段
                    cars_names.append(CARNAME)
                    cars[CARNAME] = {
                        "车牌ID": car.get("carCode", ""),  # CARCODE
                        "部门": car.get("department", ""),
                        "车辆类型": car.get("carTypeName", ""),
                        "定位卡电量": car.get("electricity", ""),
                        "主站名称": car.get("mainStationID", ""),
                        "主站时间": car.get("mainStationTime", ""),
                        "主站距离(米)": car.get("mainStationDistance", ""),
                        "入井时间": car.get("enterTime", "")

                    }
                    # 实时区域分布
                    area = car.get("mainStationID", "未知区域")
                    area_stats[area] = area_stats.get(area, 0) + 1
                    electricity = car.get("electricity", "")
                    electricity_stats[electricity] = electricity_stats.get(electricity, 0) + 1

                if now_only:
                    if info_needs:
                        return json.dumps(
                            {
                                "当前井下车辆总数": len(cars),
                                "当前定位卡电量分布": electricity_stats,
                                "当前车辆区域分布": dict(
                                    sorted(area_stats.items(), key=lambda x: x[1], reverse=True)
                                ),
                                "车辆信息": cars,
                                "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            },
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )
                    else:
                        return json.dumps(
                            {
                                "当前井下车辆总数": len(cars),
                                "当前车辆名称": cars_names,
                                "当前定位卡电量分布": electricity_stats,
                                "当前车辆区域分布": dict(
                                    sorted(area_stats.items(), key=lambda x: x[1], reverse=True)
                                ),
                                "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            },
                            ensure_ascii=False,
                            separators=(",", ":"),
                        )

                # === 今日完整模式 ===
                # underground_set: 实时在矿的车辆编号集合
                underground_set = set(cars.keys())
                query = GET_TODAY_CARS_SQL

                
                today_cars = {
                    row[1]: {
                        "车牌ID": row[0],
                        "部门": row[2],
                        "车辆类型": row[3],
                        "定位卡电量": row[4],
                        "主站名称": self.station_names.get(row[5], {}).get("name", row[5]),
                        "主站时间": row[6],
                        "主站距离(米)": row[7],
                        "入井时间": row[8],

                    }

                    for row in self.client.query(query).result_rows
                }
  
                out_cars = [c for c in today_cars if c not in underground_set]
                # 查询已出井车辆最新的一条数据

                total_underground = len(underground_set)
                total_out_today = len(out_cars)
                cars_names_status = {}
                cars_names_status["出矿"] = []
                cars_names_status["矿下"] = []
                # 填入已出井车辆
                for car_code, car in today_cars.items():
                    # 只需填核心字段, 保持格式一致
                    status = "出矿" if car_code in out_cars else "矿下"
                    cars[car_code] = {
                        "车牌ID": car.get("车牌ID", ""),
                        "部门": car.get("部门", ""),
                        "车辆类型": car.get("车辆类型", ""),
                        "定位卡电量": car.get("定位卡电量", ""),
                        "主站名称": car.get("主站名称", ""),
                        "主站时间": car.get("主站时间", ""),
                        "主站距离(米)": car.get("主站距离(米)", ""),
                        "入井时间": car.get("入井时间", ""),
                        "状态": status
                    }
                    cars_names_status[status].append(car_code)
                    
                    
                # 统计区域：只统计实时在矿车辆的区域分布即可
                if info_needs:
                    json_res = json.dumps(
                        {
                            "矿下车辆总数": total_underground,
                            "今日出井车辆数": total_out_today,
                            "今日总车辆数": len(today_cars),
                            "车辆信息": cars,
                            "当前车辆区域分布": dict(
                                sorted(area_stats.items(), key=lambda x: x[1], reverse=True)
                            ),
                            "当前车辆定位卡电量分布": electricity_stats,
                            "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                else:
                    json_res = json.dumps(
                        {
                            "矿下车辆总数": total_underground,
                            "今日出井车辆数": total_out_today,
                            "今日总车辆数": len(today_cars),
                            "今日出入井车辆信息": cars_names_status,
                        },
                        ensure_ascii=False,
                        separators=(",", ":"),
                    )
                    
                logger.info(
                    f"query_car_underground_status 查询成功: "
                    f"矿下车辆总数={total_underground}, "
                    f"今日出井车辆数={total_out_today}, "
                    f"今日总车辆数={len(today_cars)}, "
                    f"返回字段长度: {len(json_res)}"
                )
                return json_res

            except Exception as e:
                logger.error(f"query_car_underground_status 异常: {e}", exc_info=True)
                return json.dumps(
                    {"error": "查询失败", "message": str(e)}, ensure_ascii=False
                )

        @self.mcp.tool()
        def query_car_trajectory(
                cardName: Optional[Union[str, int]] = None,
                cardID: Optional[Union[str, int]] = None,
                start_time: Optional[str] = None,
                end_time: Optional[str] = None,
        ) -> str:
            """
            查询指定车辆在给定某日的出入记录轨迹。
            功能说明:
                - 查询某辆车（通过车辆名称）在指定时段内的轨迹详细信息。
                - 返回结构与人员轨迹类似，包括进出时间、区域、停留记录、基础车辆属性等。
            参数:
                cardName (str): 车辆名称, "9号自行车(瓦检)"。
                cardID (str, 可选): 车辆编号，如 "1234"。
                CardID 和 CardName 二选一，优先使用 CardID。
                start_time/end_time (str, 可选): 查询时间段，格式 "YYYY-MM-DD HH:MM:SS"，默认当天。
            返回格式:
                # 返回示例（车辆轨迹，包含分段停留详情和入口出口等信息）：
                {
                    "车辆名称": "9号自行车(瓦检)",            # 车辆名称（如9号自行车(瓦检)）
                    "车辆编号": "99",                         # 车辆编号
                    "所属部门": "机运队",                 # 所属部门
                    "query_date": "2026-04-28 14:29:26",    # 查询时间（生成报告的时间/接口返回时间）
                    "total_segments": 38,                   # 总分段数（主分段数量）
                    "segments": [
                        {
                            "inTime": "2026-04-17T07:11:56",      # 本次入井时间（格式：YYYY-MM-DDTHH:MM:SS）
                            "outTime": "2026-04-27T15:11:18",     # 本次出井时间（如无则为空或null）
                            "inPlace": "副井口69号",              # 入井地点或区域
                            "outPlace": "副井口2号",              # 出井地点或区域
                            "duration": "247时59分22秒",           # 累计在井时长（可用中文描述，时分秒格式）
                            "segments_count": 38,                  # 本次分段总数
                            "segments": [
                                {
                                    "area": "4-3煤盘区主运大巷10号",
                                    "segmentStartTime": "2026-04-23 00:54:05",     # 分段起始时间
                                    "segmentEndTime": "2026-04-23 00:54:05"         # 分段结束时间
                                },
                                {
                                    "area": "43203回风900米临时避难硐室",
                                    # 下面可继续...
                                }
                                # ...
                            ]
                        }
                        # 可以有多个出入井区间，按需追加
                    ]
                }
                查询失败时返回:
                {
                    "error": "查询失败",
                    "message": "...原因..."
                }
            """
            # INSERT_YOUR_CODE
            logger.info(
                f"query_car_trajectory called with params: cardName={cardName}, cardID={cardID}, start_time={start_time}, end_time={end_time}"
            )
            # INSERT_YOUR_CODE
            # 输入不为None时都需转为str
            if cardName is not None:
                cardName = str(cardName)
            if cardID is not None:
                cardID = str(cardID)
            if start_time is not None:
                start_time = str(start_time)
            if end_time is not None:
                end_time = str(end_time)
   
            cardID = str(cardID) if cardID is not None else None
            try:
                if not (cardName or cardID):
                    return json.dumps({"error": "参数 cardName 或 cardID 不能为空"}, ensure_ascii=False)

                # 构造默认时间
                today_str = datetime.now().strftime("%Y-%m-%d")
                if not start_time:
                    start_time = f"{today_str} 00:00:00"
                if not end_time:
                    end_time = f"{today_str} 23:59:59"
                cardID = cardID or ""
                if not cardID:
                    data = get_type_data_from_redis('car')
                    # 模糊查询车辆名称
                    max_score = 0
                    if data:
                        # 假设 data 是 [{}, {}] 的形式（即每项是 dict，包含 'name', 'id' 或类似字段）
                        exact_match = {}
                        for item in data:
                            if item.get("carName") == cardName:
                                exact_match = item

                        if exact_match:
                            cardID = exact_match.get("cardID") or ""
                        else:
                            for item in data:
                                name = item.get("carName", "")
                                score = fuzz.partial_ratio(cardName, name)
                                if score >= max_score:
                                    max_score = score
                                    cardID = item.get("cardID", "")

                            if max_score < 60:
                                cardID = ""  # 未找到有效的模糊匹配

                if not cardID:
                    # INSERT_YOUR_CODE
                    logger.info(
                        f"未找到车辆名称 '{cardName}' 的编号, cardID 未获取到, 输入参数: cardName={cardName}, start_time={start_time}, end_time={end_time}")

                    return json.dumps(
                        {"error": f"未找到车辆名称 '{cardName}' 的编号"}, ensure_ascii=False
                    )

                data, _ = fetch_and_process_car_history(
                    card_id=cardID, begin_time=start_time, end_time=end_time
                )  # 如果fetch_and_process_car_history没有返回2项，改为解包唯一返回值

                if not data or "carInfo" not in data or not data.get("segments"):
                    return json.dumps(
                        {"error": f"未找到车辆编号 '{cardID}' 的轨迹记录"}, ensure_ascii=False
                    )

                car_info = data.get("carInfo") or {}
                segments = data.get("segments") or []
                self._init_station_names()
                # 格式组装
                res_segments = []
                for seg in segments:
                    # 组合成带日期的时间（因原始为['HH:MM:SS', ...]，需拼到查询日）

                    start = f"{seg['S_E_Time'][0]}"
                    end = f"{seg['S_E_Time'][1]}"
                    area_id = seg.get("area", "未知区域")
                    area_name = self.station_names.get(int(area_id), {}).get("name", area_id)
                    res_segments.append(
                        {
                            "area": area_name,
                            "segmentStartTime": start,
                            "segmentEndTime": end,
                        }
                    )
                inout_records = self.fetch_in_out_mine_records(start_time, end_time, '')
                inout_recordsnew = {}
                for record in inout_records:
                    key = record.get('UserName')
                    if key is not None:
                        inout_recordsnew.setdefault(key, []).append(record)

                carName = car_info.get("carName")

                inout_records_person = inout_recordsnew.get(carName, [])
                resnew = self.classify_segments_by_inout(res_segments, inout_records_person)

                out = {
                    "车辆名称": car_info.get("carName", cardID),
                    "车辆编号(不是车牌号)": car_info.get("cardId") or cardID,
                    "所属部门": car_info.get("department", ""),
                    "query_date": car_info.get("time_now", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                    "total_segments": car_info.get("total_count", len(segments)),
                    "segments": resnew,
                }
                out_res = json.dumps(out, ensure_ascii=False, separators=(",", ":"))
                # INSERT_YOUR_CODE
                logger.info(
                    f"query_car_trajectory 查询成功: 车辆编号={cardID}, 车辆名称={car_info.get('carName')}, "
                    f"total_segments={out.get('total_segments')}, 返回字段长度: {len(out_res)}"
                )

                return out_res
            except Exception as e:
                logger.error(f"query_car_trajectory 异常: {e},{traceback.format_exc()}")
                return json.dumps(
                    {
                        "error": "查询失败",
                        "message": str(e),
                        "traceback": traceback.format_exc()
                    },
                    ensure_ascii=False
                )

        @self.mcp.tool()
        def query_cars_list(
            car_names_filters: Union[List[str], str, None] = None,  # 车辆名称过滤
            car_types_filters: Union[List[str], str, None] = None,  # 车辆类型过滤
            electricitys_filters: Union[List[str], str, None] = None,  # 定位卡电量过滤：['正常', '低电量']
            departments_filters: Union[List[str], str, None] = None,  # 部门过滤
            station_filters: Union[List[str], str, None] = None,  # 主站/分站过滤
            area_names_filters: Union[List[str], str, None] = None,  # 区域名称过滤
            in_places_filters: Union[List[str], str, None] = None,  # 入井地点过滤
            out_places_filters: Union[List[str], str, None] = None,  # 出井地点过滤
            # 数值过滤
            numeric_filters: Optional[Dict[str, Dict]] = None,
            statistics_filter: Union[List[str], str, None] = None,
            start_date: Union[str, 'datetime', None] = None,
            end_date: Union[str, 'datetime', None] = None,
            time_now: bool = False,
            
        ) -> str:
            """
            功能说明: 多条件综合查询车辆名单。支持根据车辆编号、车辆名称、车辆类型、部门、主站/分站区域、班次、职务、指定时间段等进行灵活组合筛选，常见应用如查询“指定区域在某时段的所有车辆”或“今日某班次出勤车辆名单”。
            如果需要确定系统start_time 和 end_time 请先调用 get_system_time 函数

            参数说明:
                car_names_filters (list, 可选): 车辆名称筛选。
                car_types_filters (list, 可选): 车辆类型筛选。
                departments_filters (list, 可选): 部门名称筛选。
                classtype_filters (list, 可选): 班次筛选，如 ["早班", "中班","夜班"]。
                electricitys_filters (list, 可选): 定位卡电量状态筛选，仅支持 ["正常", "低电量"]。
                station_filters (list, 可选): 站点(主/分站)名称模糊匹配列表。
                area_names_filters (list, 可选): 区域名称筛选。
                in_places_filters (list, 可选): 入井/入场位置名称筛选。
                out_places_filters (list, 可选): 出井/出场位置名称筛选。
                numeric_filters (dict, 可选): 数值型筛选，比如区间等。
                statistics_filter (list, 可选): 统计类高级筛选参数。
                start_date (string, 可选): 开始时间，格式为 "YYYY-MM-DD HH:MM:SS"，如未传则由内部决定默认值。
                end_date (string, 可选): 结束时间，格式为 "YYYY-MM-DD HH:MM:SS"，如未传则由内部决定默认值。
            - numeric_filters: 高级数值/时间过滤字典。格式: {"字段名": {"op": "操作符", "value": 值}}。
              支持字段名: "入井时间", "出井时间", "入井时长", "入井时长(秒)", "轨迹开始时间", "轨迹结束时间", "距离主站距离/m", "距离分站距离/m", "变化次数", "停留时长/s"。
              支持操作符: ">", ">=", "<", "<=", "==", "!=", "between", "not_between", "in"。
              
              示例: {"距离主站距离/m": {"op": "<=", "value": 150}, "停留时长/s": {"op": "between", "value": [300, 1800]}}
            - statistics_filter: 统计聚合项列表。若提供，系统将优先返回聚合统计而非海量明细。如不指定或为["all"]，则返回全部统计字段。
              可选值: "总车辆数", "车辆总览", "车辆列表_名称_编号_出入井次数", "出入井时长分布/辆次", "入井时间段分布/辆次", "出井时间段分布/辆次", "入井地点分布/辆次", "出井地点分布/辆次", "区域分布/条", "主站分布/条", "分站分布/条", "站点停留时长分布/条", "所属部门分布/辆", "车辆类型分布/辆"。

            - 如果筛选时 提示了当前、现在等时间  time_now 为True ，其余情况为False。

            返回值:

            返回一个包含所有符合条件的、去重的车辆详细信息字典，其中包括车辆编号、名称、类型、部门、班次及每辆车的详细记录。
            
            """

            logger.info(
                f"query_cars_list called with params: car_names_filters={car_names_filters}, car_types_filters={car_types_filters}, "
                f"electricitys_filters={electricitys_filters}, departments_filters={departments_filters}, "
                f"station_filters={station_filters}, area_names_filters={area_names_filters}, "
                f"in_places_filters={in_places_filters}, out_places_filters={out_places_filters}, "
                f"numeric_filters={numeric_filters}, statistics_filter={statistics_filter}, "
                f"start_date={start_date}, end_date={end_date}"
            )

            try:

                car_names = car_names_filters
                if car_names is not None:
                    if isinstance(car_names, str):
                        car_names = [car_names.strip()]
                        logger.info(f"car_names_filters is str, converted to list: {car_names}")
                    elif isinstance(car_names, list):
                        car_names = [c.strip() for c in car_names if isinstance(c, str)]
                        logger.info(f"car_names_filters is list, stripped: {car_names}")
                    else:
                        car_names = None

                car_types = car_types_filters if car_types_filters is None or isinstance(car_types_filters, list) else [car_types_filters]
                logger.info(f"car_types_filters after adapt: {car_types}")

                departments = departments_filters if departments_filters is None or isinstance(departments_filters, list) else [departments_filters]
                logger.info(f"departments_filters after adapt: {departments}")

                # area_names
                area_names = area_names_filters if area_names_filters is None or isinstance(area_names_filters, list) else [area_names_filters]
                logger.info(f"area_names_filters after adapt: {area_names}")

                # Step: 适配定位卡电量状态
                electricitys = electricitys_filters if electricitys_filters is None or isinstance(electricitys_filters, list) else [electricitys_filters]
                logger.info(f"electricitys_filters after adapt: {electricitys}")

                # Step: 适配站点
                main_stations = station_filters if station_filters is None or isinstance(station_filters, list) else [station_filters]
                logger.info(f"station_filters after adapt: {main_stations}")

                # 适配入/出场位置
                in_places = in_places_filters if in_places_filters is None or isinstance(in_places_filters, list) else [in_places_filters]
                out_places = out_places_filters if out_places_filters is None or isinstance(out_places_filters, list) else [out_places_filters]
                logger.info(f"in_places_filters after adapt: {in_places}")
                logger.info(f"out_places_filters after adapt: {out_places}")

                start_time = start_date
                end_time = end_date
                if not start_time or not end_time:
                    today = datetime.now().date()
                    if not start_time:
                        start_time = f"{today} 00:00:00"
                        logger.info(f"start_time not provided, set to today: {start_time}")
                    if not end_time:
                        end_time = f"{today} 23:59:59"
                        logger.info(f"end_time not provided, set to today: {end_time}")
                logger.info(f"Final time window: start_time={start_time}, end_time={end_time}")

                if numeric_filters is None:
                    numeric_filters = {}
                    
                if time_now:
                  
                    numeric_filters["轨迹结束时间"]={
                        "op": "==",
                        "value": ''
                    },
                    logger.info(f"添加numeric_filters {numeric_filters} 成功")

                # Step 11: 查询底层数据（修改为car_base对应方法）
                car_records = self.car_base.get_cars_infos_daytype_with_cache(
                    car_names_filters=car_names,
                    car_types_filters=car_types,
                    departments_filters=departments,
                    electricitys_filters=electricitys,
                    area_names_filters=area_names,
                    start_date=start_time,
                    end_date=end_time,
                    station_filters=main_stations,
                    in_places_filters=in_places,
                    out_places_filters=out_places,
                    numeric_filters=numeric_filters,
                )
                logger.info("car_base.get_car_infos_daytype_with_cache 已调用")

                if car_records:
                    with open("history_car_data.txt", "w", encoding="utf-8") as f:
                        f.write(json.dumps(car_records, ensure_ascii=False, indent=2))
                    print("全部数据已成功写入 history_car_data.txt")
                    
                if not car_records:
                    logger.info("未找到车辆记录，适配参数：%s", json.dumps({
        
                        "car_names": car_names,
                        "car_types": car_types,
                        "departments": departments,
                        "area_names": area_names,
                        "start_time": start_time,
                        "end_time": end_time,
                        "main_stations": main_stations,
                        "in_places": in_places,
                        "out_places": out_places,
                        "electricitys": electricitys
                    }, ensure_ascii=False))
                    logger.info("未找到车辆记录，返回 message")
                    return json.dumps({"message": "未找到符合条件的车辆记录"}, ensure_ascii=False)

                logger.info("对查询到的数据进行序列化")
                json_full = json.dumps(car_records, ensure_ascii=False, separators=(",", ":"))
                logger.info(f"已经序列化查询结果, 序列化长度: {len(json_full)}")

                statistics_filter_values = [
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
                len_json_full = len(json_full)
                logger.info(f"json_full长度: {len_json_full}")

                statistics_filter_passed = statistics_filter is not None and len(statistics_filter) > 0
                logger.info(f"statistics_filter_passed={statistics_filter_passed}")

                if statistics_filter_passed:
                    logger.info("statistics_filter 不为空，进行精准统计项过滤抽取")
                    day_datas = car_records.get('每日数据')
                    filtered_record = {'每日数据': {}}
                    key_length_info = {}
                    for key, day_data in day_datas.items():
                        logger.info(f"处理 {key} 的单日数据")
                        record = self.car_base.car_filter(
                            pdata=day_data,
                            statistics_filter=statistics_filter
                        )
                        filtered_stats = {}
                        messages = []
                        for stat_key in statistics_filter:
                            value = record['statistics'].get(stat_key)
                            single_stat_dict = {stat_key: value}
                            stat_json = json.dumps(single_stat_dict, ensure_ascii=False, separators=(",", ":"))
                            stat_len = len(stat_json)
                            key_length_info[(key, stat_key)] = stat_len
                            if stat_len > 10000:
                                message_warn = f"'{stat_key}' 统计项在 {key} 的数据体量过大，请进一步缩小查询范围或细化分布参数."
                                messages.append(message_warn)
                                logger.warning(message_warn)
                            filtered_stats[stat_key] = value
                        filtered_record['每日数据'][key] = {"statistics": filtered_stats}
                        if messages:
                            filtered_record['每日数据'][key]['message'] = "; ".join(messages)
                            logger.info(f"超大统计项产生 message: {'; '.join(messages)}")
                    json_full = json.dumps(filtered_record, ensure_ascii=False, separators=(",", ":"))
                    len_json_full = len(json_full)
                    logger.info(f"statistics_filter抽取后json长度: {len_json_full}")

                else:
                    if len_json_full > 30000:
                        logger.info("json_full超过30k，对每日数据做首次筛选压缩")
                        new_outs_record = {'每日数据': {}}
                        day_datas = car_records.get('每日数据')
                        for key, day_data in day_datas.items():
                            logger.info(f"进行car_filter筛选 key={key}")
                            record = self.car_base.car_filter(
                                pdata=day_data,
                                statistics_filter=statistics_filter_values
                            )
                            new_outs_record['每日数据'][key] = [record]
                        json_full = json.dumps(new_outs_record, ensure_ascii=False, separators=(",", ":"))
                        len_json_full = len(json_full)
                        logger.info(f"首轮压缩后json长度: {len_json_full}")

                        if len_json_full > 30000:
                            logger.info("首轮压缩仍超30k，仅保留总数与核心车辆统计")
                            all_days_agg = {}
                            day_datas = car_records.get('每日数据')
                            for k in ["总车辆数","车辆总览", "车辆列表_名称_编号_出入井次数"]:
                                all_days_agg[k] = {}
                            for day, day_data in day_datas.items():
                                logger.info(f"处理 day={day} 的核心统计")
                                summary = self.car_base.car_filter(
                                    pdata=day_data,
                                    statistics_filter=["总车辆数","车辆总览", "车辆列表_名称_编号_出入井次数"]
                                )
                                for k in ["总车辆数","车辆总览", "车辆列表_名称_编号_出入井次数"]:
                                    if k in summary["statistics"]:
                                        all_days_agg[k][day] = summary["statistics"][k]
                                        logger.info(f"保存 {k} 统计 day={day}")
                            result_json = {
                                "出入场总数": all_days_agg.get("总车辆数", {}),
                                # "车辆总览": all_days_agg.get("车辆总览", {}),
                                "车辆列表_名称_编号_出入井次数": all_days_agg.get("车辆列表_名称_编号_出入井次数", {}),
                                "message": (
                                    f"由于数据体量过大，仅返回总数与车辆进出汇总。如需其他分布/统计，请指定分布参数（如 ['进场时间段分布']），支持的分布有{statistics_filter_values}"
                                    "或进一步缩小查询范围（如指定车辆编号、名称、部门、时间段等）以获取更详细内容。"
                                )
                            }
                            json_full = json.dumps(result_json, ensure_ascii=False, separators=(",", ":"))
                            len_json_full = len(json_full)
                            logger.info(f"最精简核心统计后json长度: {len_json_full}")

                logger.info(
                    f"压缩车辆 records, 当前json长度: len_json_full={len(json_full)}"
                )

                logger.info("函数执行完成, 返回json_full")
                return json_full

            except Exception as e:
                logger.error("traceback: %s", traceback.format_exc())
                logger.error(f"查询失败: {str(e)}")
                return json.dumps({
                    "error": "查询失败",
                    "message": str(e),
                    "traceback": traceback.format_exc()
                }, ensure_ascii=False)



        @self.mcp.tool()
        def query_person_near_station(
                station_name: str,
                near_distance: Union[int, float] = 100
        ) -> str:
            """
            功能说明: 根据点位名称和距离（米，默认100）查询该点位附近的实时人员。

            参数:
                station_name: 站点名称，可为模糊名称
                near_distance: 距离（米），默认为100米

            返回说明:
                {
                    "matches": [<stationHeadID列表>],
                    "data": [
                        {
                            "CodeSenderAddress": ...,
                            "EmpName": ...,
                            "DeptName": ...,
                            "DutyName": ...,
                            "WorkTypeName": ...,
                            "CheckTime": ...,
                            "StationHeadID": ...,
                            "StationHeadPlace": ...,
                            "LastDistanceDescribe": ...,
                            "LastDirection": ...
                        }, ...
                    ]
                }
                若查询失败或未找到站点，则返回错误信息
            """
            # INSERT_YOUR_CODE
            logger.info(f"query_person_near_station called: station_name={station_name}, near_distance={near_distance}")

            try:
                self._init_station_names()
                # 模糊匹配station_name（忽略大小写）
                matches = []
                names = []
                for sid, sdict in self.station_names.items():
                    s_n = sdict.get('name', "")
                    if s_n and fuzz.partial_ratio(station_name, s_n) > 50:
                        names.append(s_n)
                        matches.append(str(sid))

                if not matches:
                    return json.dumps({
                        "error": f"未匹配到相关站点: {station_name}"
                    }, ensure_ascii=False)

                collected = []
                for sid in matches:
                    payload = {"stationHeadid": sid, "nearDistance": str(near_distance)}
                    headers = {"Content-Type": "application/json"}
                    try:
                        response = requests.post(
                            self.near_loaction_api_url,
                            json=payload,
                            headers=headers,
                            verify=False,
                            timeout=30
                        )
                        text = response.content.decode("utf-8-sig")
                        data = json.loads(text).get("data", []) if response.status_code == 200 else []
                        if data:
                            collected.extend(data)

                    except Exception as ee:
                        logger.warning(f"request error for stationHeadid {sid}: {ee} {traceback.format_exc()}")

                res_json = json.dumps({
                    "matches_ids": matches,
                    "matches_names": names,
                    "data": "未找到该点位附近人员" if not collected else collected
                }, ensure_ascii=False, separators=(",", ":"))

                logger.info(
                    f"query_person_near_station 查询成功: "
                    f"匹配站点ID={matches}, "
                    f"匹配站点名称={names}, "
                    f"返回数据人数: {len(collected)}, "
                    f"字段长度: {len(res_json)}"
                )
                return res_json


            except Exception as e:
                logger.error(f"query_person_near_station 异常: {e} {traceback.format_exc()}")

                return json.dumps(
                    {
                        "error": "查询失败",
                        "message": str(e),
                        "traceback": traceback.format_exc()
                    },
                    ensure_ascii=False
                )

        @self.mcp.tool()
        def get_infos(
                type: str = "",  # "department", "person", "car", "worktype", "area_limit", "station"
                name: str = ""
        ) -> str:
            """
            功能说明:
            该接口用于通过传入 type 参数（可选：department 部门、person 人员、car 车辆、worktype 工种、area_limit 区域限制、station 区域点位）和可选的 name，查询不同类型的信息汇总。常用于需要获取人员、部门、车辆、工种、区域限制等下拉列表、枚举或详情场景。

            参数:
                type: 查询类型，支持以下选项之一：
                    - "department" 部门信息列表
                    - "person" 人员信息列表
                    - "car" 车辆信息列表
                    - "worktype" 工种类别列表
                    - "area_limit" 区域限制信息
                    - "station" 区域点位（站点）信息
                name: 过滤名称（可选，仅部分 type 支持，支持模糊或精确名称过滤，提高检索精准度）

            返回格式:
                针对不同 type 返回对应的数据结构，格式如下：

                - 若 type 为 department:
                    返回包含所有部门的字典，key为部门名，value为部门属性字典。例如:
                    {
                        "综掘队": {
                            "deptID": "-1958297282",
                            "parentDeptID": "106796614"
                        },
                        "机电队": {
                            "deptID": "123456789",
                            "parentDeptID": "987654321"
                        },...
                    }

                - 若 type 为 person:
                    返回包含所有人员的字典，key为人员名，value为人员属性字典。例如:
                    {
                        "师伟伟": {
                            "cardID": "6153",
                            "workTypeID": "-144112766",
                            "workType": "班组长",
                            "deptID": "-1958297282",
                            "department": "综掘队",
                            "classID": "19",
                            "className": "三班",
                            "dutyID": "9335947",
                            "dutyName": "掘进班组长",
                            "maxWorkTime": "36000",
                            "minWorkTime": "0",
                            "typeID": "",
                            "otherInfo": "",
                            "role": "0",
                            "inWhiteList": false
                        },...
                    }

                - 若 type 为 car:
                    返回包含所有车辆的字典，key为车辆名，value为车辆属性字典。例如:
                    {
                        "SYG060防爆水泥罐车": {
                            "cardID": "1058",
                            "carNO": "1058",
                            "carTypeID": "1",
                            "carTypeName": "车辆",
                            "otherInfo": "",
                            "deptID": "-1208015531",
                            "department": "通防部",
                            "driverCardID": ""
                        },...
                    }

                - 若 type 为 worktype:
                    返回包含所有工种的字典，key为工种名，value为工种属性字典。例如:
                    {
                        "水泵工": {
                            "workerTypeID": "-2141367907",
                            "desc": "",
                            "otherInfo": ""
                        },...
                    }

                - 若 type 为 area_limit:
                    返回包含所有区域的字典，key为区域名，value为区域属性字典。例如:
                    {
                        "43203回风掘面": {
                            "areaID": "-403214344",
                            "areaTypeID": "318791515",
                            "areaType": "重点区域",
                            "areaLimit": 16,
                            "location": "",
                            "parentAreaID": "332397867",
                            "areaDesc": ""
                        },...
                    }

                - 若 type 为 station:
                    返回站点信息字典，key为站点名，value为站点属性字典。例如:
                    {
                        "5-1煤辅运二段650米处1号": {
                            "STATIONHEADID": 11,
                            "STATIONHEADTYPE": "井下接收器"
                        },...
                    }

                - 如果类型未实现或参数错误，则返回错误信息:
                    {
                        "error": "不支持的type类型"
                    }

                - 如果类型未实现或参数错误，则返回错误信息:
                        {
                            "error": "不支持的type类型"
                        }
            """
            # INSERT_YOUR_CODE
            logger.info(f"query_type_dict called, type={type}")

            try:

                cache_types = {
                    "department": "department",
                    "person": "person",
                    "car": "car",
                    "worktype": "worktype",
                    "area_limit": "area_limit",
                }

                data = None
                # 只对前5种类型使用redis缓存
                if type in cache_types and time.time() - self.last_query_time < 3600:
                    self.last_query_time = time.time()
                    data = get_type_data_from_redis(cache_types[type])

                # 命中缓存data只生成names，否则请求API并写回redis
                if type == "department":
                    if data is None:
                        # 请求API
                        payload = ""
                        headers = {
                            'Content-Type': 'application/json',
                            'Accept': '*/*',
                        }
                        resp = requests.post(self.department_api_url, data=payload, headers=headers, timeout=30,
                                             verify=False)
                        text = resp.content.decode("utf-8-sig")
                        arr = json.loads(text).get("data", []) if resp.status_code == 200 else []
                        set_type_data_to_redis("department", arr)
                        data = arr
                    names = {
                        x.get("deptName"): {
                            "deptID": x.get("deptID", ""),
                            "parentDeptID": x.get("parentDeptID", "")
                        }
                        for x in data if x.get("deptName")
                    }

                elif type == "person":
                    if data is None:
                        payload = json.dumps({
                            "mineCode": "",
                            "deptID": "",
                            "nameOrID": ""
                        })
                        headers = {
                            'Content-Type': 'application/json',
                            'Accept': '*/*',
                        }
                        resp = requests.post(self.person_info_api_url, headers=headers, data=payload, timeout=30,
                                             verify=False)
                        text = resp.content.decode("utf-8-sig")
                        persons = json.loads(text).get("data", []) if resp.status_code == 200 else []
                        set_type_data_to_redis("person", persons)
                        data = persons
                    else:
                        persons = data
                    # 常见字段：NAME, CARDID, DEPARTMENT, WORKTYPE, JOB, STATUS 等
                    names = {
                        f"{p.get("name")}_{p.get("cardID")}": {
                            k: v for k, v in p.items() if k in  ["name", "cardID", "department", "workType", "dutyName", "className"] 
                        }
                        for p in data if p.get("name", p.get("NAME", ""))
                    }

                elif type == "car":
                    if data is None:
                        payload = {
                            "deptID": "",
                            "nameOrID": "",
                            "mineCode": "1001"
                        }
                        resp = requests.post(self.car_info_api_url, json=payload, timeout=30, verify=False)
                        text = resp.content.decode("utf-8-sig")
                        cars = json.loads(text).get("data", []) if resp.status_code == 200 else []
                        set_type_data_to_redis("car", cars)
                        data = cars
                    names = {
                        p.get("carName"): {
                            k: v for k, v in p.items() if k != "carName"
                        }
                        for p in data if p.get("carName")
                    }

                elif type == "worktype":
                    if data is None:
                        resp = requests.post(self.work_type_api_url, json={}, timeout=30, verify=False)
                        text = resp.content.decode("utf-8-sig")
                        arr = json.loads(text).get("data", []) if resp.status_code == 200 else []
                        set_type_data_to_redis("worktype", arr)
                        data = arr
                    names = {
                        p.get("workerTypeName"): {
                            k: v for k, v in p.items() if k != "workerTypeName"
                        }
                        for p in data if p.get("workerTypeName")
                    }

                elif type == "area_limit":
                    if data is None:
                        payload = ""
                        headers = {
                            'Content-Type': 'application/json',
                            'Accept': '*/*',
                        }
                        resp = requests.post(self.area_info_api_url, headers=headers, data=payload, timeout=30,
                                             verify=False)
                        text = resp.content.decode("utf-8-sig")
                        arr = json.loads(text).get("data", []) if resp.status_code == 200 else []
                        set_type_data_to_redis("area_limit", arr)
                        data = arr

                    names = {
                        p.get("areaName"): {
                            k: v for k, v in p.items() if k != "areaName"
                        }
                        for p in data if p.get("areaName")
                    }

                elif type == "station":
                    # 查询区域点位 (主/分站)（此类不缓存）
                    query = GET_REALTIME_STATION_HEAD_INFO_SQL
                    rows = self.client.query(query).result_rows
                    names = {
                        row[1]: {
                            'STATIONHEADID': row[0],
                            'STATIONHEADTYPE': row[2]
                        }

                        for row in rows
                    }

                else:
                    return json.dumps({"error": f"未知 type: {type}"}, ensure_ascii=False)
                if name:
                    names = {
                        n: v for n, v in names.items()
                        if fuzz.partial_ratio(name, n) > 70
                    }
                    # 针对type打印一条数据，若有内容则打印第一个item
                # if names:
                #     example_key = next(iter(names))
                #     print(f"[get_infos][type={type}] 示例数据: {example_key}: {names[example_key]}")

                res_json = json.dumps({"stations": names, 'total_nums': len(names)}, ensure_ascii=False,
                                      separators=(",", ":"))
                logger.info(
                    f"get_infos 查询成功: type={type}, total_nums={len(names)}, name_filter={name}, 返回字段长度: {len(res_json)}"
                )
                return res_json



            except Exception as e:
                logger.error(f"get_infos({type},{name}) 异常: {e} {traceback.format_exc()}")
                return json.dumps(
                    {"error": "查询失败", "message": str(e)},
                    ensure_ascii=False
                )

 
        
import asyncio
import json


async def test_all_tools():
    print("🔥 开始测试 MinePersonnelService 的所有 MCP Tools\n" + "=" * 60)

    try:
        # print("\n1️⃣ get_system_time")
        # result = await mcp_app.call_tool("get_system_time", {})
        # print(result)

        # 2️⃣ 覆盖测试 personnel_list 和人员统计（person_bases.py 1025+ 的场景）
        # print("\n2️⃣ query_cars_list 基础筛选（全部默认参数）")
        # result = await mcp_app.call_tool("query_cars_list", {})
        # pprint(result)
        #
        # numeric_filters = {
        #     "距离主站距离/m": {"op": "<", "value": 150},
        #     "入井时长(秒)": {"op": ">", "value": 2 * 3600},
        #     # 更多按需添加...
        #     # "轨迹开始时间": {"op": "between", "value": ["2026-07-02 04:00:00", "2026-07-02 08:00:00"]},
        # }
        # statistics_filter = ['人员列表_姓名_卡号_入井次数', '部门分布/人', '工种分布/人']
        # # print("\n3️⃣ query_personnel_list 带 numeric/statistics 过滤")
        # params = {
        #     "start_date": "2026-07-15 00:00:00",
        #     "end_date": "2026-07-15 23:59:59",
        #     "station_filters":['51206辅运掘面'],
        #     "statistics_filter": statistics_filter
        # }
        # result = await mcp_app.call_tool("query_personnel_list", params)
        # print(result)

        # 也可测试野值、模糊查询
        print("\n4️⃣ query_person_underground_status 模糊姓名")
        params2 = {
            "now_only": True,
            "info_needs": True,
        }
        result = await mcp_app.call_tool("query_person_underground_status", params2)
        
        # print("\n4️⃣ query_car_underground_status 模糊姓名")
        # params2 = {
        #     "now_only": False,
        # }
        # result = await mcp_app.call_tool("query_car_underground_status", params2)
        
        print(result)
        
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
        host="10.11.22.80",
        port=9120,
        user="nethouse",
        password="CGC%EVXr.ET10Y_N",
        database="PS",
    )

    asyncio.run(test_all_tools())
