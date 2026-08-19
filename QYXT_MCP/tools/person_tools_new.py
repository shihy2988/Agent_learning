#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文件名:	person_tools.py
作者:	shihy
创建日期:	2026-04-22
描述:	矿井人员定位相关工具方法，提供人员最新入井记录查询、多人员状态筛选、分段轨迹分析、今日名单等能力。依赖 ClickHouse 实时/历史数据与接口服务，支持多维过滤与分析，适用于 MCP 对接的人员定位服务场景。
"""

from email import message
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
from datetime import datetime
from typing import List, Dict, Optional
import sys
import os
from pprint import pprint
from collections import defaultdict
import copy
from fuzzywuzzy import fuzz,process
from tqdm import  tqdm
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqls.person_sqls import (
    GET_PERSON_LATEST_SQL,
    GET_PERSONS_LATEST_SQL,
    GET_PERSON_TRAJECTORY_SQL,
    GET_TODAY_PERSONS_SQL,
    GET_REALTIME_STATION_HEAD_INFO_SQL,
    GET_AREA_LIMITS_SQL, GET_TODAY_CARS_SQL
)

from sqls.person_sqlites import DataCacheManager,CacheType
from utils.person_utils import (
    get_time_stats, get_type_data_from_redis, set_type_data_to_redis, fetch_and_process_car_history
)

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置日志，日志存储在文件中
from logging.handlers import RotatingFileHandler

LOG_DIR = 'logs'
LOG_FILE = os.path.join(LOG_DIR, 'mine_personnel_service.log')

# 确保日志目录存在
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# 文件日志处理器
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=50*1024*1024,  # 50MB
    backupCount=5,
    encoding='utf-8'
)
file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)

# 终端日志处理器
stream_handler = logging.StreamHandler()
stream_formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
stream_handler.setFormatter(stream_formatter)

logger = logging.getLogger("MinePersonnelService")
logger.setLevel(logging.INFO)

# 避免重复添加 handler
if not any(isinstance(h, RotatingFileHandler) for h in logger.handlers):
    logger.addHandler(file_handler)
if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler) for h in logger.handlers):
    logger.addHandler(stream_handler)


class PersonnelMCPService:
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
            'autogenerate_session_id':False
        }

        try:
            self.client = clickhouse_connect.get_client(**self.db_config)
            logger.info("Successfully connected to ClickHouse.")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
        self.station_names = {}
        self.data_cache = DataCacheManager(
            db_path="data/person_car_cache.db",logger= logger
        )
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

        self._register_resources()
        self._register_prompts()
        self._register_tools()

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
            - ELECTRICITY: 当前车辆电量百分比或描述（如“90%”或“低电量”）
            - MAIN_STATION_ID: 主基站ID，车辆信号采集基站的唯一标识
            - MAIN_STATION_TIME: 主基站接收车辆信号的时间
            - MAIN_STATION_DISTANCE: 车辆到主基站的距离（单位：米）
            - SUB_STATION_ID: 辅助基站ID（如有）
            - SUB_STATION_TIME: 辅助基站信号时间
            - SUB_STATION_DISTANCE: 车辆到辅助基站的距离（单位：米）
            - AREA_ID: 区域ID/分区主键，关联车辆所在的业务区域
            - ENTER_TIME: 进入当前区域/巷道的时间
            - ENTRY_TIME: 车辆进场时间/首次出现在系统中的时间
            """

    # ==================== 2. Prompt: 全局行为准则 ====================
    def _register_prompts(self):

        @self.mcp.prompt()
        def analysis_guide() -> str:
            """
            获取人员定位分析的专业操作指南。模型在处理用户请求前应默认加载此提示词。
            本指南包含不同工具的最佳使用场景，可指导大模型合理推断、自动调用合适的查询与分析工具。
            """
            return """
            你现在是一名矿井安全生产调度专家，具备丰富的井下作业与数据分析经验。使用本系统时，请严格遵循以下操作准则，结合各工具的用途，科学调用、组合工具以获得精确答案：

            【1. 查询井下或区域人员分布】 
            - 若用户关注实时井下总人数、各区域分布及出入情况，请优先使用 `query_person_underground_status(now_only=True/False)`。
            - now_only=True 获取当前实时井下分布，now_only=False 获取今天全量的井下记录（含出入、排班等统计）。

            【2. 某个人员轨迹与分段】
            - 若需查询某个人在一天或多个时间段的详细活动轨迹（进出井、轨迹分段、停留区域），调用 `query_person_trajectory(name, start_time, end_time)`。
            - 如果轨迹结果较多，返回内容将自动精简，仅保留核心区段及时间范围。
            - 若要确定查询的时间范围，建议先用 `get_system_time()` 获取当前时间基准。

            【3. 多条件批量人员过滤】
            - 若需要按照多字段（如姓名、区域、工种、班次、部门、时间区间等）灵活组合筛选，可调用 `query_personnel_list(names, areas, work_types, class_names, departments, start_time, end_time)`。
            - 该工具可获得所有符合条件的人员完整信息和各自详细的出入明细。

            【4. 查找最新入井记录】
            - 如需了解特定人员最新一次入井时间、班次、工种等基础信息，可调用 `find_person_latest_entry(name)`。

            【5. 获取系统基准时间】
            - 当需要推算日期区间（如“本周”或“一天前”）可用 `get_system_time()` 获取服务当前时间作为参考。

            【6. 获取基础数据字典/字段说明】
            - 若对任何数据字段（如 CLASSTIMENAME, DUTYNAME, AREANAME 等）解释有疑惑，调用 `get_data_dictionary()` 查阅说明。

            【7. 查询所有基础类型信息】
            - 如需获取全矿人员、车辆、工种、部门、区域等基础名录及详情，可调用 `get_infos(type, name)` 工具。type可为 department/person/car/worktype/area_limit/station，name为选填模糊名过滤。

            - 【8. 查询井下车辆分布】
            - 若需了解矿井实时或今日内的车辆井下分布情况，可调用 `query_car_underground_status(now_only=True/False)`。
            - now_only=True 获取实时在井下的车辆及其分布，now_only=False 获取今日所有进出井下车辆的统计，包括区域明细。

            - 【9. 查询车辆轨迹】
            - 需要分析某车辆在一天内或特定时段的移动轨迹、分段详情，调用 `query_car_trajectory(cardID, start_time, end_time)`。
            - 可根据车辆ID和时间区间追踪车辆的进出井、行驶路线、区域变化等时空轨迹详情。

            - 【10. 批量车辆条件筛选/列表查询】
            - 若要按照车辆ID、车辆名称、车辆类型、部门、区域、电量等多维组合，批量查询车辆的属性信息、进出井及出入明细等，可调用 `query_cars_list(cardids, car_names, car_types, departments, area_names, electricitys, start_time, end_time)`。
            - 支持多字段灵活过滤，并可精确统计出入井及各类车辆状态信息，适用于车辆大盘分析与场景性筛查。

            # 注意：
            - 若用户未说明时间区间，默认查询当日（或实时）数据。
            - 返回结果字符超限时将自动压缩为精简摘要，保留主要统计与分组字段。
            - 对于不明确的字段，可以调用数据字典 `get_data_dictionary()`。

            请根据用户需求描述，优先选择最贴切的工具，并合理组合使用（如先查询时间基准，再用分段、批量筛选工具）。分析时尽量给出简洁明了、结构化的答案，并解释数据中的主要维度。
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
        ) -> str:
            """
            查询今日、实时井下总人数及各区域人员分布统计。

            功能说明：
            - 获取实时井下矿下人员数据,（通过now_only=True参数控制）
            - 获取今日矿下人员数据,包含人员基本信息、职务、工种、所在区域、进入时间等关键字段（通过now_only=False参数控制）

            参数:
            now_only: 返回实时人员数据或者今日矿下人员数据。

            返回格式:
            - now_only=True: {
                "total_underground": <int>,
                "names": {"姓名": <工种,班次名,部门,areaName>, ...},,
                "area_distribution": {"区域名": <人数>, ...},
                "update_time": "YYYY-MM-DD HH:MM:SS",
              }

            - now_only=False: {
                "total_underground": <int>,
                "total_out_today": <int>,
                "total_today": <int>,
                "update_time": "YYYY-MM-DD HH:MM:SS",
                "names": {"姓名": <工种,班次名,部门,areaName/已出井>, ...},,
                "area_distribution": {"区域名": <人数>, ...},
              }
            """
            logger.info(f"query_person_underground_status called, now_only={now_only}")
       
            try:
                # 1. 获取实时井下人员
                real_time_list = self._fetch_person_realtime_api()

                # ==================== 实时模式 (now_only=True) ====================
                names = {}
                area_stats = {}
                self._init_station_names()
                for p in real_time_list:
                    name = p.get("name")
                    if not name:
                        continue
                    work_type = p.get("workType") or "未知"
                    class_time = p.get("classTimeName") or ""
                    department = p.get("department") or ""
                    main_area = p.get("areaName") or ""
                    area_id = p.get("mainStationID", "未知区域")

                    area = self.station_names.get(int(area_id), "未知区域").get("name", main_area)
                    # 按注释要求格式：工种,班次名,部门
                    info = f"{work_type},{class_time},{department},{area}".strip(",")
                    names[name] = info

                    area_stats[area] = area_stats.get(area, 0) + 1

                if now_only:
                    return json.dumps(
                        {
                            "total_underground": len(real_time_list),
                            "names": names,
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

                # ==================== 今日完整模式 (now_only=False) ====================
                underground_set = {
                    item.get("name") for item in real_time_list if item.get("name")
                }
                t1 = time.time()
                today_persons = self.get_today_persons()

                # 2. 已出井人员
                out_names = []
                for name in today_persons:
                    if name in underground_set:
                        continue
                    out_names.append(name)

                result = self.get_persons_latest(out_names)

                # 统计
                total_underground = len(underground_set)
                total_out_today = len(out_names)

                for name, p in result.items():
                    work_type = p.get("workType") or p.get("dutyName") or "未知"
                    class_time = p.get("classTimeName") or ""
                    department = p.get("department") or ""
                    status_text = "已出井"
                    info = f"{work_type},{class_time},{department},{status_text}".strip(",")
                    names[name] = info

                json_res = json.dumps(
                    {
                        "total_underground": total_underground,
                        "total_out_today": total_out_today,
                        "total_today": len(today_persons),
                        "names": names,
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
                    f"total_today={len(today_persons)}",
                    f"返回字段长度: {len(json_res)}"
                )
           
                return json_res
                
            except Exception as e:
                logger.error(f"query_underground_status 异常: {e}", exc_info=True)
                return json.dumps(
                    {"error": "查询失败", "message": str(e)}, ensure_ascii=False
                )

        @self.mcp.tool()
        def query_person_trajectory(
                name: Optional[str] = None,
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
                成功返回
                {
                    'name': '石小龙',    # 人员姓名
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

                result = self.data_cache.query_with_merge(
                    cache_type=CacheType.PERSON_TRAJECTORY,
                    unique_key=name,  # 人员姓名
                    req_start=start_time,
                    req_end=end_time,
                    # 回调1: 获取缺失数据
                    fetch_func=lambda key, s, e: self.get_person_trajectory_with_stay(
                        name=key, start_time=s, end_time=e
                    ),
                    # 回调2: 自定义合并（复用你的分类逻辑）
                    merge_func=lambda frags, meta, s, e: self._merge_person_trajectory(
                        frags, meta, s, e, name
                    ),
                )

                real_time_list = self._fetch_person_realtime_api()

                # ==================== 实时模式 (now_only=True) ====================
                under_ground = False
                for p in real_time_list:
                    name_real = p.get("name")
                    if name == name_real:
                        under_ground = True
                        break

                # 这里需要调用你实际的查询方法（请根据你的系统实际情况修改这部分）
                # 假设你有一个支持时间范围查询的方法，例如：
                person_records = self.get_person_trajectory_with_stay(
                    name=name,
                    start_time=start_time,
                    end_time=end_time,
                )

                if not person_records:
                    # INSERT_YOUR_CODE
                    logger.info(f"未找到姓名 '{name}' 在指定时间范围({start_time} 至 {end_time})内的轨迹记录")
             
                    return json.dumps({
                        "message": f"未找到姓名 '{name}' 在指定时间范围内的记录"
                    }, ensure_ascii=False)
                
                inout_records = self.fetch_in_out_mine_records(start_time,end_time,'')
                inout_recordsnew = {}
                for record in inout_records:
                    key = record.get('UserName')
                    if key is not None:
                        inout_recordsnew.setdefault(key, []).append(record)

                segments = person_records['segments']
                inout_records_person = inout_recordsnew.get(name)
                if inout_records_person is None and len(inout_recordsnew) > 1 :
                    best_match, score = process.extractOne(name, list(inout_recordsnew.keys()))
                    if score >= 80:  # 80分为相似度阈值，可调整
                        inout_records_person = inout_recordsnew[best_match]
                    else:
                        inout_records_person = []
                else:
                    inout_records_person = []
                resnew = self.classify_segments_by_inout(segments,inout_records_person)

                # 给person_records加上"leave"字段，依据under_ground状态
                person_records["leave"] = "矿下" if under_ground else "出矿"
                person_records['segments'] = resnew
                person_records['total_segments'] =  len(resnew)


                json_str = json.dumps(person_records, ensure_ascii=False, separators=(",", ":"))
                len_json_str = len(json_str)
                if len_json_str > 80000:
                    # 只保留核心精简内容
                    simple_segments = []
                    # 合并前后 areaName 相同的分段，合并时间区间
                    segments_new = person_records.get("segments", [])
                    for seg_s in segments_new:
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
                            # current_area = seg.get("areaName", "井下")
                            station = seg.get("mainStationId", " ")
                            seg_start = seg.get("segmentStartTime")
                            seg_end = seg.get("segmentEndTime")
                            # 只保留精简必要字段
                            segment_item = {
                                # "areaName": current_area,
                                "station": station,
                                "segmentStartTime": seg_start,
                                "segmentEndTime": seg_end
                            }
                            segment_list.append(segment_item)
                        prev_seg["segments"] = segment_list
                        prev_seg["segments_count"] = len(segment_list)
                        simple_segments.append(prev_seg)

                    slim_data = {
                        "name": person_records.get("name"),
                        "start": person_records.get("start"),
                        "end": person_records.get("end"),
                        "department": person_records.get("department"),
                        "workType": person_records.get("workType"),
                        "job": person_records.get("job"),
                        "cardid": person_records.get("cardid"),
                        "total_segments": person_records.get("total_segments"),
                        "leave": "矿下" if under_ground else "出矿",
                        "segments": simple_segments,
                        "desc":'由于数据过多,精简回答,需要详细信息请给出更精准的限制信息'
                    }
             
                    str_json = json.dumps(slim_data, ensure_ascii=False, separators=(",", ":"))
                    
                    logger.info(
                        f"find_status 查询成功: 返回精简JSON, 字段长度: {len(str_json)}"
                    )
                    return str_json

                detailed_json = json.dumps(person_records, ensure_ascii=False, separators=(",", ":"))
                logger.info(
                    f"find_status 查询成功: 返回详细JSON, 字段长度: {len(detailed_json)}"
                )
                return detailed_json
           

            except Exception as e:
                logger.error(f"find_status 异常: {e}", exc_info=True)
                return json.dumps(
                    {"error": "查询失败", "message": str(e)},
                    ensure_ascii=False
                )

        @self.mcp.tool()
        def query_personnel_list(
                cardids: list = None,
                names: list = None,
                electricitys: list = None,
                # areas: list = None,
                work_types: list = None,
                class_names: list = None,
                departments: list = None,
                start_time: str = None,
                end_time: str = None,
                main_stations: list = None,   # 增加主站名称模糊匹配列表
                sub_stations: list = None,    # 增加分站名称模糊匹配列表
        ) -> str:

            """
            功能说明: 多条件综合查询人员名单。支持根据主站区域、分站区域、班次、工种、部门及指定时间段进行灵活组合筛选，常见应用如查询“指定区域在某时段的所有人员”或“今日某班次出勤人员名单”。
            如果需要确定系统start_time 和 end_time 请先调用 get_system_time 函数

            参数说明:
                cardids (list, 可选): 需模糊匹配或精确匹配的工号列表，如 ["12345", "5678%"]，用于人员工号筛选，支持通配符 % 实现模糊查询。
                names (list, 可选): 需模糊匹配的人员姓名列表，如 ["张三", "李四"]，用于人员姓名筛选。
                electricitys (list, 可选): 电量状态筛选，仅支持 ["正常", "低电量"] 两种状态，低电量就是不正常的状态。
                main_stations (list, 可选): 需模糊匹配的主站区域名称列表，用于主站中文名模糊查询。
                sub_stations (list, 可选): 需模糊匹配的分站区域名称列表，用于分站中文名模糊查询。
                work_types (list, 可选): 工种名称列表，用于筛选如 ["电工", "掘进工"] 等特定工种的人员。
                class_names (list, 可选): 班次名称列表，仅支持["早班", "中班","夜班"]，用于筛选特定班次人员。
                departments (list, 可选): 部门名称列表，用于筛选所属部门人员。
                start_time (string, 可选): 开始时间，格式为 "YYYY-MM-DD HH:MM:SS"，不传则默认当天 00:00:00。
                end_time (string, 可选): 结束时间，格式为 "YYYY-MM-DD HH:MM:SS"，不传则默认当天 23:59:59。

            返回值:
            
            返回一个包含所有符合条件的、去重的人员详细信息字典，其中包括姓名、部门、工种、班次及每人的详细记录。例如:
                {
                  "total": 58643,  # 总的分段数（所有出入记录的数量）
                  "update_time": "2026-04-28 11:11:35",  # 查询时刻的数据更新时间
                  "persons": {  # 按姓名分组的所有人员信息字典
                    "霍红强": {  # 人员姓名
                      "cardid": "6731",  # 卡号
                      "workType": "皮带司机",  # 工种/岗位
                      "classTimeName": "早班",  # 班次名称
                      "department": "机运队",  # 部门名称
                      "records": [  # 该人员的每个出入矿井（完整上下班时段）为一条记录
                        {
                          "inTime": "2026-04-15T15:58:46",     # 入井时间（ISO格式）
                          "outTime": "2026-04-16T00:15:16",    # 出井时间（ISO格式）
                          "inPlace": "副井口2号",              # 入井地点
                          "outPlace": "副井口2号",              # 出井地点
                          "duration": "8时16分30秒",            # 本次在井时长
                          "segments_count": 4,                  # 分段数量
                          "segments": {                         # 井下分段信息（稀疏存储，key/value数组方式，节省体积）
                            "keys": [                           # 字段名说明：每项含义如下
                              "areaName",              # 区域名称
                              "segmentStartTime",      # 分段开始时间
                              "segmentEndTime",        # 分段结束时间
                              "electricity",           # 分段电量状态
                              "mainStationId",         # 主站标识/名称
                              "mainStationDistance",   # 主站实际距离，单位米（可多维，如[x, y]坐标或单一距离）
                              "subStationId",          # 分站标识/名称
                              "subStationDistance"     # 分站实际距离，单位米
                            ],
                            "values": [  # 每个分段一行，顺次对应 keys
                              [
                                "井下",
                                "2026-04-16 00:00:24",
                                "2026-04-16 00:00:32",
                                "正常",
                                "5-1煤主运6联巷29号",
                                [147.7, 45.4],
                                "5煤主运4联巷内14号",
                                285.8
                              ],
                              [
                                ...  # 其他分段信息，以同样结构表示

                              ]
                            ]
                          }
                        },
                        {
                          "inTime": "2026-04-21T23:47:53",  # 入井时间
                          "outTime": "2026-04-22T07:34:49", # 出井时间
                          "inPlace": "副井口2号",           # 入井地点
                          "outPlace": "主井口3号",           # 出井地点
                          "duration": "7时46分56秒",         # 本次在井时长
                          "segments_count": 370,             # 分段数量
                          "segments": {
                            # ... 此处 segments 略（其结构同上例）
                          }
                        }
                      ]
                    }
                    # ... 其他人员继续在此增加
                  }
                }
                
            如果压缩后数据仍然超过指定大小，将只返回如下三个字段：
            {
                "persons_inout_count": { ... },                  # 各人员的出入井/区域次数信息
                "_summary_area_persons_per_day": { ... },        # 全局区域人员数量（日维度统计）
                "_summary_person_unique_area_per_day": { ... }   # 各人员每日唯一出入区域次数（日维度统计）
            }
   
            """
            # INSERT_YOUR_CODE
            logger.info(
                f"query_personnel_list called with params: cardids={cardids}, names={names}, electricitys={electricitys}, work_types={work_types}, "
                f"class_names={class_names}, departments={departments}, start_time={start_time}, end_time={end_time}, "
                f"main_stations={main_stations}, sub_stations={sub_stations}"
            )
     
            try:
                # 1. 执行核心查询
                # 去除names 中 每个name的空格
                if names is not None:
                    # 支持list或str类型
                    if isinstance(names, list):
                        names = [n.replace(' ', '') if isinstance(n, str) else n for n in names]
                    elif isinstance(names, str):
                        names = names.replace(' ', '')
                # 如果 start_time 或 end_time 未提供，则默认值为今天的 00:00:00 和 23:59:59
                if not start_time or not end_time:
                    
                    today = datetime.now().date()
                    if not start_time:
                        start_time = f"{today} 00:00:00"
                    if not end_time:
                        end_time = f"{today} 23:59:59"
         
                person_records = self.get_persons_by_filters(
                    cardids=cardids,
                    names=names,
                    areas=None,
                    electricitys=electricitys,
                    work_types=work_types,
                    class_names=class_names,
                    departments=departments,
                    start_time=start_time,
                    end_time=end_time,
                    main_stations=main_stations,
                    sub_stations=sub_stations,
                )

                if not person_records or not person_records.get("persons"):
                    # INSERT_YOUR_CODE
                    logger.info("query_personnel_list 未找到人员记录: person_records=%s, filters: cardids=%s, names=%s, electricitys=%s, work_types=%s, class_names=%s, departments=%s, start_time=%s, end_time=%s, main_stations=%s, sub_stations=%s",
                                person_records, cardids, names, electricitys, work_types, class_names, departments, start_time, end_time, main_stations, sub_stations)
             
                    return json.dumps({"message": "未找到符合条件的人员记录"}, ensure_ascii=False)

                real_time_list = self._fetch_person_realtime_api()

                # ==================== 实时模式 (now_only=True) ====================
                names = []
                for p in real_time_list:
                    name_real = p.get("name")
                    names.append(name_real)

                inout_records = self.fetch_in_out_mine_records(start_time, end_time, '')
                inout_recordsnew = {}
                for record in inout_records:
                    key = record.get('UserName')
                    if key is not None:
                        inout_recordsnew.setdefault(key, []).append(record)
           
                for name, info in person_records["persons"].items():
                    segments = info['records']
                    # 使用fuzzywuzzy进行模糊匹配查找
                    inout_records_person = inout_recordsnew.get(name)
                    if inout_records_person is None:
                        best_match, score = process.extractOne(name, list(inout_recordsnew.keys()))
                        if score >= 80:  # 80分为相似度阈值，可调整
                            inout_records_person = inout_recordsnew[best_match]
                        else:
                            inout_records_person = []

                    resnew = self.classify_segments_by_inout(segments, inout_records_person)

                    info['records'] = resnew

                # --- 精简逻辑: 压缩只保留主要统计字段与去重的 uniqueAreas ---

                area_stats_per_day = defaultdict(lambda: defaultdict(set))  # {date: {area: set(persons)}}
                person_inout_stats_per_day = defaultdict(lambda: defaultdict(int))  # {date: {person: count}}

                for name, info in person_records["persons"].items():
                    remove_fields = ["updateTimes", "count", "duration", "job", "stationDate"]
                    kept_fields = ['areaName', 'segmentStartTime', 'segmentEndTime', 'electricity', 'mainStationId', 'mainStationDistance', 'subStationId', 'subStationDistance']
                    for segment in info["records"]:
                        segments = {
                            'keys': kept_fields,
                            'values': [],
                        }

                        unique_areas_per_day = defaultdict(set)  # {date: set(areaName)}

                        for r in segment['segments']:
                            # 移除不需要的字段
                            for key in remove_fields:
                                r.pop(key, None)
                            # 严格按kept_fields顺序收集value
                            kept_values = [r.get(k, None) for k in kept_fields]
                            segments['values'].append(kept_values)

                            # 收集日期和区域
                            date_str = None
                            s_time = r.get('segmentStartTime')
                            if s_time:
                                try:
                                    date_str = s_time[:10]
                                except Exception:
                                    pass
                            area_name = r.get('areaName')
                            if date_str and area_name:
                                area_stats_per_day[date_str][area_name].add(name)
                                unique_areas_per_day[date_str].add(area_name)

                        # 不是按段数，是按去过多少个不同区域
                        for date_str, areas in unique_areas_per_day.items():
                            person_inout_stats_per_day[date_str][name] = len(areas)

                        segment['segments'] = segments
           

                    if name in names:
                        now_work = "矿下"
                    else:
                        now_work = "出矿"
                    info['now_work'] = now_work
                    info["count"] = len(info["records"])

                # 构建统计结果（按天，每个区域每天有多少人、每个人每天的出入井次数）
                overall_area_stats = {}
                for date, areas in area_stats_per_day.items():
                    overall_area_stats[date] = {area: len(persons) for area, persons in areas.items()}

                overall_person_stats = {}
                for date, persons in person_inout_stats_per_day.items():
                    overall_person_stats[date] = dict(persons)

                person_records["_summary_area_persons_per_day"] = overall_area_stats
                person_records["_summary_person_inout_counts_per_day"] = overall_person_stats

                json_full = json.dumps(person_records, ensure_ascii=False, separators=(",", ":"))
                len_json_full1 = len(json_full)

                if len_json_full1 > 100000:
                    for name, info in person_records["persons"].items():
                        for segment in info["records"]:
                            segments = [v[4] for v in  segment['segments']['values']]
                            segment['segments'] = segments
                    person_records['desc'] = '数据量过大,如果需要更精细信息请提供更明确信息再查询'
                    json_full = json.dumps(person_records, ensure_ascii=False, separators=(",", ":"))

                len_json_full2 = len(json_full)
                if len_json_full2 > 80000:
                    for name, info in person_records["persons"].items():
                        for segment in info["records"]:
                            segment.pop('segments')

                    json_full = json.dumps(person_records, ensure_ascii=False, separators=(",", ":"))
                len_json_full3 = len(json_full)
                if len_json_full3 > 50000:
                    for name, info in person_records["persons"].items():
                        keys = ['inTime','outTime','inPlace','outPlace','duration']
                        values = []
                        for segment in info["records"]:
                            value = [segment.get(k, None) for k in keys]
                            values.append(value)
                        info["records"] = [keys, values]

                    json_full = json.dumps(person_records, ensure_ascii=False, separators=(",", ":"))

                # 再次判断，如果仍然大于 50000 则进一步压缩：只保留人名和出入井次数（出入井段数）
                len_json_full4 = len(json_full)
                if len_json_full4 > 50000:
                    compact_records = {}
                    for name, info in person_records["persons"].items():
                        # 'records' 存在时统计个数
                        cnt = 0
                        records = info.get("records")
                        # 记录已被精简为 [keys, values] 或直接是段数？
                        if isinstance(records, list) and len(records) == 2 and isinstance(records[1], list):
                            cnt = len(records[1])
                        elif isinstance(records, list):
                            cnt = len(records)
                        info.clear()  # 移除其他字段
                        info["count"] = cnt
                    # 只保留 persons 字段及 desc
                    person_records = {
                        "persons_inout_count": person_records["persons"]
                    }
                    # 如果前面有 desc 信息则也保留
                    if "desc" in person_records:
                        person_records["desc"] = person_records["desc"]

                    # 再加上全局区域人员数量和个人出入井次数（日统计）
                    person_records["_summary_area_persons_per_day"] = overall_area_stats
                    person_records["_summary_person_unique_area_per_day"] = overall_person_stats

                    json_full = json.dumps(person_records, ensure_ascii=False, separators=(",", ":"))
                # INSERT_YOUR_CODE
                logger.info(
                    f"压缩人员 records, 当前json长度: len_json_full4={len(json_full)}, "
                    f"最终输出字段: keys={list(person_records.keys())}"
                )
         
                return json_full

            except Exception as e:
                logger.error("traceback: %s", traceback.format_exc())
           
                return json.dumps({
                    "error": "查询失败",
                    "message": str(e),
                    "traceback": traceback.format_exc()
                }, ensure_ascii=False)

        @self.mcp.tool()
        def find_person_latest_entry(
                name: Optional[str] = None,
                cardid: Optional[str] = None,
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
        ) -> str:
            """
            查询今日、实时井下车辆及各区域车辆分布统计。

            功能说明：
            - 获取实时井下矿下车辆数据,（通过now_only=True参数控制）
            - 获取今日矿下车辆数据,包含车辆基本信息、部门、所在区域、入井时间等关键字段（通过now_only=False参数控制）

            参数:
            now_only: 返回实时车辆数据或者今日矿下车辆数据。

            返回格式:
            - now_only=True: {
                "总井下车辆数": <int>,
                "车辆信息": {"车牌号": <车牌ID, 部门, 车辆类型, 电量, 主站名称, 距主站距离(米), 入井时间>, ...},
                "区域分布": {"区域名": <车辆数>, ...},
                "更新时间": "YYYY-MM-DD HH:MM:SS",
              }

            - now_only=False: {
                "总井下车辆数": <int>,
                "今日出井车辆数": <int>,
                "今日总车辆数": <int>,
                "更新时间": "YYYY-MM-DD HH:MM:SS",
                "车辆信息": {"车牌号": <车牌ID, 部门, 车辆类型, 电量, 主站名称, 距主站距离(米), 入井时间, 出井/入井>, ...},
                "区域分布": {"区域名": <车辆数>, ...},
              }
         

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

                for car in car_realtime_list:
                    CARNAME = car.get("carName", "") or car.get("carCode") or car.get("cardId") or ""
                    if not CARNAME:
                        continue
                    # 填充标准化字段

                    cars[CARNAME] = {
                        "车牌ID": car.get("carCode", ""),  # CARCODE
                        "部门": car.get("department", ""),
                        "车辆类型": car.get("carTypeName", ""),
                        "电量": car.get("electricity", ""),
                        "主站名称": car.get("mainStationID", ""),
                        "主站时间": car.get("mainStationTime", ""),
                        "主站距离(米)": car.get("mainStationDistance", ""),
                        "入井时间": car.get("enterTime", "")
                   
                    }
                    # 实时区域分布
                    area = car.get("mainStationID", "未知区域")
                    area_stats[area] = area_stats.get(area, 0) + 1

                if now_only:
                    return json.dumps(
                        {
                            "total_underground": len(cars),
                            "cars": cars,
                            "area_distribution": dict(
                                sorted(area_stats.items(), key=lambda x: x[1], reverse=True)
                            ),
                            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
                        "电量": row[4],
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

                # 填入已出井车辆
                for car_code, car in today_cars.items():
                    # 只需填核心字段, 保持格式一致
                    cars[car_code] = {
                        "车牌ID": car.get("CARCODE", ""),
                        "部门": car.get("DEPARTMENT", ""),
                        "车辆类型": car.get("CARTYPENAME", ""),
                        "电量": car.get("ELECTRICITY", ""),
                        "主站名称": car.get("MAINSTATIONID", ""),
                        "主站时间": car.get("MAINSTATIONTIME", ""),
                        "主站距离(米)": car.get("MAINSTATIONDISTANCE", ""),
                        "入井时间": car.get("ENTERTIME", ""),
                        "状态": "出矿" if car_code in out_cars else "矿下"
                   
                    }
                # 统计区域：只统计实时在矿车辆的区域分布即可
                json_res = json.dumps(
                    {
                        "矿下车辆总数": total_underground,
                        "今日出井车辆数": total_out_today,
                        "今日总车辆数": len(today_cars),
                        "车辆信息": cars,
                        "区域分布": dict(
                            sorted(area_stats.items(), key=lambda x: x[1], reverse=True)
                        ),
                        "更新时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
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
                cardName: Optional[str] = None,
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
                    logger.info(f"未找到车辆名称 '{cardName}' 的编号, cardID 未获取到, 输入参数: cardName={cardName}, start_time={start_time}, end_time={end_time}")
             
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

                inout_records_person = inout_recordsnew.get(carName,[])
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
                cardids: list = None,
                car_names: list = None,
                electricitys: list = None,
                area_names: list = None,
                departments: list = None,
                car_types: list = None,
                start_time: str = None,
                end_time: str = None,
                main_stations: list = None,   # 增加主站名称模糊匹配列表
                sub_stations: list = None,    # 增加分站名称模糊匹配列表
        ) -> str:
            """
            功能说明: 多条件综合查询车辆名单。支持根据车辆ID、车辆名称、电量状况、车辆类型、所在区域、部门、指定时间段进行灵活组合筛选，例如“指定区域在某时段的所有车辆”或“今日低电量车辆名单”。

            参数:
                cardids (list, 可选): 需模糊/精确匹配的车辆ID，如 ["012", "99"]，支持通配符%实现模糊查询。
                car_names (list, 可选): 车辆名称列表，用于模糊匹配车辆名,如["9号自行车(瓦检)","SYG009"]。
                electricitys (list, 可选): 电量状态筛选，仅支持["正常", "低电量"]。
                area_names (list, 可选): 需模糊匹配的区域名称列表，如["43204"]，用于筛选在指定区域的车辆。
                departments (list, 可选): 部门名称列表。
                car_types (list, 可选): 车辆类型列表，如["运输车", "人车"]，用于车辆类型筛选。
                start_time (string, 可选): 开始时间，格式 "YYYY-MM-DD HH:MM:SS"。
                end_time (string, 可选): 结束时间，格式同上。
                main_stations (list, 可选): 主站名称模糊匹配列表，新增字段。
                sub_stations (list, 可选): 分站名称模糊匹配列表，新增字段。

            返回格式:
                {
                    "total": <车辆总数>,
                    "update_time": "YYYY-MM-DD HH:MM:SS",
                    "cars": {
                        "125": {
                            "carName": "桂B12345",
                            "carType": "运输车",
                            "cardId": "125",   #车辆编号
                            "department": "运输队",
                            "area": ["井下南翼",...],  # 车辆出现区域列表
                            "enterTime": ["2026-04-23 08:41:02",...], # 入井时间列表
                            "leaveTime": ["2026-04-23 16:45:12",...], # 出井时间列表
                            "electricity": ["正常"],
                            "now_status": "矿下"  # 实时状态：矿下/出矿
                        },
                        ...
                    }
                }
                若未找到记录，返回: {"message": "未找到符合条件的车辆记录"}

            """
            # INSERT_YOUR_CODE
            logger.info(
                f"query_cars_list called with params: cardids={cardids}, car_names={car_names}, electricitys={electricitys}, area_names={area_names}, departments={departments}, car_types={car_types}, start_time={start_time}, end_time={end_time}, main_stations={main_stations}, sub_stations={sub_stations}"
            )
     
            try:

                # 调用数据查询逻辑，这里假设存在 self.get_cars_by_filters 方法，新增car_types参数和主分站支持
                if not start_time or not end_time:
                    today = datetime.now().date()
                    if not start_time:
                        start_time = f"{today} 00:00:00"
                    if not end_time:
                        end_time = f"{today} 23:59:59"
                car_records = self.get_cars_by_filters(
                    cardids=cardids,
                    car_names=car_names,
                    area_names=area_names,
                    electricitys=electricitys,
                    departments=departments,
                    car_types=car_types,
                    start_time=start_time,
                    end_time=end_time,
                    main_stations=main_stations,
                    sub_stations=sub_stations,
                )
        

                if not car_records or not car_records.get("cars"):
                    # INSERT_YOUR_CODE
                    logger.info(
                        f"query_cars_list 未找到车辆记录: car_records={car_records}, filters: cardids={cardids}, car_names={car_names}, electricitys={electricitys}, area_names={area_names}, departments={departments}, car_types={car_types}, start_time={start_time}, end_time={end_time}, main_stations={main_stations}, sub_stations={sub_stations}"
                    )
             
                    return json.dumps({"message": "未找到符合条件的车辆记录"}, ensure_ascii=False)

                # 实时车辆情况，判断车辆是否在矿下
                real_time_list = self._fetch_car_realtime_api()
                in_mine_ids = set()
                for car in real_time_list:
                    carid = car.get("cardID") or ""
                    if carid:
                        in_mine_ids.add(carid)

                inout_records = self.fetch_in_out_mine_records(start_time, end_time, '')
                inout_recordsnew = {}
                for record in inout_records:
                    key = record.get('UserName')
                    if key is not None:
                        inout_recordsnew.setdefault(key, []).append(record)


                # 给每辆车增加 now_status 字段：矿下/出矿，同时补充carType字段（如没有可为空字符串）
                for cid, info in car_records.get("cars", {}).items():
                    carName = info.get("carName")
                    res_segments = info.get('records')
                    # 找到key中包含carName的（允许key为carName或carName+其他字符串）
                    matched_keys = [k for k in inout_recordsnew.keys() if carName and (k == carName or k.startswith(f"{carName}"))]
                    if not matched_keys:
                        inout_records_person = []
                    else:
                        # 如果有多个匹配，合并所有记录
                        inout_records_person = []
                        for k in matched_keys:
                            inout_records_person.extend(inout_recordsnew[k])

                    resnew = self.classify_segments_by_inout(res_segments, inout_records_person)
                    resnew_merged = {}
                    if not resnew or not isinstance(resnew, list) or len(resnew) == 0:
                        resnew_merged = {}
                    else:
                        # 针对resnew[0]['segments']按mainStationId进行整合分组，并汇总相关属性
                        segs = resnew[0].get('segments', [])
                        main_station_dict = defaultdict(list)
                        for seg in segs:
                            main_station_dict[seg.get('mainStationId', '')].append(seg)
                        merged_segments = []
                        for main_station, seg_list in main_station_dict.items():
                            # 合并所有该mainStationId下的信息
                            merged_data = {
                                "mainStationId": main_station,
                                "count": len(seg_list),
                                "inTime": seg_list[0].get("segmentStartTime"),
                                "outTime": seg_list[-1].get("segmentEndTime"),
                                "minMainStationDistance": min(seg.get("mainStationDistance", 0.0) or 0.0 for seg in seg_list),
                                "maxMainStationDistance": max(seg.get("mainStationDistance", 0.0) or 0.0 for seg in seg_list),
                                "subStations": list({seg.get("subStationId", "") for seg in seg_list}),
                                "electricity": seg_list[0].get("electricity", ""),
                                # 可以继续添加需要聚合的其他属性...
                            }
                            merged_segments.append(merged_data)
                        resnew_merged = copy.deepcopy(resnew[0])
                        resnew_merged["segments_merged_by_mainStationId"] = merged_segments
                    resnew_merged.pop('segments',None)
                    info['records'] = resnew_merged
                    if cid in in_mine_ids:
                        info['now_status'] = "矿下"
                    else:
                        info['now_status'] = "出矿"
                    # 确保包含carType字段
                    if "carType" not in info:
                        info["carType"] = ""

                car_records["total"] = len(car_records.get("cars", {}))
                car_records["update_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                json_full =   json.dumps(car_records, ensure_ascii=False, separators=(",", ":"))
                len_json_full = len(json_full)
                # INSERT_YOUR_CODE
                logger.info(
                    f"query_cars_list 查询成功: 总车辆数={car_records.get('total')}, "
                    f"输出JSON长度={len_json_full}, "
                    f"输出字段: keys={list(car_records.keys())}"
                )
         
                return json_full
            except Exception as e:
                logger.error(f"query_cars_list 异常: {e} {traceback.format_exc()}")
                return json.dumps(
                    {
                        "error": "查询失败",
                        "message": str(e),

                    },
                    ensure_ascii=False
                )

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

                    if s_n and fuzz.partial_ratio(station_name, s_n) > 90:
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
                        collected.extend(data)

                    except Exception as ee:
                        logger.warning(f"request error for stationHeadid {sid}: {ee}")

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
                if type in cache_types:
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
                        p.get("name"): {
                            k: v for k, v in p.items() if k != "name"
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

    # ==================== 4. 内部辅助逻辑 ====================
    def _merge_person_trajectory(self, fragments: List[Dict], base_meta: Dict,
                                 req_start: str, req_end: str, name: str) -> Dict:
        """人员轨迹自定义合并：复用 classify_segments_by_inout"""
        # 1. 收集所有 segments
        all_segs = []
        for frag in fragments:
            all_segs.extend(frag.get('segments', []))

        # 2. 去重 + 裁剪
        trimmed = self.data_cache._deduplicate_and_trim_segments(all_segs, req_start, req_end)

        # 3. 重新分组（复用你的逻辑）
        inout_records = self.fetch_in_out_mine_records(req_start, req_end, '')
        grouped = self.classify_segments_by_inout(trimmed, inout_records)

        return {
            **base_meta,
            'name': name,
            'start': req_start,
            'end': req_end,
            'segments': grouped,
            'total_segments': len(grouped)
        }

    def get_person_name_cardid_dicts(self):
        """
        获取 name->cardid 和 cardid->name 的映射字典

        Returns:
            tuple: (name2cardid, cardid2name)
        """
        try:
            # 尝试先从缓存拿数据
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
                name = p.get("name") or p.get("personName")    # 兼容不同字段
                cardid = p.get("cardID") or p.get("cardId")
                if name and cardid:
                    name2cardid[name] = cardid
                    cardid2name[cardid] = name

            return name2cardid, cardid2name
        except Exception as e:
            logger.error(f"get_person_name_cardid_dicts异常: {e}", exc_info=True)
            return {}, {}
    
    def parse_time(self,t: str):
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

        for idx_r, record in tqdm(enumerate(inout_records)):
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


    def _init_station_names(self):
        """
        初始化 self.station_names，若其为空，则从数据库查询并赋值。
        """
        if len(self.station_names) < 1:
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
            main_stations: list = None,   # 新增 主站（中文名模糊）
            sub_stations: list = None,    # 新增 分站（中文名模糊）
    ):
        """
        支持全字段模糊查询。
        如果不传 start_time/end_time，默认查询当日数据。
        cardids 支持精确或模糊。
        electricitys 仅支持 ['正常', '低电量'] 这两种状态过滤。
        main_stations/sub_stations 支持模糊中文名传入，自动匹配到ID
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
                    # 模糊匹配（可根据实际需求调整阈值）
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
                    # 用户传了通配符，直接作为模糊
                    cardid_clauses.append(f"CARDID LIKE %({key})s")
                    params[key] = f"{cid}"
                else:
                    # 也用模糊 (通常工号是数字，可支持部分匹配)
                    cardid_clauses.append(f"CARDID LIKE %({key})s")
                    params[key] = f"%{cid}%"
            where_conditions.append(f"({' OR '.join(cardid_clauses)})")

        # 3.2. 处理 electricitys（电量）-- 只支持['正常','低电量','其他']
        # 假设电量字段名为 ELECTRICITY
        if electricitys:
            statuses = set([str(x) for x in electricitys])
            normal_selected = "正常" in statuses
            low_selected = "低电量" in statuses
            other_selected = "其他" in statuses

            elec_clauses = []

            # 先选定需要的
            sub_statuses = []
            if normal_selected:
                sub_statuses.append("正常")
            if low_selected:
                sub_statuses.append("低电量")
            if sub_statuses:
                # 选了正常/低电量，加 IN 子句
                placeholder = []
                for idx, val in enumerate(sub_statuses):
                    params[f"elec_status_{idx}"] = val
                    placeholder.append(f"%({f'elec_status_{idx}'})s")
                elec_clauses.append(f"ELECTRICITY IN ({', '.join(placeholder)})")

            if other_selected:
                # '其他' 表示非正常/低电量
                other_condition = "ELECTRICITY NOT IN (%(elec_not_normal)s, %(elec_not_low)s)"
                params["elec_not_normal"] = "正常"
                params["elec_not_low"] = "低电量"
                elec_clauses.append(other_condition)

            if elec_clauses:
                # 括号合并
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

        # 5. 组合最终 SQL
        where_sql = " AND ".join(where_conditions)

        # 【修改点 1】：SQL 增加 any(CARDID) 和 any(ELECTRICITY)

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
                        PARTITION BY CARDID
                        ORDER BY u_time
                    ) AS prev_station,

                lagInFrame(stationDate)
                    OVER (
                        PARTITION BY CARDID
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
                    PARTITION BY CARDID
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

                # 每次碰到新的人时，初始化基础信息（如果还没建）
                if name not in persons_dict:
                    persons_dict[name] = {
                        "cardid": row[13],
                        "workType": row[11],
                        "classTimeName": row[2],
                        "department": row[10],
                        "records": [],
                    }

                # 记录分段的出入信息
                record = {
                    "areaName": row[1],
                    "segmentStartTime": str(row[5]).replace('+08:00',''),
                    "segmentEndTime": str(row[6]).replace('+08:00',''),
                    "electricity": row[15],
                    "mainStationId": self.station_names.get(int(row[3]), {}).get("name", row[3]),
                    "mainStationDistance": row[9],
                    "subStationId": self.station_names.get(int(row[16]), {}).get("name", row[16]) if  row[16]  else row[16],
                    "subStationDistance": row[17],
                    "stationDate": str(row[4]),
                    "count": row[7],
                    "duration": row[8],
                    "job": row[12],
                    "updateTimes": list({str(x) for x in row[14]}) if row[14] else [],
               
                }

                persons_dict[name]["records"].append(record)
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
            main_stations: list = None,   # 新增 主站（中文名模糊）
            sub_stations: list = None,    # 新增 分站（中文名模糊）
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

    def get_persons_latest(self, names: Union[str, List[str]]) -> Dict[str, Optional[Dict]]:
        if isinstance(names, str):
            names = [names]

        name_list = list(dict.fromkeys([n.strip() for n in names if n]))
        if not name_list:
            return {}

        query = GET_PERSONS_LATEST_SQL

        try:
            result = self.client.query(query, parameters={"names": name_list}).result_rows

            latest_dict = {}
            for row in result:
                name = row[0]
                latest_dict[name] = {
                    "name": name,
                    "department": row[1],
                    "classTimeName": row[2],
                    "dutyName": row[3],
                    "workType": row[4],
                    "areaName": row[5],
                    "mainStationTime": row[6],
                    "mainStationDistance": row[7],
                    "subStationTime": row[8],
                    "subStationDistance": row[9],
                    "areaTime": row[10],
                    "enterTime": row[11],
                    "updateTime": row[12],
                }

            # 补全没有记录的人员
            for name in name_list:
                if name not in latest_dict:
                    latest_dict[name] = None

            return latest_dict

        except Exception as e:
            logger.error(f"get_persons_latest 查询失败: {e}")
            return {name: None for name in name_list}

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
        query = GET_PERSON_TRAJECTORY_SQL
        query2 = GET_REALTIME_STATION_HEAD_INFO_SQL
        try:
            result = self.client.query(query, parameters={
                "name": name,
                "start": start_time,
                "end": end_time
            })
            result2 = self.client.query(query2, parameters={})
            result2 = {
                str(row[0]): (row[1], row[2])
                for row in result2.result_rows
            }
            segments = []
            dept, w_type, job_title, card_id = "", "", "", ""

            for row in tqdm(result.result_rows):
                dept, w_type, job_title, card_id = row[10], row[11], row[12], row[13]

                # 获取该段内所有的 u_time，计算统计值
                time_list = list(row[14])
                station_id = row[3]
                station_name = result2[station_id][0] if station_id in result2 else f"主分站id{station_id},分站信息未入库"
                stats = self.get_time_stats(time_list)
                try:
                    segments.append({
                        # "areaName": row[1],
                        "classTimeName": row[2],
                        "mainStationId": station_name,
                        "segmentStartTime": stats["earliest"],
                        "segmentEndTime": stats["latest"],
                        "segmentDurationSeconds": stats["duration_seconds"],
                        # row[9] 已经是按时间排序且去除了相邻重复的距离列表
                        "areaChanges": [str(d) for d in list(row[9])],
                        "recordCount": int(row[7])
                    })
                except Exception as err:
                    logger.error(f"Error appending segment for row: {row}, error: {err}")
               
            res = {
                "name": name,
                "start": start_time,
                "end": end_time,
                "department": dept,
                "workType": w_type,
                "job": job_title,
                "cardId": card_id,
                "total_segments": len(segments),
                "segments": segments
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
        return [row[0] for row in result]


import asyncio
import json


async def test_all_tools():
    print("🔥 开始测试 MinePersonnelService 的所有 MCP Tools\n" + "=" * 60)

    try:
        # 1. 系统时间
        # print("\n1️⃣ get_system_time")
        # result = await mcp_app.call_tool("get_system_time", {})
        # print(result)

        # 2. 实时井下状态（最常用）
        # print("\n2️⃣ query_person_underground_status (实时模式)")
        # result = await mcp_app.call_tool("query_person_underground_status", {"now_only": True})
        # print(result)

        # # 3. 今日完整模式
        # print("\n3️⃣ query_person_underground_status (今日完整模式)")
        # result = await mcp_app.call_tool(
        #     "query_person_underground_status", {"now_only": False}
        # )
        # print(result)
        # # 4. 特定人员查询
        # print("\n4️⃣ trajectory")
        # test_name = "安明芸"  # ← 请改成你系统中真实存在的人员姓名！
        # result = await mcp_app.call_tool("query_person_trajectory", {"name": test_name,"start_time":"2026-04-10 08:00:00",
        # "end_time":"2026-04-10 10:00:00"})
        # pprint(result)

        # # 5.多项状态查询
        # print("\n6️⃣ query_personnel_list")
        # result = await mcp_app.call_tool(
        #     "query_personnel_list", {"cardids":["6369"]}
        # )
        # print(result)
        # result = await mcp_app.call_tool(
        #     "query_personnel_list", {"start_time": "2026-05-25 00:00:00",
        #                              "end_time": "2026-05-28 23:59:59"}
        # )
        
        # print(result)
        print("\n4️⃣ trajectory")
        test_name = "王利明"  # ← 请改成你系统中真实存在的人员姓名！
        result = await mcp_app.call_tool("query_person_trajectory",
                                         {"name": test_name, "start_time": "2026-05-21 00:00:00",
                                          "end_time": "2026-05-28 23:59:59"})
        
        pprint(result)


        # # 7. 测试 get_infos - 多个参数全面测试
        # test_types = [
        #     {"type": "department", "desc": "部门列表"},
        #     {"type": "person", "desc": "人员列表"},
        #     {"type": "car", "desc": "车辆列表"},
        #     {"type": "worktype", "desc": "工种列表"},
        #     {"type": "area_limit", "desc": "区域人数上限"},
        #     {"type": "station", "desc": "区域点位"},
        #     {"type": "person", "name": "张", "desc": "模糊查找姓名中含'张'的人员"}
        # ]
        # for params in test_types:
        #     print(f"\n7️⃣ get_infos 测试 [{params.get('desc','')}] - 参数: {params}")
        #     result = await mcp_app.call_tool("get_infos", {k: v for k, v in params.items() if k in {"type", "name"}})
        #     # 只取一个数据进行打印
        #     # print(result)

        # print("\n8️⃣ 测试 query_car_underground_status (实时模式)")
        # try:
        #     result = await mcp_app.call_tool("query_car_underground_status", {"now_only": True})
        #     print("实时井下车辆分布：")
        #     print(result)
        # except Exception as e:
        #     print(f"query_car_underground_status (now_only=True) 异常: {e}")

        # print("\n9️⃣ 测试 query_car_underground_status (今日完整模式)")
        # try:
        #     result = await mcp_app.call_tool("query_car_underground_status", {"now_only": False})
        #     print("今日车辆分布统计：")
        #     print(result)
        # except Exception as e:
        #     print(f"query_car_underground_status (now_only=False) 异常: {e}")
        # print("\n🔟 测试 query_car_trajectory")
        # car_name = "9 号自行车 (瓦检)"  # ← 请改成你系统中真实存在的车辆名称！
        # car_id = '1059'
        # try:
        #     result = await mcp_app.call_tool(
        #         "query_car_trajectory",
        #         {
        #             "cardName": car_name,
        #             # "cardID": car_id,
        #             "start_time": "2026-05-07 00:00:00",
        #             "end_time": "2026-05-08 23:59:59"
        #         }
        #     )
        #     pprint(result)
        # except Exception as e:
        #     print(f"query_car_trajectory 测试异常: {e}")

        # 测试 query_cars_list 方法每个字段的情况
        # print("\n🅰️  测试 query_cars_list - 全字段覆盖")

        # 1. 测试只传cardids
        # params1 = {
        #     "cardids": ["12", "1059"],
        #     "start_time": "2026-04-21 00:00:00",
        #     "end_time": "2026-04-29 23:59:59"
        # }
        #
        # print("1️⃣ 只有 cardids:", await mcp_app.call_tool("query_cars_list", params1))

        # # 2. 只传car_names（模糊）
        # params2 = {
        #     "car_names": ["自行车"],
        #     "start_time": "2026-04-22 00:00:00",
        #     "end_time": "2026-04-24 23:59:59"
        # }
        # print("2️⃣ 只有 car_names:", await mcp_app.call_tool("query_cars_list", params2))

        # # 3. 只传electricitys
        # params3 = {
        #     "electricitys": ["低电量"],
        #     "start_time": "2026-04-22 00:00:00",
        #     "end_time": "2026-04-24 23:59:59"
        # }
        # print("3️⃣ 只有 electricitys:", await mcp_app.call_tool("query_cars_list", params3))

        # # 4. 只传areas
        # params4 = {
        #     "area_names": ["4-3煤","51204"]
        # }
        # print("4️⃣ 只有 area_names:", await mcp_app.call_tool("query_cars_list", params4))

        # # 5. 只传departments
        # params5 = {
        #     "departments": ["综采","通防"]
        # }
        # print("5️⃣ 只有 departments:", await mcp_app.call_tool("query_cars_list", params5))

        # # 6. 只传car_types
        # params6 = {
        #     "car_types": ["运输车","防爆"]
        # }
        # print("6️⃣ 只有 car_types:", await mcp_app.call_tool("query_cars_list", params6))

        # # 7. 只传start_time和end_time
        # params7 = {
        #     "start_time": "2026-04-23 00:00:00",
        #     "end_time": "2026-04-24 23:59:59"
        # }
        # print("7️⃣ 只有 start_time/end_time:", await mcp_app.call_tool("query_cars_list", params7))

        # # 8. 测试 query_person_near_station 工具（模糊名称，如“副井口”）
        # params8 = {
        #     "station_name": "51204综采工作面",
        #     "near_distance": 80
        # }
        # print("8️⃣ query_person_near_station:", await mcp_app.call_tool("query_person_near_station", params8))



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
