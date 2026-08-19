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
    
# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("MinePersonnelService")


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
        }

        try:
            self.client = clickhouse_connect.get_client(**self.db_config)
            logger.info("Successfully connected to ClickHouse.")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise

        self.table = "PS.HISTORY_PERSONNEL_LOCATION"
        self.api_url = "https://10.11.22.80:38443/apiaccess/api/syg/SingleDataFactotyWebHttp/getLocationWeb"

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
            - 若用户关注实时井下总人数、各区域分布及出入情况，请优先使用 `query_underground_status(now_only=True/False)`。
            - now_only=True 获取当前实时井下分布，now_only=False 获取今天全量的井下记录（含出入、排班等统计）。

            【2. 某个人员轨迹与分段】
            - 若需查询某个人在一天或多个时间段的详细活动轨迹（进出井、轨迹分段、停留区域），调用 `find_person_status(name, start_time, end_time)`。
            - 如果轨迹结果较多，返回内容将自动精简，仅保留核心区段及时间范围。
            - 若要确定查询的时间范围，建议先用 `get_system_time()` 获取当前时间基准。

            【3. 多条件批量人员过滤】
            - 若需要按照多字段（如姓名、区域、工种、班次、部门、时间区间等）灵活组合筛选，可调用 `query_personnel_list(names, areas, work_types, class_names, departments, start_time, end_time)`。
            - 该工具可获得所有符合条件的人员完整信息和各自详细的出入明细。

            【4. 查找最新入井记录】
            - 如需了解特定人员最新一次入井时间、班次、工种等基础信息，调用 `find_person_latest_entry(name)`。

            【5. 获取系统基准时间】
            - 当需要推算日期区间（如“本周”或“一天前”）可用 `get_system_time()` 获取服务当前时间作为参考。

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
        def query_underground_status(
            now_only: bool = False
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
            try:
                # 1. 获取实时井下人员
                real_time_list = self._fetch_realtime_api()

                # ==================== 实时模式 (now_only=True) ====================
                names = {}
                area_stats = {}
                for p in real_time_list:
                    name = p.get("name")
                    if not name:
                        continue
                    work_type = p.get("workType") or "未知"
                    class_time = p.get("classTimeName") or ""
                    department = p.get("department") or ""
                    area = p.get("areaName", "未知区域")
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
                
                return json.dumps(
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

            except Exception as e:
                logger.error(f"query_underground_status 异常: {e}", exc_info=True)
                return json.dumps(
                    {"error": "查询失败", "message": str(e)}, ensure_ascii=False
                )
                

        @self.mcp.tool()
        def find_person_trajectory(
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
                    'segments': [                     # 每个分段的明细
                        {
                            'areaName': '井下',                     # 区域名称
                            'classTimeName': '早班',                 # 班次名称
                            'mainStationId': '181',                 # 主站点ID
                            'segmentStartTime': '2026-04-01 09:09:31', # 分段开始时间
                            'segmentEndTime': '2026-04-01 14:36:31',   # 分段结束时间
                            'segmentDurationSeconds': 19620,          # 此段持续秒数
                            'areaChanges': [9.6, 102.2, 81.3, 90.1, 67.8]  # 过程中的距离变化（区域变动轨迹）
                        },
                        {...}， # 其他分段，以同样结构表示
                    ]
                }
            """
            try:
                if not name:
                    return json.dumps({"error": "必须提供 name 参数"}, ensure_ascii=False)
                
                real_time_list = self._fetch_realtime_api()

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
                    return json.dumps({
                        "message": f"未找到姓名 '{name}' 在指定时间范围内的记录"
                    }, ensure_ascii=False)
                
                # 给person_records加上"leave"字段，依据under_ground状态
                person_records["leave"] = "矿下" if under_ground else "出矿"
                json_str = json.dumps(person_records, ensure_ascii=False, separators=(",", ":"))
                if len(json_str) > 40000:
                    # 只保留核心精简内容
                    simple_segments = []
                    # 合并前后 areaName 相同的分段，合并时间区间
                    prev_seg = None
                    for seg in person_records.get("segments", []):
                        current_area = seg.get("areaName", "井下")
                        seg_start = seg.get("segmentStartTime")
                        seg_end = seg.get("segmentEndTime")
                        if not prev_seg:
                            prev_seg = {
                                "areaName": current_area,
                                "segmentStartTime": seg_start,
                                "segmentEndTime": seg_end
                            }
                        else:
                            if prev_seg["areaName"] == current_area:
                                # 合并区间
                                if seg_start and prev_seg["segmentEndTime"]:
                                    # 用当前片段的结束时间更新到最新
                                    prev_seg["segmentEndTime"] = seg_end
                            else:
                                simple_segments.append(prev_seg)
                                prev_seg = {
                                    "areaName": current_area,
                                    "segmentStartTime": seg_start,
                                    "segmentEndTime": seg_end
                                }
                    if prev_seg:  # 补上最后一段
                        simple_segments.append(prev_seg)
                    slim_data = {
                        "name": person_records.get("name"),
                        "start": person_records.get("start"),
                        "end": person_records.get("end"),
                        "department": person_records.get("department"),
                        "workType": person_records.get("workType"),
                        "job": person_records.get("job"),
                        "leave": "矿下" if  under_ground else "出矿",
                        "segments": simple_segments
                    }
                    str_json =  json.dumps(slim_data, ensure_ascii=False, separators=(",", ":"))
                    print('--------'*10,len(str_json))
                    return str_json
            
                return json.dumps(person_records, ensure_ascii=False, separators=(",", ":"))
                
            except Exception as e:
                logger.error(f"find_status 异常: {e}", exc_info=True)
                return json.dumps(
                    {"error": "查询失败", "message": str(e)}, 
                    ensure_ascii=False
                )
        
        @self.mcp.tool()
        def query_personnel_list(
            cardids:  list = None, 
            names: list = None, 
            electricitys: list = None, 
            areas: list = None, 
            work_types: list = None, 
            class_names: list = None, 
            departments: list = None,
            start_time: str = None, 
            end_time: str = None,
        ) -> str:
        
            """
            功能说明: 多条件综合查询人员名单。支持根据区域、班次、工种、部门及指定时间段进行灵活组合筛选，常见应用如查询“指定区域在某时段的所有人员”或“今日某班次出勤人员名单”。
            如果需要确定系统start_time 和 end_time 请先调用 get_system_time 函数
            
            参数说明:
                cardids (list, 可选): 需模糊匹配或精确匹配的工号列表，如 ["12345", "5678%"]，用于人员工号筛选，支持通配符 % 实现模糊查询。
                names (list, 可选): 需模糊匹配的人员姓名列表，如 ["张三", "李四"]，用于人员姓名筛选。
                electricitys (list, 可选): 电量状态筛选，仅支持 ["正常", "低电量"] 两种状态，低电量就是不正常的状态。
                areas (list, 可选): 需模糊匹配的区域名称列表，如 ["43204"]，用于筛选在指定区域出现过的人员。
                work_types (list, 可选): 工种名称列表，用于筛选如 ["电工", "掘进工"] 等特定工种的人员。
                class_names (list, 可选): 班次名称列表，仅支持["早班", "中班","夜班"]，用于筛选特定班次人员。
                departments (list, 可选): 部门名称列表，用于筛选所属部门人员。
                start_time (string, 可选): 开始时间，格式为 "YYYY-MM-DD HH:MM:SS"，不传则默认当天 00:00:00。
                end_time (string, 可选): 结束时间，格式为 "YYYY-MM-DD HH:MM:SS"，不传则默认当天 23:59:59。

            返回值:
            返回一个包含所有符合条件的、去重的人员详细信息字典，其中包括姓名、部门、工种、班次及每人的详细记录。例如:
                {
                    "total": 1,  # 总分段数（总共找到几段出入记录）
                    "update_time": "2026-04-02 20:35:00",  # 返回数据的更新时间（通常为查询时刻）
                    "persons": {  # 所有符合条件的人员字典（按姓名分组，每个姓名一个键）
                        "张三": {  # 某个人员姓名
                            "cardid": "6285"   #卡号
                            "workType": "电工",  # 工种
                            "classTimeName": "早班",  # 班次名称
                            "department": "机电队",  # 部门
                            "count":26,  # 人员累计出入次数
                            "now_work": "矿下"/"出矿",  # 现在的工作状态 
                            "records": [  # 本人每条出入记录组成的列表
                                {
                                    "areaName": "中央变电所",  # 所在区域
                                    "enterTime": "2026-04-02 08:30:00",  # 进入时间
                                    "leaveTime": "2026-04-02 16:45:00",  # 离开时间
                                    "electricity": "正常"                # 电量
                                }
                            ]
                        }
                        # ... 其他人员继续在此增加
                        {...}
                    }
                }
            若数据量较大（超过15000字符），自动进行字段压缩仅保留主要统计信息及 uniqueAreas 字段例如
            {
                    "total": 1,  # 总分段数（记录总计/人数分段总共多少条）
                    "update_time": "2026-04-02 20:35:00",  # 返回数据的时间戳（一般为查询结果生成时间）
                    "persons": {
                        "张三": {  # 人员姓名（以姓名为字典key）
                            "workType": "电工",   # 工种
                            "classTimeName": "早班",  # 班次名
                            "department": "机电队",  # 部门名
                            "uniqueAreas":["井下"],  # 历史出现过的所有唯一区域名去重列表
                            "now_work": "矿下"/"出矿",  # 现在的工作状态 
                            "totalEnterTime":"2026-03-01 08:43:02.634000+08:00",  # 最早进入时间（该人的首段记录时间）
                            "totalExitTime":"2026-04-02 13:26:22.819000+08:00",   # 最晚离开时间（该人的末段记录时间）
                            "count":26  # 满足条件的该人员累计出入区域次数
                        }
                        # ... 其他人员同理，用 {...} 表示
                    }
                    }
                }
            """
            try:
                # 1. 执行核心查询
                person_records = self.get_persons_by_filters(
                    cardids = cardids,
                    names=names,
                    areas=areas,
                    electricitys= electricitys,
                    work_types=work_types,
                    class_names=class_names,
                    departments=departments,
                    start_time=start_time,
                    end_time=end_time,
                )
                
                if not person_records or not person_records.get("persons"):
                    return json.dumps({"message": "未找到符合条件的人员记录"}, ensure_ascii=False)

                real_time_list = self._fetch_realtime_api()

                # ==================== 实时模式 (now_only=True) ====================
                names = []
                for p in real_time_list:
                    name_real = p.get("name")
                    names.append(name_real)
                    
                # 2. 预估长度，设定阈值（例如超过 15000 字符进行精简）
                json_full = json.dumps(person_records, ensure_ascii=False, separators=(",", ":"))
                
                if len(json_full) > 40000:
                    # --- 精简逻辑: 压缩只保留主要统计字段与去重的 uniqueAreas ---
                    compact_persons = {}
                    for name, info in person_records["persons"].items():
                        # 只保留每条记录的 leaveTime 字段用于统计 uniqueAreas
                        unique_areas = list(set(r["areaName"] for r in info["records"]))
                        
                        # 统计所有进入与离开时间，便于展示总跨度
                        all_enters = [r["enterTime"] for r in info["records"]]
                        all_exits = [r["leaveTime"] for r in info["records"]]
                        if name in names:
                            now_work = "矿下"
                        else:
                            now_work = "出矿"
                            
                        compact_persons[name] = {
                            "workType": info["workType"],
                            "classTimeName": info["classTimeName"],
                            "department": info["department"],
                            "uniqueAreas": unique_areas,
                            "now_work": now_work,
                            "totalEnterTime": min(all_enters) if all_enters else "",
                            "totalExitTime": max(all_exits) if all_exits else "",
                            "count": len(info["records"])
                        }
                    person_records["persons"] = compact_persons
                    person_records["is_compacted"] = True # 标记为精简内容
                    # --- 精简逻辑结束 ---
                # 给没有超过阈值（即没有走精简逻辑）的每个人员也加一个count字段（为 records 长度）
                else:
                    for name, info in person_records.get("persons", {}).items():
                        if name in names:
                            now_work = "矿下"
                        else:
                            now_work = "出矿"
                        if "records" in info:
                            info['now_work'] = now_work
                            info["count"] = len(info["records"])
                return json.dumps(person_records, ensure_ascii=False, separators=(",", ":"))
                
            except Exception as e:
                print( "traceback",traceback.format_exc())
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
            try:
                if not name:
                    return json.dumps({"error": "参数 name 不能为空"}, ensure_ascii=False)

                result = self.get_person_latest(name,cardid)
                # get_person_latest 要么返回 {"success": False, ...}，要么返回 {"success": True, ...}
                if not result or not result.get("success", False):
                    return json.dumps({"message": f"未找到姓名 '{name}' 的最近入井记录"}, ensure_ascii=False)
                r = result

                ret = {
                    "name": r.get("name", name),
                    "cardId":r.get("cardId", name),
                    "enterTime": r.get("enterTime", ""),
                    "areaName": r.get("areaName", ""),
                    "department": r.get("department", ""),
                    "workType": r.get("workType", ""),
                    "classTimeName": r.get("classTimeName", ""),
                    "other": {k: v for k, v in r.items() if k not in (
                        "name", "enterTime", "areaName", "department", "workType", "classTimeName", "success"
                    )}
                }
                return json.dumps(ret, ensure_ascii=False, separators=(",", ":"))
            except Exception as e:
                import traceback
                print( "traceback-----------------",traceback.format_exc())
                return json.dumps({
                    "error": "查询失败",
                    "message": str(e),
                    "traceback": traceback.format_exc()
                }, ensure_ascii=False)
            

    # ==================== 4. 内部辅助逻辑 ====================
    def _fetch_realtime_api(self) -> List[Dict]:
        try:
            headers = {"Content-Type": "application/json"}
            resp = requests.post(
                self.api_url,
                json={"mineCode": ""},
                headers=headers,
                verify=False,
                timeout=8,
            )
            return resp.json().get("data", []) if resp.status_code == 200 else []
        except Exception as e:
            logger.error(f"API Error: {e}")
            return []

    def _format_time(self, t: Optional[str]) -> str:
        if not t:
            return datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        clean = re.sub(r"T", " ", t).split(".")[0]
        return clean

    def _match_filters(self, item: Dict, filters: Dict) -> bool:
        """简单过滤匹配"""
        for key, value in filters.items():
            if value and str(item.get(key, "")).lower() != str(value).lower():
                return False
        return True

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
    ):
        """
        支持全字段模糊查询。
        如果不传 start_time/end_time，默认查询当日数据。
        cardids 支持精确或模糊。
        electricitys 仅支持 ['正常', '低电量'] 这两种状态过滤。
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

        # 3.3 定义模糊查询字段映射
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
        query = f"""
        SELECT 
            NAME,                                      -- 0
            toDate(UPDATE_TIME) AS recordDate,         -- 1
            any(WORKTYPE),                             -- 2
            any(CLASSTIMENAME),                        -- 3
            any(DEPARTMENT),                           -- 4
            any(AREANAME),                             -- 5
            min(UPDATE_TIME) AS enterTime,             -- 6
            max(UPDATE_TIME) AS areaTime,              -- 7
            max(UPDATE_TIME) AS timestamp,             -- 8
            any(CARDID),                               -- 9  (新增：工号)
            any(ELECTRICITY)                           -- 10 (新增：电量)
        FROM PS.HISTORY_PERSONNEL_LOCATION
        WHERE {where_sql}
        GROUP BY 
            NAME, 
            recordDate
        ORDER BY recordDate DESC, enterTime DESC
        """

        try:
            result = self.client.query(query, parameters=params)

            persons_dict = {}
            total_segments = 0

            for row in result.result_rows:
                name = row[0]

                # 【修改点 2】：同一个人只记录一次基础信息 (增加 cardid)
                if name not in persons_dict:
                    persons_dict[name] = {
                        "cardid": row[9],              # 提取索引 9 的工号数据
                        "workType": row[2],
                        "classTimeName": row[3],
                        "department": row[4],
                        "records": [],
                    }

                # 【修改点 3】：记录具体的时间段信息 (增加 electricity)
                record = {
                    "areaName": row[5],
                    "enterTime": str(row[6]),
                    "leaveTime": str(row[7]),
                    "electricity": row[10],            # 提取索引 10 的电量数据
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

        query = f"""
        SELECT
            NAME, DEPARTMENT, CLASSTIMENAME, DUTYNAME, WORKTYPE,
            AREANAME, MAINSTATIONTIME, MAINSTATIONDISTANCE,
            SUBSTATIONTIME, SUBSTATIONDISTANCE, AREATIME,
            ENTERTIME, UPDATE_TIME, CARDID
        FROM PS.HISTORY_PERSONNEL_LOCATION
        {where_clause}
        ORDER BY UPDATE_TIME DESC
        LIMIT 1
        """

        result = self.client.query(query, parameters=parameters).result_rows

        if not result:
            return {
                "success": False,
                "message": f"未找到该人员的记录（{'CARDID: ' + cardid if cardid else 'NAME: ' + name}）"
            }

        row = result[0]

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
            "subStationTime": str(row[8]) if row[8] is not None else None,
            "subStationDistance": row[9],
            "areaTime": str(row[10]) if row[10] is not None else None,
            "enterTime": str(row[11]) if row[11] is not None else None,
            "updateTime": str(row[12]) if row[12] is not None else None,
            "cardId": row[13],          # 新增返回 CARDID
        }

        
    def get_persons_latest(self, names: Union[str, List[str]]) -> Dict[str, Optional[Dict]]:
        if isinstance(names, str):
            names = [names]
        
        name_list = list(dict.fromkeys([n.strip() for n in names if n]))
        if not name_list:
            return {}

        query = """
            SELECT 
                NAME,
                argMax(DEPARTMENT, UPDATE_TIME),
                argMax(CLASSTIMENAME, UPDATE_TIME),
                argMax(DUTYNAME, UPDATE_TIME),
                argMax(WORKTYPE, UPDATE_TIME),
                argMax(AREANAME, UPDATE_TIME),
                argMax(MAINSTATIONTIME, UPDATE_TIME),
                argMax(MAINSTATIONDISTANCE, UPDATE_TIME),
                argMax(SUBSTATIONTIME, UPDATE_TIME),
                argMax(SUBSTATIONDISTANCE, UPDATE_TIME),
                argMax(AREATIME, UPDATE_TIME),
                argMax(ENTERTIME, UPDATE_TIME),
                argMax(UPDATE_TIME, UPDATE_TIME) AS latest_update
            FROM PS.HISTORY_PERSONNEL_LOCATION
            WHERE NAME IN %(names)s 
            AND UPDATE_TIME >= today()
            AND UPDATE_TIME < today() + 1
            GROUP BY NAME
        """
        
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
    
    def get_time_stats(self,time_changes: List[datetime]) -> Dict:
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
        if not time_changes or len(time_changes) == 0:
            return {
                "earliest": None,
                "latest": None,
                "duration_seconds": 0,
                "earliest_dt": None,
                "latest_dt": None
            }
        
        # 找出最早和最新时间
        earliest_dt = min(time_changes)
        latest_dt = max(time_changes)
        
        # 计算时间差（秒）
        duration_seconds = int((latest_dt - earliest_dt).total_seconds())
        
        # 转为易读的字符串格式（推荐格式）
        earliest_str = earliest_dt.strftime("%Y-%m-%d %H:%M:%S")
        latest_str = latest_dt.strftime("%Y-%m-%d %H:%M:%S")
        
        return {
            "earliest": earliest_str,
            "latest": latest_str,
            "duration_seconds": duration_seconds,
            "earliest_dt": earliest_dt,
            "latest_dt": latest_dt
        }
        
    def get_person_trajectory_with_stay(self, name: str, start_time: str, end_time: str):
        query = """
        WITH base AS (
            SELECT 
                NAME, AREANAME, CLASSTIMENAME, MAINSTATIONID, MAINSTATIONDISTANCE,
                DEPARTMENT, WORKTYPE, JOB,
                toDateTime(MAINSTATIONTIME) AS m_time,
                toDateTime(UPDATE_TIME) AS u_time,
                toDate(MAINSTATIONTIME) AS stationDate
            FROM PS.HISTORY_PERSONNEL_LOCATION
            WHERE NAME = %(name)s
              AND UPDATE_TIME >= %(start)s
              AND UPDATE_TIME < %(end)s
            ORDER BY u_time ASC  -- 第一层排序：确保后续窗口函数逻辑正确
        ),
        -- 步骤1：获取前一行的值
        prev_values AS (
            SELECT 
                u_time,
                MAINSTATIONID,
                stationDate,
                lagInFrame(MAINSTATIONID) OVER (ORDER BY u_time ASC) AS prev_id,
                lagInFrame(stationDate) OVER (ORDER BY u_time ASC) AS prev_date
            FROM base
        ),
        -- 步骤2：计算变化标记 (Gap and Islands)
        flag_change AS (
            SELECT 
                u_time,
                if(MAINSTATIONID != prev_id OR stationDate != prev_date, 1, 0) AS is_change
            FROM prev_values
        ),
        -- 步骤3：生成段 ID
        grouping_id AS (
            SELECT 
                u_time,
                sum(is_change) OVER (ORDER BY u_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS segment_id
            FROM flag_change
        )
        -- 步骤4：聚合。关键点：使用 groupArray 并依赖外部排序来保证 areaChanges 的时间顺序
        SELECT 
            any(b.NAME),                                      -- 0
            any(b.AREANAME),                                  -- 1
            any(b.CLASSTIMENAME),                             -- 2
            any(b.MAINSTATIONID),                             -- 3
            any(b.stationDate),                               -- 4
            min(b.m_time),                                    -- 5
            max(b.m_time),                                    -- 6
            count(*),                                         -- 7
            dateDiff('second', min(b.m_time), max(b.m_time)), -- 8
            -- 使用 groupArray 保证顺序，arrayCompact 去除相邻重复值（如 10, 10, 20 -> 10, 20）
            arrayCompact(groupArray(b.MAINSTATIONDISTANCE)) AS distChanges, -- 9
            any(b.DEPARTMENT),                                -- 10
            any(b.WORKTYPE),                                  -- 11
            any(b.JOB),                                       -- 12
            groupArray(b.u_time)                              -- 13
        FROM base b
        INNER JOIN grouping_id g ON b.u_time = g.u_time
        GROUP BY segment_id
        ORDER BY min(b.u_time) ASC
        """

        try:
            result = self.client.query(query, parameters={
                "name": name,
                "start": start_time,
                "end": end_time
            })

            segments = []
            dept, w_type, job_title = "", "", ""

            for row in result.result_rows:
                dept, w_type, job_title = row[10], row[11], row[12]
                
                # 获取该段内所有的 u_time，计算统计值
                time_list = list(row[13])
                stats = self.get_time_stats(time_list)

                segments.append({
                    "areaName": row[1],
                    "classTimeName": row[2],
                    "mainStationId": row[3],
                    "segmentStartTime": stats["earliest"],
                    "segmentEndTime": stats["latest"],
                    "segmentDurationSeconds": stats["duration_seconds"],
                    # row[9] 已经是按时间排序且去除了相邻重复的距离列表
                    "areaChanges": [str(d) for d in list(row[9])],
                    "recordCount": int(row[7])
                })

            return {
                "name": name,
                "start": start_time,
                "end": end_time,
                "department": dept,
                "workType": w_type,
                "job": job_title,
                "total_segments": len(segments),
                "segments": segments
            }
            
        except Exception as e:
            import traceback
            print(f"分段轨迹查询失败: {e}\n{traceback.format_exc()}")
            return {"error": "查询失败", "message": str(e)}
   
    def get_today_persons(self) -> List[str]:
        """获取今天出现过的人员名单（去重）"""
        query = """
        SELECT DISTINCT NAME
        FROM PS.HISTORY_PERSONNEL_LOCATION
        WHERE UPDATE_TIME >= today()
          AND UPDATE_TIME < today() + 1
        """

        result = self.client.query(query).result_rows
        return [row[0] for row in result]

    def _build_person_dict(self, data: Dict, detailed: bool, status: str) -> Dict:
        """统一构建返回结构"""
        if detailed:
            return {
                "name": data.get("name"),
                "department": data.get("department"),
                "classTimeName": data.get("classTimeName"),
                "dutyName": data.get("dutyName"),
                "workType": data.get("workType"),
                "areaName": data.get("areaName"),
                "mainStationTime": data.get("mainStationTime"),
                "mainStationDistance": data.get("mainStationDistance"),
                "subStationTime": data.get("subStationTime"),
                "subStationDistance": data.get("subStationDistance"),
                "areaTime": data.get("areaTime"),
                "enterTime": data.get("enterTime"),
                "status_text": "井下" if status == "underground" else "已出井",
            }
        else:

            return {
                "name": data.get("name"),
                "job": data.get("workType"),
                "areaName": data.get("areaName"),
                "enterTime": data.get("enterTime"),
                "status_text": "井下" if status == "underground" else "已出井",
            }


import asyncio
import json


async def test_all_tools():
    print("🔥 开始测试 MinePersonnelService 的所有 MCP Tools\n" + "=" * 60)

    try:
        # 1. 系统时间
        print("\n1️⃣ get_system_time")
        result = await mcp_app.call_tool("get_system_time", {})
        print(result)

        # 2. 实时井下状态（最常用）
        print("\n2️⃣ query_underground_status (实时模式)")
        result = await mcp_app.call_tool("query_underground_status", {"now_only": True})
        print(result)

        # 3. 今日完整模式
        print("\n3️⃣ query_underground_status (今日完整模式)")
        result = await mcp_app.call_tool(
            "query_underground_status", {"now_only": False}
        )
        print(result)
        # 4. 特定人员查询
        print("\n4️⃣ trajectory")
        test_name = "陈玉岭"  # ← 请改成你系统中真实存在的人员姓名！
        result = await mcp_app.call_tool("find_person_trajectory", {"name": test_name,"start_time":"2026-04-01 08:00:00",
        "end_time":"2026-04-02 18:00:00"})
        print(result)

        # 5.多项状态查询
        print("\n6️⃣ query_personnel_list")
        result = await mcp_app.call_tool(
            "query_personnel_list", {"cardids":["6033",'6668'],"start_time":"2026-01-31 08:00:00",
        "end_time":"2026-04-02 18:00:00"}
        )
        
        print(result)
        
        # 6. 测试 find_person_latest_entry
        print("\n7️⃣ find_person_latest_entry")
        result = await mcp_app.call_tool("find_person_latest_entry", {"name": test_name})
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
