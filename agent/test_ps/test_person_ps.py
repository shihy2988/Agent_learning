import traceback
import clickhouse_connect
from datetime import datetime, time
import traceback
from datetime import datetime
from typing import List, Dict, Optional

class PersonnelService:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host="10.11.22.80",
            port=9120,
            username="nethouse",
            password="CGC%EVXr.ET10Y_N",
            secure=True,
            verify=False,
        )

    # ✅ 1. 查询某人最新位置
    # def get_person_latest(self, name):
    #     query = """
    #     SELECT NAME, AREANAME, UPDATE_TIME
    #     FROM PS.HISTORY_PERSONNEL_LOCATION
    #     WHERE NAME = %(name)s
    #     ORDER BY UPDATE_TIME DESC
    #     LIMIT 1
    #     """
    #     return self.client.query(query, parameters={"name": name}).result_rows

    # ✅ 2. 查询当天人员（去重）
    def get_today_persons(self):
        query = """
        SELECT DISTINCT NAME
        FROM PS.HISTORY_PERSONNEL_LOCATION
        WHERE UPDATE_TIME >= today()
          AND UPDATE_TIME < today() + 1
        """
        return [row[0] for row in self.client.query(query).result_rows]

    # ✅ 3. 查询当天每个人最新状态（实时井下）
    def get_today_latest(self):
        query = """
        SELECT NAME, AREANAME, UPDATE_TIME
        FROM
        (
            SELECT *,
                   row_number() OVER (PARTITION BY NAME ORDER BY UPDATE_TIME DESC) AS rn
            FROM PS.HISTORY_PERSONNEL_LOCATION
            WHERE UPDATE_TIME >= today()
              AND UPDATE_TIME < today() + 1
        )
        WHERE rn = 1
        """
        return self.client.query(query).result_rows

    # ✅ 4. 查询某区域某时间段人员
    def get_persons_by_filters(
        self,
        cardids: list = None,  # 工号
        names: list = None,
        electricitys: list = None,  # 电量：['正常', '低电量', '其他']
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
        electricitys 仅支持 ['正常', '低电量', '其他'] 这三种状态过滤。
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
                    cardid_clauses.append(f"toString(CARDID) LIKE %({key})s")
                    params[key] = cid
                else:
                    cardid_clauses.append(f"toString(CARDID) = %({key})s")
                    params[key] = str(cid)

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
            NAME,
            toDate(UPDATE_TIME) AS recordDate,
            any(WORKTYPE),
            any(CLASSTIMENAME),
            any(DEPARTMENT),
            any(AREANAME),
            min(UPDATE_TIME) AS enterTime,
            max(UPDATE_TIME) AS areaTime,
            max(UPDATE_TIME) AS timestamp,
            CARDID,
            any(ELECTRICITY)
        FROM PS.HISTORY_PERSONNEL_LOCATION
        WHERE {where_sql}
        GROUP BY 
            NAME,
            recordDate,
            CARDID
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
                    "areaTime": str(row[7]),
                    "status": "井下",
                    "timestamp": str(row[8]),
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

    def get_field_unique_values(self, fields: list = None):
        """
        查询指定字段的独特值（去重），通常用于前端初始化下拉筛选框。
        如果不传 fields，默认返回最常用的分类字段选项。
        """
        # 1. 定义允许查询的字段白名单（已排除时间字段 AREATIME, ENTERTIME, UPDATETIME, MAINSTATIONTIME, SUBSTATIONTIME）
        # 将允许的字段统一定义为大写，便于校验
        ALLOWED_FIELDS = {
            "CARDID", "NAME", "ELECTRICITY", "DEPARTMENT", "CLASSTIMENAME", 
            "DUTYNAME", "WORKTYPE", "MAINSTATIONID", "MAINSTATIONDISTANCE", 
            "SUBSTATIONID", "SUBSTATIONDISTANCE", "IDENTITYARD", 
            "AREANAME", "STATEID"
        }

        # 2. 如果前端没有指定字段，则默认查询这些最具有分类意义的字段
        if not fields:
            fields = [
                "CARDID", "NAME", "ELECTRICITY", "DEPARTMENT", "CLASSTIMENAME", 
                "DUTYNAME", "WORKTYPE", "MAINSTATIONID",  
                "SUBSTATIONID", "AREANAME", "STATEID"
            ]

        result_dict = {}

        # 3. 循环遍历字段进行查询
        for field in fields:
            field_upper = str(field).upper()
            
            # 安全校验：防止 SQL 注入或查询不支持的时间字段
            if field_upper not in ALLOWED_FIELDS:
                print(f"警告: 字段 '{field}' 不在允许的查询白名单中或属于时间字段，已跳过。")
                continue

            # 构建查询语句：过滤掉 NULL 和空字符串，限制最大返回数量防止前端卡死
            query = f"""
            SELECT DISTINCT {field_upper} 
            FROM PS.HISTORY_PERSONNEL_LOCATION 
            WHERE {field_upper} IS NOT NULL AND {field_upper} != ''
            LIMIT 1000
            """

            try:
                # 假设使用的是 ClickHouse 客户端 (self.client)
                result = self.client.query(query)
                
                # result_rows 是包含元组的列表，例如 [('掘进队',), ('采煤队',), ...]
                # 提取第一个元素变成一维列表
                unique_values = [row[0] for row in result.result_rows]
                
                result_dict[field_upper] = unique_values

            except Exception as e:
                import traceback
                print(f"查询字段 {field_upper} 独特值失败: {e}\n{traceback.format_exc()}")
                result_dict[field_upper] = []

        return {
            "code": 200,
            "message": "success",
            "data": result_dict
        }
    
    
    # ✅ 5. 查询区域详细轨迹
    def get_area_records(self, area, start, end):
        query = """
        SELECT NAME, AREANAME, UPDATE_TIME
        FROM PS.HISTORY_PERSONNEL_LOCATION
        WHERE AREANAME LIKE %(area)s
        AND UPDATE_TIME >= %(start)s
        AND UPDATE_TIME < %(end)s
        ORDER BY UPDATE_TIME
        """
        return self.client.query(
            query, parameters={"area": f"%{area}%", "start": start, "end": end}
        ).result_rows

    # ✅ 6. 查询班次人员
    def get_class_persons(self, class_name, start, end):
        query = """
        SELECT DISTINCT NAME
        FROM PS.HISTORY_PERSONNEL_LOCATION
        WHERE CLASSTIMENAME = %(class_name)s
          AND UPDATE_TIME >= %(start)s
          AND UPDATE_TIME < %(end)s
        """
        return [
            row[0]
            for row in self.client.query(
                query, parameters={"class_name": class_name, "start": start, "end": end}
            ).result_rows
        ]

    # ✅ 7. 查询班次统计
    def get_class_stats(self, start, end):
        query = """
        SELECT CLASSTIMENAME, count(DISTINCT NAME)
        FROM PS.HISTORY_PERSONNEL_LOCATION
        WHERE UPDATE_TIME >= %(start)s
          AND UPDATE_TIME < %(end)s
        GROUP BY CLASSTIMENAME
        """
        return self.client.query(
            query, parameters={"start": start, "end": end}
        ).result_rows

    # ✅ 8. 查询某区域人数分布
    def get_area_distribution(self):
        query = """
        SELECT AREANAME, count(DISTINCT NAME) AS num
        FROM
        (
            SELECT *,
                   row_number() OVER (PARTITION BY NAME ORDER BY UPDATE_TIME DESC) AS rn
            FROM PS.HISTORY_PERSONNEL_LOCATION
        )
        WHERE rn = 1
        GROUP BY AREANAME
        ORDER BY num DESC
        """
        return self.client.query(query).result_rows



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
        if not time_changes or len(time_changes) == 0:
            return {
                "earliest": None,
                "latest": None,
                "duration_seconds": 0,
                "earliest_dt": None,
                "latest_dt": None,
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
            "latest_dt": latest_dt,
        }

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
        
        
    def get_person_trajectory_with_stay(self, name: str, start: str, end: str):
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
            result = self.client.query(
                query, parameters={"name": name, "start": start, "end": end}
            )

            segments = []
            dept, w_type, job_title = "", "", ""

            for row in result.result_rows:
                dept, w_type, job_title = row[10], row[11], row[12]

                # 获取该段内所有的 u_time，计算统计值
                time_list = list(row[13])
                stats = self.get_time_stats(time_list)

                segments.append(
                    {
                        "name": row[0],
                        "areaName": row[1],
                        "classTimeName": row[2],
                        "mainStationId": row[3],
                        "segmentStartTime": stats["earliest"],
                        "segmentEndTime": stats["latest"],
                        "segmentDurationSeconds": stats["duration_seconds"],
                        # row[9] 已经是按时间排序且去除了相邻重复的距离列表
                        "areaChanges": [str(d) for d in list(row[9])],
                        "recordCount": int(row[7]),
                    }
                )

            return {
                "name": name,
                "start": start,
                "end": end,
                "department": dept,
                "workType": w_type,
                "job": job_title,
                "total_segments": len(segments),
                "segments": segments,
            }

        except Exception as e:
            import traceback

            print(f"分段轨迹查询失败: {e}\n{traceback.format_exc()}")
            return {"error": "查询失败", "message": str(e)}

    # ✅ 9. 查询求救人员
    def get_help_persons(self):
        query = """
        SELECT NAME, AREANAME, UPDATE_TIME
        FROM
        (
            SELECT *,
                   row_number() OVER (PARTITION BY NAME ORDER BY UPDATE_TIME DESC) AS rn
            FROM PS.HISTORY_PERSONNEL_LOCATION
        )
        WHERE rn = 1
          AND STATEID LIKE '%10%'
        """
        return self.client.query(query).result_rows

    # ✅ 10. 查询长时间未移动（示例：30分钟）
    def get_static_persons(self, start, end, threshold_seconds=1800):
        """
        查询某时间段内 长时间未移动人员
        threshold_seconds: 静止阈值（默认30分钟）
        """
        query = """
        SELECT
            NAME,
            max(UPDATE_TIME) AS last_time,
            dateDiff('second', max(UPDATE_TIME), now()) AS idle_seconds
        FROM PS.HISTORY_PERSONNEL_LOCATION
        WHERE UPDATE_TIME >= %(start)s
        AND UPDATE_TIME < %(end)s
        GROUP BY NAME
        HAVING idle_seconds > %(threshold)s
        ORDER BY idle_seconds DESC
        """

        result = self.client.query(
            query,
            parameters={"start": start, "end": end, "threshold": threshold_seconds},
        )

        return result.result_rows

    def close(self):
        self.client.close()


if __name__ == "__main__":
    
    import pprint

    service = PersonnelService()

    # print("\n===== 1. 最新位置 =====")
    result = service.get_person_latest(cardid="6285")
    # print(result)
    pprint.pprint(result, sort_dicts=False, width=120)
    # print("\n===== 2. 今日人员 =====")
    # result = service.get_today_persons()
    # print(f"人数: {len(result)}")
    # print(result[:10])

    # print("\n===== 3. 今日每人最新状态 =====")
    # result = service.get_today_latest()
    # print(f"人数: {len(result)}")
    # print(result[:5])

    print("\n===== 4. 某区域人员（回风） =====")
    result = service.get_persons_by_filters(
        cardids=["6668"],
        start_time="2026-04-01 08:00:00",
        end_time="2026-04-02 18:00:00",
    )
    print(f"人数: {len(result)}")

    # print("\n===== 5. 区域轨迹记录 =====")
    # result = service.get_area_records(
    #     "回风",
    #     "2026-02-01 08:00:00",
    #     "2026-04-01 12:00:00"
    # )
    # print(f"记录数: {len(result)}")
    # print(result[:5])

    # print("\n===== 6. 班次人员 =====")
    # result = service.get_class_persons(
    #     "早班",
    #     "2026-04-01 00:00:00",
    #     "2026-04-02 00:00:00"
    # )
    # print(f"人数: {len(result)}")
    # print(result)

    # print("\n===== 7. 班次统计 =====")
    # result = service.get_class_stats(
    #     "2026-04-01 00:00:00",
    #     "2026-04-02 00:00:00"
    # )
    # print(result)

    # print("\n===== 8. 区域人数分布 =====")
    # result = service.get_area_distribution()
    # print(result[:10])

    # print("\n===== 9. 人员轨迹 + 停留时间 =====")
    # result = service.get_person_trajectory_with_stay(
    #     "石小龙",
    #     "2026-04-01 08:00:00",
    #     "2026-04-02 18:00:00"
    # )

    pprint.pprint(result, sort_dicts=False, width=120)
    # result = service.get_field_unique_values()
    # pprint.pprint(result, sort_dicts=False, width=120)
    
    # print("\n===== 10. 求救人员 =====")
    # result = service.get_help_persons()
    # print(result)

    # print("\n===== 11. 长时间未移动人员 =====")
    # result = service.get_static_persons("2026-02-01 08:00:00",
    #     "2026-04-01 18:00:00")
    # print(result)

    service.close()
