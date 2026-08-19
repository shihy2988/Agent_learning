# -*- coding: utf-8 -*-
'''
@File    : __init__.py
@IDE     : PyCharm
@Author  : shihongyu
@Date    : 2025/09/01
@Describe: 日志配置模块的初始化文件
'''
from .person_tool import  PersonnelMCPService


import re
from datetime import datetime, time, timedelta
from typing import Optional


class PersonnelLocationService:
    # 分段最大允许时间间隙（分钟），超过此间隙即使在同一基站也视为新分段
    SEGMENT_GAP_MINUTES = 30

    def __init__(self, client):
        self.client = client
        self.station_names = {}
        self._station_name_pattern_cache = {}

    def _init_station_names(self):
        """初始化站点名称映射，仅在首次调用时加载"""
        if self.station_names:
            return
        try:
            result = self.client.query(
                "SELECT STATIONID, STATIONNAME FROM PS.DIM_STATION"
            )
            for row in result.result_rows:
                sid, sname = str(row[0]), row[1] or ""
                self.station_names[sid] = {"name": sname}
                # 预编译模糊匹配正则（忽略大小写，支持部分匹配）
                if sname:
                    escaped = re.escape(sname)
                    self._station_name_pattern_cache[sid] = re.compile(
                        escaped, re.IGNORECASE
                    )
        except Exception as e:
            print(f"[WARN] 加载站点名称失败: {e}")

    @staticmethod
    def _escape_like(value: str) -> str:
        """转义 LIKE 特殊字符，防止注入和意外匹配"""
        return value.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")

    def _match_station_ids(self, keywords: list, station_type: str = "main") -> list:
        """通过关键词模糊匹配站点ID列表"""
        if not keywords:
            return []
        matched = set()
        for kw in keywords:
            kw_clean = str(kw).strip()
            if not kw_clean:
                continue
            for sid, pattern in self._station_name_pattern_cache.items():
                if pattern.search(kw_clean):
                    matched.add(sid)
        return list(matched)

    def get_persons_by_filters(
        self,
        cardids: list = None,
        names: list = None,
        electricitys: list = None,
        areas: list = None,
        work_types: list = None,
        class_names: list = None,
        departments: list = None,
        start_time: str = None,
        end_time: str = None,
        main_stations: list = None,
        sub_stations: list = None,
    ):
        """
        支持全字段模糊查询的人员轨迹分段检索。
        - 不传日期默认查当日全天
        - 强制最大查询跨度7天，防止OOM
        - leaveTime 取 UPDATE_TIME 最大值，保证时间最新
        - 同基站停留超过30分钟自动断开分段
        """
        # ==================== 1. 时间范围处理 ====================
        now = datetime.now()
        if not start_time:
            start_time = datetime.combine(now.date(), time.min).strftime("%Y-%m-%d %H:%M:%S")
        if not end_time:
            end_time = datetime.combine(now.date(), time.max).strftime("%Y-%m-%d %H:%M:%S")

        # 安全检查：防止全表扫描导致 OOM
        st_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        et_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
        if (et_dt - st_dt).days > 7:
            return {
                "total": 0, "update_time": "", "persons": {},
                "error": "查询时间跨度不能超过7天，请缩小范围"
            }

        # ==================== 2. 站点模糊匹配 ====================
        self._init_station_names()
        matched_main_ids = self._match_station_ids(main_stations)
        matched_sub_ids = self._match_station_ids(sub_stations)

        # ==================== 3. 构建 WHERE 条件 ====================
        where_clauses = [
            "UPDATE_TIME >= %(start_time)s",
            "UPDATE_TIME < %(end_time)s",
        ]
        params = {"start_time": start_time, "end_time": end_time}

        # 3.1 cardids
        if cardids:
            cid_parts = []
            for i, cid in enumerate(cardids):
                key = f"cardid_{i}"
                raw = str(cid).strip()
                if "%" in raw and not raw.startswith("%"):
                    params[key] = raw
                else:
                    params[key] = f"%{self._escape_like(raw)}%"
                cid_parts.append(f"CARDID LIKE %({key})s")
            where_clauses.append(f"({' OR '.join(cid_parts)})")

        # 3.2 electricitys
        if electricitys:
            statuses = {str(x) for x in electricitys}
            elec_parts = []
            normal_or_low = [s for s in ("正常", "低电量") if s in statuses]
            if normal_or_low:
                placeholders = []
                for idx, val in enumerate(normal_or_low):
                    k = f"elec_{idx}"
                    params[k] = val
                    placeholders.append(f"%({k})s")
                elec_parts.append(f"ELECTRICITY IN ({', '.join(placeholders)})")
            if "其他" in statuses:
                elec_parts.append(
                    "ELECTRICITY NOT IN (%(en)s, %(el)s)"
                )
                params["en"] = "正常"
                params["el"] = "低电量"
            if elec_parts:
                where_clauses.append(f"({' OR '.join(elec_parts)})")

        # 3.3 站点ID过滤
        if matched_main_ids:
            ph = []
            for i, sid in enumerate(matched_main_ids):
                k = f"ms_{i}"
                params[k] = sid
                ph.append(f"%({k})s")
            where_clauses.append(f"MAINSTATIONID IN ({', '.join(ph)})")
        if matched_sub_ids:
            ph = []
            for i, sid in enumerate(matched_sub_ids):
                k = f"ss_{i}"
                params[k] = sid
                ph.append(f"%({k})s")
            where_clauses.append(f"SUBSTATIONID IN ({', '.join(ph)})")

        # 3.4 模糊字段
        fuzzy_map = {
            "NAME": (names, "nm"),
            "AREANAME": (areas, "ar"),
            "WORKTYPE": (work_types, "wt"),
            "CLASSTIMENAME": (class_names, "cn"),
            "DEPARTMENT": (departments, "dp"),
        }
        for col, (vals, prefix) in fuzzy_map.items():
            if vals:
                parts = []
                for i, v in enumerate(vals):
                    k = f"{prefix}_{i}"
                    params[k] = f"%{self._escape_like(str(v))}%"
                    parts.append(f"{col} LIKE %({k})s")
                where_clauses.append(f"({' OR '.join(parts)})")

        where_sql = " AND ".join(where_clauses)

        # ==================== 4. 核心分段查询 ====================
        segment_query = f"""
        WITH base AS (
            SELECT
                NAME, CARDID, AREANAME, CLASSTIMENAME, DEPARTMENT,
                WORKTYPE, JOB, DUTYNAME, ELECTRICITY,
                MAINSTATIONID, MAINSTATIONDISTANCE,
                SUBSTATIONID, SUBSTATIONDISTANCE,
                toDateTime(MAINSTATIONTIME) AS m_time,
                toDateTime(UPDATE_TIME)   AS u_time,
                toDate(MAINSTATIONTIME)   AS station_date
            FROM PS.HISTORY_PERSONNEL_LOCATION
            WHERE {where_sql}
        ),
        flagged AS (
            SELECT *,
                lagInFrame(MAINSTATIONID) OVER w AS prev_station,
                lagInFrame(station_date)  OVER w AS prev_date,
                lagInFrame(u_time)        OVER w AS prev_u_time
            FROM base
            WINDOW w AS (PARTITION BY NAME, CARDID ORDER BY u_time)
        ),
        grouped AS (
            SELECT *,
                sum(if(
                    MAINSTATIONID != prev_station
                    OR station_date != prev_date
                    OR dateDiff('minute', prev_u_time, u_time) > {self.SEGMENT_GAP_MINUTES},
                    1, 0
                )) OVER (
                    PARTITION BY NAME, CARDID
                    ORDER BY u_time
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS segment_id
            FROM flagged
        )
        SELECT
            any(NAME)              AS name,
            any(CARDID)            AS cardid,
            any(AREANAME)          AS area_name,
            any(CLASSTIMENAME)     AS class_time_name,
            any(DEPARTMENT)        AS department,
            any(WORKTYPE)          AS work_type,
            any(JOB)               AS job,
            any(DUTYNAME)          AS duty_name,
            any(ELECTRICITY)       AS electricity,
            any(MAINSTATIONID)     AS main_station_id,
            any(SUBSTATIONID)      AS sub_station_id,
            any(station_date)      AS station_date,
            min(m_time)            AS enter_time,
            max(u_time)            AS leave_time,
            count()                AS point_count,
            dateDiff('second', min(m_time), max(u_time)) AS duration_sec,
            groupUniqArray(100)(MAINSTATIONDISTANCE)     AS main_distances,
            groupUniqArray(100)(SUBSTATIONDISTANCE)      AS sub_distances,
            groupArray(1000)(u_time)                     AS update_times
        FROM grouped
        GROUP BY NAME, CARDID, segment_id
        ORDER BY enter_time ASC
        """

        # ==================== 5. 执行与结果组装 ====================
        try:
            result = self.client.query(segment_query, parameters=params)
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            print(f"[ERROR] 人员轨迹查询失败:\n{tb}")
            return {"total": 0, "update_time": "", "persons": {}, "error": str(e)}

        persons_dict = {}
        total_segments = 0

        for row in result.result_rows:
            name, cardid = row[0], row[1]
            key = f"{name}_{cardid}"

            if key not in persons_dict:
                persons_dict[key] = {
                    "name": name,
                    "cardid": cardid,
                    "workType": row[5],
                    "classTimeName": row[3],
                    "department": row[4],
                    "dutyName": row[7],
                    "records": [],
                }

            ms_id_raw = row[9]
            ss_id_raw = row[10]
            ms_name = self.station_names.get(str(ms_id_raw), {}).get("name", str(ms_id_raw))
            ss_name = self.station_names.get(str(ss_id_raw), {}).get("name", str(ss_id_raw)) if ss_id_raw else None

            record = {
                "areaName": row[2],
                "segmentStartTime": str(row[12]).replace("+08:00", ""),
                "segmentEndTime": str(row[13]).replace("+08:00", ""),   # ← 现在是 max(u_time)
                "electricity": row[8],
                "mainStationId": ms_name,
                "mainStationDistance": row[16],
                "subStationId": ss_name,
                "subStationDistance": row[17],
                "stationDate": str(row[11]),
                "count": row[14],
                "duration": row[15],
                "job": row[6],
                "updateTimes": sorted({str(t) for t in row[18]}) if row[18] else [],
                "dutyName": row[7],
            }
            persons_dict[key]["records"].append(record)
            total_segments += 1

        return {
            "total": total_segments,
            "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "persons": persons_dict,
        }