#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文件名:	person_sqls.py
作者:	shihy
创建日期:	2026-04-22
描述:	矿井人员定位相关 SQL 模板，包含最新入井记录、多人员最新状态、人员轨迹及今日人员名单等查询的 SQL 语句。用于 ClickHouse 后端的数据查询，支持通过格式化 where 子句或传参灵活筛选人员、时间区间、区域等信息，配合工具服务实现多维度人员状态与轨迹分析。

"""

# 1. 查询某个人员最新一条入井记录
# 说明: 用于获取单个人员（可通过 where 子句灵活设定筛选条件）的最新定位信息。
GET_PERSON_LATEST_SQL = """
SELECT
    NAME, DEPARTMENT, CLASSTIMENAME, DUTYNAME, WORKTYPE,
    AREANAME, MAINSTATIONTIME, MAINSTATIONDISTANCE,
    MAINSTATIONID,
    SUBSTATIONTIME, SUBSTATIONDISTANCE, 
    SUBSTATIONID,
    AREATIME,
    ENTERTIME, UPDATE_TIME, CARDID
FROM PS.HISTORY_PERSONNEL_LOCATION
{where_clause}
ORDER BY UPDATE_TIME DESC
LIMIT 1
"""


# 2. 查询一组人员今天的最新状态（批量）
# 说明: 对给定姓名列表，聚合查询每个人“今天内”最新的一次出入井记录，用于批量状态分析。
GET_PERSONS_LATEST_SQL = """
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


# 3. 查询某个人员的分段轨迹
# 说明: 通过滑动窗口检测主站点或日期的变化来分割轨迹片段，对应每一段生成详细的分段记录，可用于轨迹可视化或安全分析。
GET_PERSON_TRAJECTORY_SQL = """
WITH base AS (
    SELECT
        NAME, AREANAME, CLASSTIMENAME, MAINSTATIONID, MAINSTATIONDISTANCE,
        DEPARTMENT, WORKTYPE, JOB,
        CARDID,
        toDateTime(MAINSTATIONTIME) AS m_time,
        toDateTime(UPDATE_TIME) AS u_time,
        toDate(MAINSTATIONTIME) AS stationDate
    FROM PS.HISTORY_PERSONNEL_LOCATION
    WHERE NAME = %(name)s
      AND UPDATE_TIME >= %(start)s
      AND UPDATE_TIME < %(end)s
    ORDER BY CARDID, u_time ASC
),
prev_values AS (
    SELECT
        u_time,
        MAINSTATIONID,
        stationDate,
        CARDID,
        lagInFrame(MAINSTATIONID) OVER (PARTITION BY CARDID ORDER BY u_time ASC) AS prev_id,
        lagInFrame(stationDate) OVER (PARTITION BY CARDID ORDER BY u_time ASC) AS prev_date,
        lagInFrame(CARDID) OVER (ORDER BY u_time ASC) AS prev_cardid
    FROM base
),
flag_change AS (
    SELECT
        u_time,
        CARDID,
        if(MAINSTATIONID != prev_id OR stationDate != prev_date OR CARDID != prev_cardid, 1, 0) AS is_change
    FROM prev_values
),
grouping_id AS (
    SELECT
        u_time,
        CARDID,
        sum(is_change) OVER (PARTITION BY CARDID ORDER BY u_time ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS segment_id
    FROM flag_change
)
SELECT
    any(b.NAME),
    any(b.AREANAME),
    any(b.CLASSTIMENAME),
    any(b.MAINSTATIONID),
    any(b.stationDate),
    min(b.m_time),
    max(b.m_time),
    count(*),
    dateDiff('second', min(b.m_time), max(b.m_time)),
    arrayCompact(groupArray(b.MAINSTATIONDISTANCE)) AS distChanges,
    any(b.DEPARTMENT),
    any(b.WORKTYPE),
    any(b.JOB),
    any(b.CARDID),
    groupArray(b.u_time)
FROM base b
INNER JOIN grouping_id g ON b.u_time = g.u_time AND b.CARDID = g.CARDID
GROUP BY b.CARDID, segment_id
ORDER BY b.CARDID, min(b.u_time) ASC
"""


# 4. 查询当天所有有记录的人员姓名
# 说明: 获取今日入井或出井的人名名单（唯一去重），便于做当天数据覆盖面统计。
# 有些人名可能对应多个CARDID，需一一列出。此写法每条记录唯一确定(name, cardid)组合。
GET_TODAY_PERSONS_SQL = """
SELECT NAME, CARDID,DEPARTMENT, WORKTYPE, CLASSTIMENAME,DUTYNAME
FROM PS.HISTORY_PERSONNEL_LOCATION
WHERE UPDATE_TIME >= today()
  AND UPDATE_TIME < today() + 1
GROUP BY NAME, CARDID,DEPARTMENT, WORKTYPE, CLASSTIMENAME,DUTYNAME
"""


# 5. 查询主站点静态信息
# 说明: 获取今日主站头信息，STATIONHEADID 唯一，对应位置及类型。

GET_REALTIME_STATION_HEAD_INFO_SQL = """
SELECT
    STATIONHEADID,
    STATIONHEADPLACE,
    STATIONHEADTYPE
FROM PS.REAL_TIME_STATION_HEAD_INFO
WHERE toDate(UPDATE_TIME) = today()
"""


# 6. 查询每个地点的限制人数
GET_AREA_LIMITS_SQL = """
SELECT
    AREANAME,
    AREALIMIT
FROM PS.REAL_TIME_AREA_INFO
"""

# 7. 查询当天车辆信息
"""
获取当天所有最新的、去重的车辆信息，包含: 
CARCODE,CARNAME, DEPARTMENT, CARTYPENAME, ELECTRICITY, MAINSTATIONID, MAINSTATIONTIME, MAINSTATIONDISTANCE, ENTERTIME, UPDATE_TIME
"""

GET_TODAY_CARS_SQL = """
SELECT
    CARCODE,
    CARNAME,
    DEPARTMENT,
    CARTYPENAME,
    ELECTRICITY,
    MAINSTATIONID,
    MAINSTATIONTIME,
    MAINSTATIONDISTANCE,
    ENTERTIME,
    UPDATE_TIME
FROM (
    SELECT *,
        row_number() OVER (PARTITION BY CARNAME ORDER BY UPDATE_TIME DESC) AS rn
    FROM PS.REAL_TIME_CAR_LOCATION
    WHERE UPDATE_TIME >= today()
    AND UPDATE_TIME < today() + 1
)
WHERE rn = 1
"""



