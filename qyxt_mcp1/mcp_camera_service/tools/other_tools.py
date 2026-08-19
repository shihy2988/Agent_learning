# other_tools.py - 自动生成的模块文件
import json
import logging
from datetime import datetime
import clickhouse_connect
from fastmcp import FastMCP

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("PersonnelMCP")

# --- ClickHouse 连接配置 ---
HOST = "10.11.22.80"
PORT = 9120
USERNAME = "nethouse"
PASSWORD = "CGC%EVXr.ET10Y_N"
DATABASE = "PS"
TABLE_NAME = "personnel_tracking" # 需替换为您实际的表名

def get_client():
    return clickhouse_connect.get_client(
        host=HOST,
        port=PORT,
        secure=True,
        verify=False,
        username=USERNAME,
        password=PASSWORD,
        database=DATABASE
    )

mcp = FastMCP("PersonnelTrackingService")

# --- 核心工具 1：多维度动态查询 (仿摄像头工具) ---
@mcp.tool()
def query_personnel_info(
    name: str = None,
    department: str = None,
    duty_name: str = None,
    work_type: str = None,
    area_name: str = None,
    state_id: str = None,
    start_time: str = None,  # 格式: YYYY-MM-DD HH:MM:SS
    end_time: str = None,
    max_rows: int = 100
) -> str:
    """
    多维度动态查询人员定位基础信息。
    支持通过姓名、部门、职务、工种、区域、状态及时间范围进行过滤。
    """
    client = get_client()
    try:
        # 基础 SQL 结构
        query = f"SELECT * FROM {TABLE_NAME} WHERE 1=1"
        params = {}

        # 动态构建过滤条件
        filters = {
            "NAME": name,
            "DEPARTMENT": department,
            "DUTYNAME": duty_name,
            "WORKTYPE": work_type,
            "AREANAME": area_name,
            "STATEID": state_id
        }
        
        for col, val in filters.items():
            if val:
                query += f" AND {col} = %({col.lower()})s"
                params[col.lower()] = val

        if start_time:
            query += " AND UPDATETIME >= %(start)s"
            params["start"] = start_time
        if end_time:
            query += " AND UPDATETIME <= %(end)s"
            params["end"] = end_time

        query += " ORDER BY UPDATETIME DESC LIMIT %(limit)s"
        params["limit"] = max_rows

        result = client.query(query, parameters=params)
        
        # 转换数据格式
        data = [dict(zip(result.column_names, row)) for row in result.result_rows]
        
        # 处理时间序列化
        for row in data:
            if 'UPDATETIME' in row:
                row['UPDATETIME'] = row['UPDATETIME'].strftime('%Y-%m-%d %H:%M:%S')

        return json.dumps({
            "count": len(data),
            "data": data
        }, ensure_ascii=False, default=str)
    finally:
        client.close()

# --- 核心工具 2：人员轨迹智能分析 (去重与停留计算) ---
@mcp.tool()
def analyze_person_trajectory(name: str, start_time: str, end_time: str) -> str:
    """
    分析指定人员在一段时间内的运动轨迹。
    功能：自动去重、识别区域变化、计算在每个地点的停留时长。
    """
    client = get_client()
    try:
        sql = f"""
            SELECT NAME, DUTYNAME, AREANAME, MAINSTATIONID, UPDATETIME 
            FROM {TABLE_NAME} 
            WHERE NAME = %(name)s AND UPDATETIME BETWEEN %(start)s AND %(end)s
            ORDER BY UPDATETIME ASC
        """
        result = client.query(sql, parameters={"name": name, "start": start_time, "end": end_time})
        
        if not result.result_rows:
            return "在该时间段内未找到该人员的活动记录。"

        rows = [dict(zip(result.column_names, row)) for row in result.result_rows]
        
        # 轨迹压缩算法
        compressed_trajectory = []
        if not rows: return "[]"

        last_loc = None
        start_stay_time = rows[0]['UPDATETIME']
        
        for i, row in enumerate(rows):
            # 定义“位置”：区域 + 主分站
            current_loc = f"{row['AREANAME'] or '未知'}-{row['MAINSTATIONID'] or '无'}"
            
            # 如果位置发生变化，或者已经是最后一条记录
            if current_loc != last_loc or i == len(rows) - 1:
                if last_loc is not None:
                    duration = (row['UPDATETIME'] - start_stay_time).total_seconds()
                    compressed_trajectory.append({
                        "location": last_loc,
                        "arrival": start_stay_time.strftime('%H:%M:%S'),
                        "leave": row['UPDATETIME'].strftime('%H:%M:%S'),
                        "stay_minutes": round(duration / 60, 1)
                    })
                last_loc = current_loc
                start_stay_time = row['UPDATETIME']

        return json.dumps({
            "person": name,
            "duty": rows[0]['DUTYNAME'],
            "summary": f"共经过 {len(compressed_trajectory)} 个位置节点",
            "details": compressed_trajectory
        }, ensure_ascii=False)
    finally:
        client.close()

# --- 核心工具 3：当前井下统计 ---
@mcp.tool()
def query_underground_summary() -> str:
    """
    统计当前井下总人数及各区域人员分布情况。
    """
    client = get_client()
    try:
        # ClickHouse 特色：使用 argMax 获取每个卡号最新的记录
        sql = f"""
            SELECT 
                count(DISTINCT CARDID) as total_people,
                groupArray((AREANAME, count_per_area)) as distribution
            FROM (
                SELECT AREANAME, count(DISTINCT CARDID) as count_per_area
                FROM (
                    SELECT CARDID, argMax(AREANAME, UPDATETIME) as AREANAME
                    FROM {TABLE_NAME}
                    GROUP BY CARDID
                )
                GROUP BY AREANAME
            )
        """
        res = client.query(sql)
        row = res.result_rows[0]
        
        return json.dumps({
            "total_underground": row[0],
            "area_distribution": dict(row[1])
        }, ensure_ascii=False)
    finally:
        client.close()

# --- 核心工具 4：全局元数据去重查询 ---
@mcp.tool()
def query_system_metadata() -> str:
    """
    查询数据库中去重后的全局信息：总人数、总站点、总区域、所有职务及班次名单。
    """
    client = get_client()
    try:
        # 一次性查询多个去重计数
        sql = f"""
            SELECT 
                uniq(NAME), 
                uniq(MAINSTATIONID), 
                uniq(AREANAME), 
                groupUniqArray(DUTYNAME),
                groupUniqArray(CLASSTIMENAME),
                groupUniqArray(WORKTYPE)
            FROM {TABLE_NAME}
        """
        res = client.query(sql).result_rows[0]
        
        return json.dumps({
            "counts": {
                "total_people": res[0],
                "total_stations": res[1],
                "total_areas": res[2]
            },
            "lists": {
                "duties": res[3],
                "shifts": res[4],
                "work_types": res[5]
            }
        }, ensure_ascii=False)
    finally:
        client.close()

# --- 核心工具 5：个人详情分析 (最新记录 vs 变化总结) ---
@mcp.tool()
def analyze_person_status(name: str, analyze_period: bool = False, start_time: str = None) -> str:
    """
    查询个人状态详情。
    - analyze_period=False: 返回该人最新的一条状态。
    - analyze_period=True: 结合start_time，总结该时段内变动的字段（如电量、位置变迁、报警状态）。
    """
    client = get_client()
    try:
        if not analyze_period:
            # 仅返回最新
            sql = f"SELECT * FROM {TABLE_NAME} WHERE NAME = %(name)s ORDER BY UPDATETIME DESC LIMIT 1"
            res = client.query(sql, parameters={"name": name})
            if not res.result_rows: return "未找到数据"
            data = dict(zip(res.column_names, res.result_rows[0]))
            return json.dumps(data, ensure_ascii=False, default=str)
        else:
            # 变化总结：只关注变化的字段
            sql = f"""
                SELECT UPDATETIME, ELECTRICITY, AREANAME, STATEID 
                FROM {TABLE_NAME} 
                WHERE NAME = %(name)s AND UPDATETIME >= %(start)s
                ORDER BY UPDATETIME ASC
            """
            res = client.query(sql, parameters={"name": name, "start": start_time})
            rows = [dict(zip(res.column_names, r)) for r in res.result_rows]
            
            changes = []
            last_state = {}
            for r in rows:
                current_state = {k: v for k, v in r.items() if k != 'UPDATETIME'}
                if current_state != last_state:
                    r['UPDATETIME'] = r['UPDATETIME'].strftime('%H:%M:%S')
                    changes.append(r)
                    last_state = current_state
            
            return json.dumps({
                "summary": "仅列出状态变更时刻",
                "change_log": changes
            }, ensure_ascii=False)
    finally:
        client.close()

if __name__ == "__main__":
    mcp.run()