import clickhouse_connect
from collections import defaultdict

# 导入 query_base_station_info
from .base_info_sqls import query_base_station_info

# 状态码映射字典
RUN_STATE_MAP = {
    '0': '通讯正常',
    '1': '通讯中断',
    '2': '故障',
    '9': '未知'
}

POWER_STATE_MAP = {
    '0': '直流供电',
    '1': '交流供电',
    '2': '电源故障',
    '9': '未知'
}


def map_run_state(state):
    return RUN_STATE_MAP.get(str(state), f'未知({state})')


def map_power_state(state):
    return POWER_STATE_MAP.get(str(state), f'未知({state})')


def _format_time_str(t_str):
    """统一处理时间字符串，去除时区后缀"""
    if not t_str:
        return ""
    return str(t_str).replace("+08:00", "")


def query_jizhan_history(client, where_clause=None):
    """
    查询基站历史信息（HJL_RYDW_JZSS）
    利用 ClickHouse 窗口函数进行状态聚合
    """

    # 先获取 code->name 映射
    code2name = {}
    jizhan_info = query_base_station_info(client)
    for code, info in jizhan_info.items():
        if isinstance(info, dict):
            code2name[code] = info.get("基站名称") or code

    sql = """
    WITH 
    -- 1. 获取上一行的状态和时间
    base_data AS (
        SELECT
            MINE_CODE,
            MINE_NAME,
            BASE_STATION_CODE,
            BASE_STATION_RUN_STATE,
            BASE_STATION_POWER_STATE,
            TIME,
            lagInFrame(BASE_STATION_RUN_STATE) OVER (PARTITION BY BASE_STATION_CODE ORDER BY TIME) as prev_run_state,
            lagInFrame(BASE_STATION_POWER_STATE) OVER (PARTITION BY BASE_STATION_CODE ORDER BY TIME) as prev_power_state,
            lagInFrame(TIME) OVER (PARTITION BY BASE_STATION_CODE ORDER BY TIME) as prev_time
        FROM HJL_RYDW_JZSS
        {where_clause}
    ),
    -- 2. 标记状态变化的起始点 (Island Start)
    marked_data AS (
        SELECT
            *,
            if(
                prev_run_state IS NULL OR 
                prev_run_state != BASE_STATION_RUN_STATE OR 
                prev_power_state != BASE_STATION_POWER_STATE,
                1, 0
            ) as is_new_island
        FROM base_data
    ),
    -- 3. 生成 Island ID (累加 is_new_island)
    islanded_data AS (
        SELECT
            *,
            sum(is_new_island) OVER (PARTITION BY BASE_STATION_CODE ORDER BY TIME ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as island_id
        FROM marked_data
    )
    -- 4. 按 Island ID 聚合，获取每个状态段的起止时间和状态值
    SELECT
        MINE_CODE,
        MINE_NAME,
        BASE_STATION_CODE,
        any(BASE_STATION_RUN_STATE) as run_state,
        any(BASE_STATION_POWER_STATE) as power_state,
        min(TIME) as start_time,
        max(TIME) as end_time
    FROM islanded_data
    GROUP BY
        MINE_CODE,
        MINE_NAME,
        BASE_STATION_CODE,
        island_id
    ORDER BY
        BASE_STATION_CODE,
        start_time
    """.format(where_clause=f"WHERE {where_clause}" if where_clause else "")

    rows = client.query(sql).result_rows

    result = defaultdict(list)

    for row in rows:
        mine_code, mine_name, station_code, run_state, power_state, start_time, end_time = row
        station_name = code2name.get(station_code, station_code)
        period = {
            "基站运行状态": map_run_state(run_state),
            "基站供电状态": map_power_state(power_state),
            "起始时间": _format_time_str(start_time),
            "结束时间": _format_time_str(end_time),
            "基站编码": station_code
            # 如果需要保留矿井信息，也可以加在这里，但通常按基站名称分组即可
        }
        result[station_name].append(period)

    return dict(result)


def query_jizhan_realtime(client, where_clause=None):
    """
    查询基站实时信息（HJL_RYDW_JZSS_REAL_TIME）
    同样利用 ClickHouse 进行状态聚合
    """
    # 先获取 code->name 映射
    code2name = {}
    jizhan_info = query_base_station_info(client)
    for code, info in jizhan_info.items():
        if isinstance(info, dict):
            code2name[code] = info.get("基站名称") or code

    sql = """
    WITH 
    base_data AS (
        SELECT
            MINE_CODE,
            MINE_NAME,
            BASE_STATION_CODE,
            BASE_STATION_RUN_STATE,
            BASE_STATION_POWER_STATE,
            TIME,
            lagInFrame(BASE_STATION_RUN_STATE) OVER (PARTITION BY BASE_STATION_CODE ORDER BY TIME) as prev_run_state,
            lagInFrame(BASE_STATION_POWER_STATE) OVER (PARTITION BY BASE_STATION_CODE ORDER BY TIME) as prev_power_state
        FROM HJL_RYDW_JZSS_REAL_TIME
        {where_clause}
    ),
    marked_data AS (
        SELECT
            *,
            if(
                prev_run_state IS NULL OR 
                prev_run_state != BASE_STATION_RUN_STATE OR 
                prev_power_state != BASE_STATION_POWER_STATE,
                1, 0
            ) as is_new_island
        FROM base_data
    ),
    islanded_data AS (
        SELECT
            *,
            sum(is_new_island) OVER (PARTITION BY BASE_STATION_CODE ORDER BY TIME ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as island_id
        FROM marked_data
    )
    SELECT
        MINE_CODE,
        MINE_NAME,
        BASE_STATION_CODE,
        any(BASE_STATION_RUN_STATE) as run_state,
        any(BASE_STATION_POWER_STATE) as power_state,
        min(TIME) as start_time,
        max(TIME) as end_time
    FROM islanded_data
    GROUP BY
        MINE_CODE,
        MINE_NAME,
        BASE_STATION_CODE,
        island_id
    ORDER BY
        BASE_STATION_CODE,
        start_time
    """.format(where_clause=f"WHERE {where_clause}" if where_clause else "")

    rows = client.query(sql).result_rows

    result = defaultdict(list)

    for row in rows:
        mine_code, mine_name, station_code, run_state, power_state, start_time, end_time = row
        station_name = code2name.get(station_code, station_code)
        period = {
            "基站运行状态": map_run_state(run_state),
            "基站供电状态": map_power_state(power_state),
            "起始时间": _format_time_str(start_time),
            "结束时间": _format_time_str(end_time),
            "基站编码": station_code
        }
        result[station_name].append(period)

    return dict(result)


if __name__ == "__main__":
    client = clickhouse_connect.get_client(
        host="10.11.3.210",
        port=8123,
        database="PS",
        username="default",
        password="xt123456"
    )

    # 测试历史数据
    print("正在查询历史基站状态...")
    data_history = query_jizhan_history(
        client,
        where_clause="TIME >= '2026-07-15 08:00:00' AND TIME <= '2026-07-16 09:00:00' "
    )
    print(f"历史基站数量: {len(data_history)}")
    if data_history:
        first_station = list(data_history.keys())[0]
        print(f"示例基站 {first_station} 的前3条记录:", data_history[first_station][:3])

    # # 测试实时数据
    # print("\n正在查询实时基站状态...")
    # data_realtime = query_jizhan_realtime(client)
    # print(f"实时基站数量: {len(data_realtime)}")
    # if data_realtime:
    #     first_station = list(data_realtime.keys())[0]
    #     print(f"示例基站 {first_station} 的记录:", data_realtime[first_station])