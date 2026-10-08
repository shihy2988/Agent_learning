# -*- coding: utf-8 -*-
'''
@File    : substation_sqls.py
@Describe: 淖尔壕煤矿 安全监控 —— 分站实时/历史状态(含分站安装位置)
           对应 NAO_AQJC_tables_desc.yaml:
             ODS_AQJK_FZSS_*  分站实时数据(1D/30D/2Y)
             ODS_AQJK_FZDY_*  分站基本信息(1D/30D/2Y)  —— 取 SUBSTATION_LOCATION
           利用 ClickHouse 窗口函数做“状态岛”(island) 聚合:
           将连续相同 (运行状态, 供电状态) 的时段合并为一段, 输出起止时间。
           数据源: ClickHouse, database = PS_NAO
           注: FZSS 组表无独立 TIME 字段, 以 ENTRY_TIME 作为状态时间戳排序。
'''
from collections import defaultdict
from typing import Optional
from typing import Any
from datetime import datetime

# 状态码映射(与 yaml desc 一致)
RUN_STATE_MAP = {
    '0': '通信正常',
    '1': '通信中断',
    '2': '故障',
    '9': '未知',
}
POWER_STATE_MAP = {
    '0': '直流供电',
    '1': '交流供电',
    '2': '电源故障',
    '9': '未知',
}

# 分站状态表(按粒度) —— 对应 ODS_AQJK_FZSS_*
FZSS_TABLE_MAP = {
    '1':  'ODS_AQJK_FZSS_1D',
    '30': 'ODS_AQJK_FZSS_30D',
    '2y': 'ODS_AQJK_FZSS_2Y',
}
# 分站基本信息表(按粒度) —— 对应 ODS_AQJK_FZDY_*, 用于取 SUBSTATION_LOCATION
FZDY_TABLE_MAP = {
    '1':  'ODS_AQJK_FZDY_1D',
    '30': 'ODS_AQJK_FZDY_30D',
    '2y': 'ODS_AQJK_FZDY_2Y',
}


def map_run_state(state):
    return RUN_STATE_MAP.get(str(state), f'未知({state})')


def map_power_state(state):
    return POWER_STATE_MAP.get(str(state), f'未知({state})')


def _format_time_str(t_str):
    """统一处理时间字符串, 去除时区后缀"""
    if not t_str:
        return ""
    return str(t_str).replace("+08:00", "")


def _resolve_tables(choice: str):
    """把 choice 解析成 (FZSS 表, FZDY 表)"""
    key = str(choice).lower()
    if key not in FZSS_TABLE_MAP:
        raise ValueError(f"不支持的 choice={choice!r}, 可选: {list(FZSS_TABLE_MAP.keys())}")
    return FZSS_TABLE_MAP[key], FZDY_TABLE_MAP[key]


def _query_fz_status_islands(client, fzss_table, fzdy_table, where_clause=None):
    """
    通用: 按 状态岛 聚合分站 (运行状态, 供电状态) 的连续时段,
          并 LEFT JOIN FZDY 表获取该分站的安装位置(SUBSTATION_LOCATION)。
    :return: {分站编码: [ {分站编码, 分站安装位置, 分站运行状态, 分站供电状态, 起始时间, 结束时间}, ... ]}
    """
    sql = """
    WITH
    base_data AS (
        SELECT
            SUBSTATION_CODE,
            SUBSTATION_STATUS,
            SUBSTATION_POWER,
            ENTRY_TIME,
            lagInFrame(SUBSTATION_STATUS) OVER (PARTITION BY SUBSTATION_CODE ORDER BY ENTRY_TIME) as prev_status,
            lagInFrame(SUBSTATION_POWER)  OVER (PARTITION BY SUBSTATION_CODE ORDER BY ENTRY_TIME) as prev_power
        FROM {fzss_table}
        {where_clause}
    ),
    marked_data AS (
        SELECT
            *,
            if(
                prev_status IS NULL OR
                prev_status != SUBSTATION_STATUS OR
                prev_power  != SUBSTATION_POWER,
                1, 0
            ) as is_new_island
        FROM base_data
    ),
    islanded_data AS (
        SELECT
            *,
            sum(is_new_island) OVER (
                PARTITION BY SUBSTATION_CODE ORDER BY ENTRY_TIME
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) as island_id
        FROM marked_data
    ),
    fzdy_info AS (
        -- 每个分站取 DATA_TIME 最新的一条, 拿到安装位置
        SELECT
            SUBSTATION_CODE,
            SUBSTATION_LOCATION
        FROM {fzdy_table}
        ORDER BY DATA_TIME DESC
        LIMIT 1 BY SUBSTATION_CODE
    )
    SELECT
        i.SUBSTATION_CODE,
        any(f.SUBSTATION_LOCATION)      as substation_location,
        any(i.SUBSTATION_STATUS)        as run_state,
        any(i.SUBSTATION_POWER)         as power_state,
        min(i.ENTRY_TIME)               as start_time,
        max(i.ENTRY_TIME)               as end_time
    FROM islanded_data i
    LEFT JOIN fzdy_info f
           ON f.SUBSTATION_CODE = i.SUBSTATION_CODE
    GROUP BY
        i.SUBSTATION_CODE,
        i.island_id
    ORDER BY
        i.SUBSTATION_CODE,
        start_time
    """.format(
        fzss_table=fzss_table,
        fzdy_table=fzdy_table,
        where_clause=(f"WHERE {where_clause}" if where_clause else "")
    )

    rows = client.query(sql).result_rows

    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        (substation_code, location,
        run_state, power_state,
        start_time, end_time) = row

        bucket = result.get(substation_code)
        if bucket is None:
            bucket = {
                "分站编码": substation_code,
                "分站安装位置": location,
                "时段": [],
            }
            result[substation_code] = bucket

        # ★ 计算持续秒数
        duration_s = 0
        if isinstance(start_time, datetime) and isinstance(end_time, datetime):
            duration_s = int((end_time - start_time).total_seconds())

        bucket["时段"].append({
            "分站运行状态": map_run_state(run_state),
            "分站供电状态": map_power_state(power_state),
            "起始时间": _format_time_str(start_time),
            "结束时间": _format_time_str(end_time),
            "持续秒数": duration_s,          # ★ 新增
            "持续小时": round(duration_s / 3600, 2),  # 顺带加一个, 可选
        })

    return result


def query_fz_history(client, where_clause: Optional[str] = None, choice: str = '2y'):
    """
    查询分站状态历史, 按状态岛聚合(含安装位置)
    :param where_clause: 可选, 如 "ENTRY_TIME >= '2026-09-01 00:00:00' AND ENTRY_TIME <= '2026-09-09 23:59:59'"
    :param choice: '1' / '30' / '2y' —— 分别对应 1D / 30D / 2Y 表
    :return: {分站编码: [状态时段, ...]}
    """
    fzss_table, fzdy_table = _resolve_tables(choice)
    return _query_fz_status_islands(client, fzss_table, fzdy_table, where_clause)

if __name__ == "__main__":
    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host="10.11.3.210",
        port=8123,
        database="PS_NAO",
        username="default",
        password="xt123456"
    )

    print("=" * 60)
    print("正在查询历史分站状态(30D, 含安装位置)...")
    data_history = query_fz_history(
        client,
        where_clause="ENTRY_TIME >= '2026-09-01 00:00:00' AND ENTRY_TIME <= '2026-09-10 23:59:59'",
        choice='2y'
    )
    print(f"历史分站数量: {len(data_history)}")
    print(data_history)

   