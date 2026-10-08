# -*- coding: utf-8 -*-
'''
@File    : point_sqls.py
@Describe: 淖尔壕煤矿 安全监控 —— 测点实时数据(时序)/统计/异常
           对应 NAO_AQJK_tables_desc.yaml:
             ODS_AQJK_CDSS_*  测点实时数据(1D/30D/2Y)
             ODS_AQJK_TJSJ_*  测点统计信息(1D/30D/2Y)
             ODS_AQJK_YCBJ_*  测点异常信息(30D/2Y)
           数据源: ClickHouse, database = PS_NAO
           说明:
             * 实时数据为高频时序, 查询时强制带时间范围, 按 天 + 测点 分组;
             * 实时数据行本身已带 传感器类型名称/安装位置/单位, 无需 join;
             * 本模块在返回前会用 测点基本信息(CDDY) 字典补齐 门限/量程/类型 等字段,
               便于上层过滤。
'''
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Optional, List, Dict, Any, Union

from .base_info_sqls import query_cddy_info,query_fzdy_info

STRFTIME_FMT = "%Y-%m-%d %H:%M:%S"

CDSS_HISTORY_TABLE = "ODS_AQJK_CDSS_2Y"
CDSS_REALTIME_TABLE = "ODS_AQJK_CDSS_2Y"
TJSJ_TABLE = "ODS_AQJK_TJSJ_2Y"
YCBJ_TABLE = "ODS_AQJK_YCBJ_2Y"

POINT_STATUS_BIT_MAP = {
    "0": "正常",
    "1": "报警",
    "2": "断电",
    "4": "标校",
    "8": "超量程",
    "16": "分站故障",
    "32": "不巡检",
    "64": "暂停",
    "128": "传感器故障",
}


# 测点基本信息(CDDY)中需要并入实时数据、供上层过滤的字段
CDDY_ENRICH_FIELDS = [
    "分站编码", "系统编码", "数值类型", "高量程", "低量程",
    "上限报警门限", "上限解报门限", "下限报警门限", "下限解报门限",
    "上限断电门限", "上限复电门限", "下限断电门限", "下限复电门限",
    "传感器类型", "测点编码",
]

ABNORMAL_TYPE_MAP = {
    "001": "超限报警",
    "002": "断电报警",
    "003": "馈电异常",
    "004": "传感器断线",
    "005": "基站断电",
    "006": "基站不通",
    "007": "标校",
    "008": "超量程",
    "009": "超上限预警",
    "010": "超下限预警"
}


def _format_time_str(t_str):
    if not t_str:
        return ""
    return str(t_str).replace("+08:00", "")


def _day_str(t):
    """从时间值中取 前10位 作为天 key"""
    return str(t)[:10] if t else "Unknown"


from typing import Optional, Dict, Any
from datetime import date, datetime


# =========================================================
# 1. 主聚合: 统计量 + 元信息 + 趋势
# =========================================================
def query_cd_realtime(
    client,
    point_code: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    where_clause: Optional[str] = None,
    table_name: Optional[str] = None,
    with_runs: bool = True,
    with_status: bool = True,        # ★ 新增: 是否统计状态分布
    min_duration_sec: int = 20,
    max_stages_per_group: int = 500,
    value_eps: float = 1e-4,
    max_gap_sec: int = 180,
):
    """
    返回结构:
    {
      day_str: {
        point_code: {
            ... 原有统计字段 ...
            "主要状态": "正常",          # 出现次数最多的状态
            "测点状态": [                # ★ 状态明细 (with_status=True 时才有)
                {"状态": "正常", "个数": 100, "起始时间": "...", "结束时间": "..."},
                {"状态": "报警", "个数": 5,   "起始时间": "...", "结束时间": "..."},
            ],
        }
      }
    }
    """
    table_name = table_name or CDSS_HISTORY_TABLE

    # ---------- 组装 WHERE ----------
    wheres = []
    if point_code:
        wheres.append(f"POINT_CODE = '{point_code}'")
    if start_time:
        wheres.append(f"DATA_TIME >= '{start_time}'")
    if end_time:
        wheres.append(f"DATA_TIME <= '{end_time}'")
    if where_clause:
        wheres.append(f"({where_clause})")
    where_sql = "WHERE " + " AND ".join(wheres) if wheres else ""

    # ---------- 主统计量 (不再 any(POINT_STATUS)) ----------
    sql = f"""
        SELECT
            toDate(DATA_TIME)                                              AS day,
            POINT_CODE,
            any(SENSOR_TYPE_NAME)                                          AS SENSOR_TYPE_NAME,
            any(POINT_LOCATION)                                            AS POINT_LOCATION,
            any(POINT_VALUE_UNIT)                                          AS POINT_VALUE_UNIT,
            count()                                                        AS cnt,
            avg(accurateCastOrNull(POINT_VALUE, 'Float64'))                AS mean_v,
            median(accurateCastOrNull(POINT_VALUE, 'Float64'))             AS median_v,
            stddevPop(accurateCastOrNull(POINT_VALUE, 'Float64'))          AS std_v,
            min(accurateCastOrNull(POINT_VALUE, 'Float64'))                AS min_v,
            argMin(DATA_TIME, accurateCastOrNull(POINT_VALUE, 'Float64'))  AS min_t,
            max(accurateCastOrNull(POINT_VALUE, 'Float64'))                AS max_v,
            argMax(DATA_TIME, accurateCastOrNull(POINT_VALUE, 'Float64'))  AS max_t,
            argMin(accurateCastOrNull(POINT_VALUE, 'Float64'), DATA_TIME)  AS first_v,
            min(DATA_TIME)                                                 AS first_t,
            argMax(accurateCastOrNull(POINT_VALUE, 'Float64'), DATA_TIME)  AS last_v,
            max(DATA_TIME)                                                 AS last_t
        FROM {table_name}
        {where_sql}
        GROUP BY day, POINT_CODE
        ORDER BY day, POINT_CODE
    """
    rows = client.query(sql).result_rows

    # ---------- 测点基本信息 ----------
    fz = query_fzdy_info(client)
    cddy_info = query_cddy_info(client, fz_dict=fz)

    # ---------- 组装 ----------
    result: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for (
        day, pc,
        sensor_type, location, unit,
        cnt, mean_v, median_v, std_v,
        min_v, min_t, max_v, max_t,
        first_v, first_t, last_v, last_t,
    ) in rows:
        day_str = _day_str(day)
        day_dict = result.setdefault(day_str, {})

        base = cddy_info.get(pc, {})
        fv = float(first_v) if first_v is not None else None
        lv = float(last_v) if last_v is not None else None

        info = {
            "测点编码": pc,
            "传感器类型名称": sensor_type or base.get("传感器类型", ""),
            "测点安装位置": location or base.get("测点安装位置", ""),
            "数值单位": unit or base.get("数值单位", ""),

            "类型": "数值",
            "个数": int(cnt or 0),
            "平均值": round(float(mean_v or 0), 4),
            "中位数": round(float(median_v or 0), 4),
            "标准差": round(float(std_v or 0), 4),
            "最小值": {"数值": float(min_v or 0), "时间": _format_time_str(min_t)},
            "最大值": {"数值": float(max_v or 0), "时间": _format_time_str(max_t)},
            "最早值": fv,
            "最早值时间": _format_time_str(first_t),
            "最新值": lv,
            "最新值时间": _format_time_str(last_t),
            "趋势": (
                "上升" if (fv is not None and lv is not None and lv > fv)
                else "下降" if (fv is not None and lv is not None and lv < fv)
                else "平稳"
            ),
        }

        for f in CDDY_ENRICH_FIELDS:
            if f not in info and f in base:
                info[f] = base.get(f)

        day_dict[str(pc)] = info

    # ---------- 状态分布 (按 day + point + status 分组) ----------
    if with_status:
        status_info = query_cd_realtime_status(
            client,
            point_code=point_code,
            start_time=start_time,
            end_time=end_time,
            where_clause=where_clause,
            table_name=table_name,
        )
        for day_str, points in status_info.items():
            for pc, extra in points.items():
                if day_str in result and pc in result[day_str]:
                    result[day_str][pc].update(extra)

    # ---------- 稳定阶段 + 变化次数 ----------
    if with_runs:
        runs_info = query_cd_realtime_runs(
            client,
            point_code=point_code,
            start_time=start_time,
            end_time=end_time,
            where_clause=where_clause,
            table_name=table_name,
            min_duration_sec=min_duration_sec,
            max_stages_per_group=max_stages_per_group,
            value_eps=value_eps,
            max_gap_sec=max_gap_sec,
        )
        for day_str, points in runs_info.items():
            for pc, extra in points.items():
                if day_str in result and pc in result[day_str]:
                    result[day_str][pc].update(extra)

    return result


def query_cd_realtime_status(
    client,
    point_code: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    where_clause: Optional[str] = None,
    table_name: Optional[str] = None,
):
    """
    按 (天, 测点, 状态) 分组统计状态分布。
    返回:
    {
      day_str: {
        point_code: {
            "主要状态": "正常",                       # 出现次数最多的状态
            "测点状态": [                            # 按首次出现时间排序
                {"状态": "正常", "个数": 100, "起始时间": "...", "结束时间": "..."},
                {"状态": "报警", "个数": 5,   "起始时间": "...", "结束时间": "..."},
            ],
        }
      }
    }
    """
    table_name = table_name or CDSS_HISTORY_TABLE

    wheres = []
    if point_code:
        wheres.append(f"POINT_CODE = '{point_code}'")
    if start_time:
        wheres.append(f"DATA_TIME >= '{start_time}'")
    if end_time:
        wheres.append(f"DATA_TIME <= '{end_time}'")
    if where_clause:
        wheres.append(f"({where_clause})")
    where_sql = "WHERE " + " AND ".join(wheres) if wheres else ""

    sql = f"""
        SELECT
            toDate(DATA_TIME)   AS day,
            POINT_CODE,
            POINT_STATUS,
            count()             AS cnt,
            min(DATA_TIME)      AS start_t,
            max(DATA_TIME)      AS end_t
        FROM {table_name}
        {where_sql}
        GROUP BY day, POINT_CODE, POINT_STATUS
        ORDER BY day, POINT_CODE, start_t
    """
    rows = client.query(sql).result_rows

    result: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for day, pc, status, cnt, start_t, end_t in rows:
        day_str = _day_str(day)
        bucket = result.setdefault(day_str, {}).setdefault(str(pc), {
            "主要状态": "",
            "测点状态": [],
        })

        status_name = POINT_STATUS_BIT_MAP.get(status, POINT_STATUS_BIT_MAP.get(str(status), "未知"))
        bucket["测点状态"].append({
            "状态": status_name,
            "原始状态码": status,
            "个数": int(cnt or 0),
            "起始时间": _format_time_str(start_t),
            "结束时间": _format_time_str(end_t),
        })

    # 取数量最多的作为"主要状态"
    for day_str, points in result.items():
        for pc, bucket in points.items():
            if bucket["测点状态"]:
                main = max(bucket["测点状态"], key=lambda x: x["个数"])
                bucket["主要状态"] = main["状态"]

    return result

# =========================================================
# 2. 稳定阶段 + 变化次数 (窗口函数一次扫描)
# =========================================================
def query_cd_realtime_runs(
    client,
    point_code: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    where_clause: Optional[str] = None,
    table_name: Optional[str] = None,
    min_duration_sec: int = 20,       # 过滤瞬时波动
    max_stages_per_group: int = 500,
    value_eps: float = 1e-4,          # ★ 数值容差, 差值 <= eps 视为不变
    max_gap_sec: int = 180,           # ★ 相邻点最大间隔, 超过视为新段 (0=不判断)
):
    """
    稳定阶段 + 变化次数 (带容差 + 时间连续性)
    返回:
    {
      day_str: {
        point_code: {
            "稳定阶段": [...],
            "变化次数": int,        # 值变化超过 value_eps 的次数
            "重要变化次数": int,     # 超过 mean/std 阈值的次数
        }
      }
    }
    """
    table_name = table_name or CDSS_HISTORY_TABLE

    wheres = []
    if point_code:
        wheres.append(f"POINT_CODE = '{point_code}'")
    if start_time:
        wheres.append(f"DATA_TIME >= '{start_time}'")
    if end_time:
        wheres.append(f"DATA_TIME <= '{end_time}'")
    if where_clause:
        wheres.append(f"({where_clause})")
    where_sql = "WHERE " + " AND ".join(wheres) if wheres else ""

    # 时间断档判断 (max_gap_sec=0 时禁用)
    gap_expr = (
        f"(NOT isNull(prev_ts) AND dateDiff('second', prev_ts, DATA_TIME) > {max_gap_sec})"
        if max_gap_sec and max_gap_sec > 0
        else "0"
    )

    sql = f"""
    WITH
        base AS (
            SELECT
                toDate(DATA_TIME)                                  AS day,
                POINT_CODE                                         AS POINT_CODE,
                DATA_TIME                                          AS DATA_TIME,
                accurateCastOrNull(POINT_VALUE, 'Float64')         AS val
            FROM {table_name}
            {where_sql}
        ),
        stats AS (
            SELECT
                day,
                POINT_CODE,
                avg(val)       AS mean_v,
                stddevPop(val) AS std_v
            FROM base
            GROUP BY day, POINT_CODE
        ),
        with_prev AS (
            SELECT
                b.day        AS day,
                b.POINT_CODE AS POINT_CODE,
                b.DATA_TIME  AS DATA_TIME,
                b.val        AS val,
                lagInFrame(b.val)      OVER (
                    PARTITION BY b.day, b.POINT_CODE ORDER BY b.DATA_TIME
                ) AS prev_val,
                lagInFrame(b.DATA_TIME) OVER (
                    PARTITION BY b.day, b.POINT_CODE ORDER BY b.DATA_TIME
                ) AS prev_ts,
                s.mean_v AS mean_v,
                s.std_v  AS std_v
            FROM base b
            INNER JOIN stats s
                ON b.day = s.day AND b.POINT_CODE = s.POINT_CODE
        ),
        marked AS (
            SELECT
                day, POINT_CODE, DATA_TIME, val, prev_val, prev_ts, mean_v, std_v,
                -- ★ 值变化标记: 用容差代替 round 精确比较
                --   NULL 点不算变化; |val-prev| <= eps 视为不变
                if(
                    isNull(prev_val) OR isNull(val)
                    OR abs(val - prev_val) <= {value_eps},
                    0, 1
                ) AS val_change_flag,
                -- ★ 时间断档标记: 距上一点超过 max_gap_sec 视为新段
                if({gap_expr}, 1, 0) AS gap_flag,
                -- 重要变化标记(仍用 mean/std 阈值)
                if(
                    isNull(prev_val) OR isNull(val)
                    OR abs(val - prev_val)
                        <= greatest(abs(mean_v) * 0.25, std_v * 1.5 + 1e-8),
                    0, 1
                ) AS important_change_flag
            FROM with_prev
        ),
        runs AS (
            SELECT
                day, POINT_CODE, DATA_TIME, val,
                sum(val_change_flag + gap_flag) OVER (
                    PARTITION BY day, POINT_CODE
                    ORDER BY DATA_TIME
                    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                ) AS run_id
            FROM marked
        )
    -- ① 稳定阶段
    SELECT
        'stage'                         AS kind,
        toString(day)                   AS day_str,
        POINT_CODE                      AS POINT_CODE,
        run_id                          AS run_id,
        any(val)                        AS run_val,
        min(DATA_TIME)                  AS start_t,
        max(DATA_TIME)                  AS end_t,
        dateDiff('second', min(DATA_TIME), max(DATA_TIME)) AS duration_s,
        0                               AS change_cnt,
        0                               AS important_cnt
    FROM runs
    GROUP BY day, POINT_CODE, run_id
    HAVING duration_s >= {min_duration_sec}

    UNION ALL

    -- ② 汇总: 变化次数 / 重要变化次数
    SELECT
        'summary'                       AS kind,
        toString(day)                   AS day_str,
        POINT_CODE                      AS POINT_CODE,
        -1                              AS run_id,
        NULL                            AS run_val,
        min(DATA_TIME)                  AS start_t,
        max(DATA_TIME)                  AS end_t,
        0                               AS duration_s,
        countIf(val_change_flag = 1)         AS change_cnt,
        countIf(important_change_flag = 1)   AS important_cnt
    FROM marked
    GROUP BY day, POINT_CODE

    ORDER BY day_str, POINT_CODE, kind, run_id
    """
    rows = client.query(sql).result_rows

    result: Dict[str, Dict[str, Dict[str, Any]]] = {}
    for (
        kind, day_str, pc, run_id, run_val,
        start_t, end_t, duration_s, change_cnt, important_cnt,
    ) in rows:
        bucket = result.setdefault(day_str, {}).setdefault(str(pc), {
            "稳定阶段": [],
            "变化次数": 0,
            "重要变化次数": 0,
        })

        if kind == "summary":
            bucket["变化次数"] = int(change_cnt or 0)
            bucket["重要变化次数"] = int(important_cnt or 0)
        else:
            if len(bucket["稳定阶段"]) >= max_stages_per_group:
                continue
            bucket["稳定阶段"].append({
                "取值": run_val,
                "起始时间": _format_time_str(start_t),
                "结束时间": _format_time_str(end_t),
                "持续秒数": int(duration_s or 0),
                "持续小时": round((duration_s or 0) / 3600, 2),
            })

    return result


# =========================================================
# 3. 工具函数 (若项目里已有, 直接复用即可)
# =========================================================
def _day_str(d) -> str:
    """统一转成 'YYYY-MM-DD'"""
    if isinstance(d, str):
        return d[:10]
    if isinstance(d, (datetime, date)):
        return d.strftime("%Y-%m-%d")
    return str(d)[:10]


def _format_time_str(t) -> str:
    """统一时间格式化为 'YYYY-MM-DD HH:MM:SS'"""
    if t is None:
        return ""
    if isinstance(t, str):
        return t
    if isinstance(t, datetime):
        return t.strftime("%Y-%m-%d %H:%M:%S")
    return str(t)




def query_cd_statistics(
    client,
    point_code: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    where_clause: Optional[str] = None,
    table_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    查询测点统计信息(最大/最小/平均 及对应时刻), 返回按测点分组的 list of dict
    对应 ODS_AQJK_TJSJ_*
    :return: [{测点编码, 传感器类型名称, 测点安装位置, 数值单位, 统计区间: [ {...}, ... ]}]
    """
    table_name = table_name or TJSJ_TABLE
    wheres = []
    if point_code:
        wheres.append(f"POINT_CODE = '{point_code}'")
    if start_time:
        wheres.append(f"DATA_TIME >= '{start_time}'")
    if end_time:
        wheres.append(f"DATA_TIME <= '{end_time}'")
    if where_clause:
        wheres.append(f"({where_clause})")
    where_sql = "WHERE " + " AND ".join(wheres) if wheres else ""

    fields = [
        "POINT_CODE", "SENSOR_TYPE_NAME", "POINT_LOCATION", "POINT_VALUE_UNIT",
        "START_TIME", "END_TIME",
        "MAX_VALUE", "MAX_VALUE_TIME", "MIN_VALUE", "MIN_VALUE_TIME", "AVG_VALUE",
        "DATA_TIME",
    ]
    sql = f"SELECT {', '.join(fields)} FROM {table_name} {where_sql} ORDER BY POINT_CODE, DATA_TIME"
    rows = client.query(sql).result_rows

    grouped: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        fm = dict(zip(fields, row))
        code = str(fm["POINT_CODE"])
        g = grouped.setdefault(code, {
            "测点编码": fm["POINT_CODE"],
            "传感器类型名称": fm["SENSOR_TYPE_NAME"],
            "测点安装位置": fm["POINT_LOCATION"],
            "数值单位": fm["POINT_VALUE_UNIT"],
            "统计区间": [],
        })
        g["统计区间"].append({
            "开始时间": _format_time_str(fm["START_TIME"]),
            "结束时间": _format_time_str(fm["END_TIME"]),
            "最大值": fm["MAX_VALUE"],
            "最大值时刻": _format_time_str(fm["MAX_VALUE_TIME"]),
            "最小值": fm["MIN_VALUE"],
            "最小值时刻": _format_time_str(fm["MIN_VALUE_TIME"]),
            "平均值": fm["AVG_VALUE"],
            "数据时间": _format_time_str(fm["DATA_TIME"]),
        })
    return list(grouped.values())


def query_yb_info(
    client,
    point_code: Optional[str] = None,
    start_time: Optional[str] = None,
    end_time: Optional[str] = None,
    where_clause: Optional[str] = None,
    table_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    查询测点异常信息，按测点编码分组，每个测点下按异常时间段区分。
    针对同一异常开始时间，若存在结束时间非空的记录，则过滤掉结束时间为空的记录。
    支持同一测点多次异常开始-结束循环。
    最后进行完全重复去重。
    """
    table_name = table_name or YCBJ_TABLE
    wheres = []
    if point_code:
        wheres.append(f"POINT_CODE = '{point_code}'")
    if start_time:
        wheres.append(f"DATA_TIME >= '{start_time}'")
    if end_time:
        wheres.append(f"DATA_TIME <= '{end_time}'")
    if where_clause:
        wheres.append(f"({where_clause})")
    where_sql = "WHERE " + " AND ".join(wheres) if wheres else ""

    fields = [
        "POINT_CODE", "SENSOR_TYPE_NAME", "POINT_LOCATION", "POINT_VALUE_UNIT",
        "ABNORMAL_TYPE", "START_TIME", "END_TIME",
        "MAX_VALUE", "MAX_VALUE_TIME", "MIN_VALUE", "MIN_VALUE_TIME", "AVG_VALUE",
        "ABNORMAL_CAUSE", "MEASURE", "ENTER_TIME", "ENTER_USER", "DATA_TIME",
    ]
    sql = f"SELECT {', '.join(fields)} FROM {table_name} {where_sql} ORDER BY POINT_CODE, START_TIME ASC, END_TIME ASC"
    rows = client.query(sql).result_rows

    # 第一步：按测点分组，再按 (START_TIME, ABNORMAL_TYPE) 分组存储原始记录
    grouped_by_point = defaultdict(lambda: defaultdict(list))
    for row in rows:
        fm = dict(zip(fields, row))
        code = str(fm["POINT_CODE"])
        # 去除时区后缀并转换为字符串，便于比较
        start = str(fm["START_TIME"]).replace("+08:00", "").strip()
        end = str(fm["END_TIME"]).replace("+08:00", "").strip() if fm["END_TIME"] else None
        abnormal_type = fm["ABNORMAL_TYPE"] or ""
        # 存储原始记录，同时保留原始时间对象以备后续格式化
        key = (start, abnormal_type)
        grouped_by_point[code][key].append({
            "raw": fm,
            "start_str": start,
            "end_str": end,
        })

    # 第二步：处理每个测点的每个 (start, type) 组，过滤掉结束时间为空的记录（如果存在非空结束时间）
    result_dict = {}
    for code, groups in grouped_by_point.items():
        # 获取该测点的元信息（取第一条记录的）
        first_fm = None
        for lst in groups.values():
            if lst:
                first_fm = lst[0]["raw"]
                break
        if first_fm is None:
            continue

        point_info = {
            "测点编码": first_fm["POINT_CODE"],
            "传感器类型名称": first_fm["SENSOR_TYPE_NAME"],
            "测点安装位置": first_fm["POINT_LOCATION"],
            "数值单位": first_fm["POINT_VALUE_UNIT"],
            "异常事件列表": [],
            "_seen": set(),  # 用于去重
        }

        for (start, ab_type), records in groups.items():
            # 检查该组是否存在结束时间非空的记录
            has_nonempty_end = any(rec["end_str"] is not None for rec in records)
            # 筛选记录
            if has_nonempty_end:
                filtered = [rec for rec in records if rec["end_str"] is not None]
            else:
                filtered = records  # 全部为空，保留所有（可能有多条重复）

            # 对筛选后的记录进行去重（基于开始时间、结束时间、异常类型完全一致）
            for rec in filtered:
                fm = rec["raw"]
                start_time_val = rec["start_str"]
                end_time_val = rec["end_str"]  # 可能为 None
                # 去重键
                dedup_key = (start_time_val, end_time_val, ab_type)
                if dedup_key in point_info["_seen"]:
                    continue
                point_info["_seen"].add(dedup_key)

                # 计算持续时长（分钟）
                duration_minutes = None
                if fm["START_TIME"] and fm["END_TIME"]:
                    try:
                        start_dt = datetime.strptime(start_time_val, "%Y-%m-%d %H:%M:%S")
                        end_dt = datetime.strptime(end_time_val, "%Y-%m-%d %H:%M:%S")
                        duration_minutes = int((end_dt - start_dt).total_seconds() / 60)
                    except (ValueError, TypeError):
                        pass

                point_info["异常事件列表"].append({
                    
                    "异常类型编码": ab_type,  # 保留原始编码
                    "异常类型描述": ABNORMAL_TYPE_MAP.get(str(ab_type).zfill(3), ab_type),  # 转换为中文描述，找不到时默认返回原值
                    "异常开始时间": _format_time_str(fm["START_TIME"]),
                    "异常结束时间": _format_time_str(fm["END_TIME"]),
                    "异常期间最大值": fm["MAX_VALUE"],
                    "最大值时刻": _format_time_str(fm["MAX_VALUE_TIME"]),
                    "异常期间最小值": fm["MIN_VALUE"],
                    "最小值时刻": _format_time_str(fm["MIN_VALUE_TIME"]),
                    "异常期间平均值": fm["AVG_VALUE"],
                    "异常原因": fm["ABNORMAL_CAUSE"],
                    "处理措施": fm["MEASURE"],
                    "录入时间": _format_time_str(fm["ENTER_TIME"]),
                    "录入人": fm["ENTER_USER"],
                    "数据时间": _format_time_str(fm["DATA_TIME"]),
                    "持续时长(分钟)": duration_minutes,
                })

        # 移除临时集合
        point_info.pop("_seen", None)
        result_dict[code] = point_info

    return list(result_dict.values())



if __name__ == "__main__":
    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host="10.11.3.210",
        port=8123,
        database="PS_NAO",
        username="default",
        password="xt123456"
    )

    print("=" * 70)
    print("1. 查询测点实时数据...")
    rt = query_cd_realtime(client, start_time="2026-09-09 00:00:00", end_time="2026-09-09 23:59:59")
   
    print(f"测点实时-天数: {len(rt)}")
    if rt:
        d = list(rt.keys())[0]
        p = list(rt[d].keys())[0]
        print("示例测点:", p, "该测点数:", len(rt[d][p]["数据"]), "首条:", rt[d][p]["数据"][0])

    # print("\n" + "=" * 70)
    # print("2. 查询测点统计信息...")
    # st = query_cd_statistics(client, start_time="2026-09-09 00:00:00", end_time="2026-09-10 23:59:59")
    # print(f"测点统计-测点数: {len(st)}")
    # # if st:
    # #     print("示例:", st[0])

    print("\n" + "=" * 70)
    print("3. 查询测点异常信息(按测点分组, 支持同一测点多次异常开始-结束循环)...")
    yb = query_yb_info(client, start_time="2026-09-09 00:00:00", end_time="2026-09-09 23:59:59")
    print(f"测点异常-测点数: {len(yb)}")
    if yb:
        for item in yb[:3]:
            print(f"  测点: {item['测点编码']}, 异常事件数: {len(item['异常事件列表'])}")
        # 打印第一个测点的详细信息
        first = yb[0]
        print(f"\n示例测点: {first['测点编码']}")
        print(f"  传感器类型: {first['传感器类型名称']}")
        print(f"  安装位置: {first['测点安装位置']}")
        print(f"  异常事件列表:")
        for i, event in enumerate(first['异常事件列表'][:3], 1):
            print(f"    事件{i}: {event['异常开始时间']} → {event['异常结束时间']} "
                  f"(类型: {event['异常类型']}, 持续: {event['持续时长(分钟)']}分钟)")

    