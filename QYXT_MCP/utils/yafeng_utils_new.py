import datetime
import traceback
import os
import yaml
from typing import Union, List, Dict, Optional, Any
import clickhouse_connect
import datetime as dt

# ==================== ClickHouse 分析 SQL 模板 (最终修复版) ====================
# 关键修复: 移除对已为 Float64 类型的字段使用 toFloat64OrNull()
CH_ANALYSIS_SQL = """
WITH base AS (
    SELECT
        YF_TIMESTAMP AS ts,
        toFloat64OrNull({FIELD}) AS val
    FROM PS.SDI_YA_FENG_XI_TONG
    WHERE YF_TIMESTAMP >= toDateTime('{START}')
      AND YF_TIMESTAMP < toDateTime('{END}')
      AND val IS NOT NULL
      {VALUE_FILTER}
),
-- 1. 基础统计 (按天聚合)
daily_stats AS (
    SELECT
        toDate(ts) AS day,
        count() AS n,
        avg(val) AS mean_v,
        stddevPop(val) AS std_v,
        quantileExact(0.5)(val) AS median_v,
        min(val) AS min_v, argMin(ts, val) AS min_ts,
        max(val) AS max_v, argMax(ts, val) AS max_ts,
        argMin(val, ts) AS first_v, min(ts) AS first_ts,
        argMax(val, ts) AS last_v, max(ts) AS last_ts
    FROM base
    GROUP BY day
    ORDER BY day
),
-- 2. 动态阈值计算
params AS (
    SELECT
        *,
        if(last_v > first_v, '上升', if(last_v < first_v, '下降', '平稳')) AS trend,
        greatest(abs(mean_v) * 0.25, std_v * 1.5 + 1e-8) AS change_thresh,
        greatest(5 * std_v, 1e-8) AS anomaly_thresh
    FROM daily_stats
),
-- 3. 变化点检测 (标准窗口函数)
changes_raw AS (
    SELECT
        ts, val, day, change_thresh,
        lagInFrame(val, 1, NULL) OVER (PARTITION BY day ORDER BY ts) AS prev_val,
        val - lagInFrame(val, 1, NULL) OVER (PARTITION BY day ORDER BY ts) AS diff,
        row_number() OVER (PARTITION BY day ORDER BY ts) AS rn
    FROM base b
    JOIN params p ON toDate(b.ts) = p.day
),
changes_flagged AS (
    SELECT
        ts, val, day, prev_val, diff,
        if(rn > 1 AND abs(diff) > change_thresh, 1, 0) AS is_change
    FROM changes_raw
),
-- 4. 提取变化点 (✅ 修复: 移除多余的 toFloat64OrNull)
changes AS (
    SELECT
        day,
        arraySlice(
            groupArrayIf(
                tuple(ts, prev_val, val, diff),  -- 已是正确类型，直接构造元组
                is_change = 1
            ),
            1, 50
        ) AS changes_arr
    FROM changes_flagged
    GROUP BY day
),
-- 5. 异常值检测 (✅ 修复: 移除多余的 toFloat64OrNull)
anomalies_raw AS (
    SELECT
        b.ts, b.val, p.day, p.anomaly_thresh, p.mean_v,
        abs(b.val - p.mean_v) > p.anomaly_thresh AS is_anomaly
    FROM base b
    JOIN params p ON toDate(b.ts) = p.day
),
anomalies AS (
    SELECT
        day,
        arraySlice(
            groupArrayIf(
                tuple(ts, val),  -- val 已是 Float64
                is_anomaly = 1
            ),
            1, 100
        ) AS anomalies_arr
    FROM anomalies_raw
    GROUP BY day
),
-- 6. 稳定阶段检测 (窗口分组 + 聚合)
stable_groups AS (
    SELECT
        ts, val, day,
        sum(is_change) OVER (PARTITION BY day ORDER BY ts) AS group_id
    FROM changes_flagged
),
stable_agg AS (
    SELECT
        day, group_id,
        any(val) AS stable_val,
        min(ts) AS start_ts,
        max(ts) AS end_ts,
        dateDiff('second', min(ts), max(ts)) AS duration_sec
    FROM stable_groups
    GROUP BY day, group_id
    HAVING duration_sec >= 20
),
-- 7. 稳定阶段数组组装 (✅ 修复: 移除多余转换)
stable_periods AS (
    SELECT
        day,
        arraySlice(
            groupArrayIf(
                tuple(stable_val, start_ts, end_ts, duration_sec),  -- 类型已正确
                duration_sec >= 20
            ),
            1, 500
        ) AS stable_arr
    FROM stable_agg
    GROUP BY day
)
-- 8. 最终组装
SELECT
    p.day, p.n, round(p.mean_v, 4), round(p.median_v, 4), round(p.std_v, 4),
    tuple(p.min_v, p.min_ts), tuple(p.max_v, p.max_ts),
    p.first_v, p.first_ts, p.last_v, p.last_ts, p.trend,
    ifNull(c.changes_arr, []) AS changes_arr,
    ifNull(a.anomalies_arr, []) AS anomalies_arr,
    ifNull(sp.stable_arr, []) AS stable_arr
FROM params p
LEFT JOIN changes c ON p.day = c.day
LEFT JOIN anomalies a ON p.day = a.day
LEFT JOIN stable_periods sp ON p.day = sp.day
ORDER BY p.day
"""

class YafengAnalyzer:
    def __init__(self, client: clickhouse_connect.driver.Client):
        self.client = client
        self._fan_system_cache = None
        
    def _load_yaml(self):
        """懒加载 YAML 配置，带缓存"""
        if self._fan_system_cache is None:
            yaml_path = os.path.join(os.path.dirname(__file__), "yafeng_system.yaml")
            with open(yaml_path, "r", encoding="utf-8") as f:
                self._fan_system_cache = yaml.safe_load(f)
        return self._fan_system_cache
    
    def print_Yafeng_today_by_yaml_system(
        self,
        system_name_filters: Union[List[str], str],
        start_date: Union[str, datetime.datetime, None] = None,
        end_date: Union[str, datetime.datetime, None] = None,
        subgroup_filters: Union[List[str], str, None] = None,
        value_filters: Optional[Dict[str, tuple]] = None,
        enable_stable_periods: bool = True,
    ) -> Dict[str, Any]:
        try:
            yaml_data = self._load_yaml()
            fan_system = yaml_data.get("fan_system", {})
            
            # 1. 参数标准化
            if isinstance(system_name_filters, str):
                system_name_filters = [system_name_filters]
            valid_systems = [s for s in system_name_filters if s in fan_system]
            if not valid_systems:
                print("⚠️ 无有效系统名")
                return {}

            if isinstance(subgroup_filters, str):
                subgroup_filters = [subgroup_filters]

            # 2. 时间解析
            start_dt = start_date or datetime.datetime.now()
            end_dt = end_date or start_dt
            
            if isinstance(start_dt, str):
                start_str = start_dt
                start_dt_obj = dt.datetime.strptime(start_dt, "%Y-%m-%d %H:%M:%S")
            else:
                start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
                start_dt_obj = start_dt
                
            if isinstance(end_dt, str):
                end_str = end_dt
                end_dt_obj = dt.datetime.strptime(end_dt, "%Y-%m-%d %H:%M:%S")
            else:
                end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
                end_dt_obj = end_dt
                
            is_multi_day = (end_dt_obj.date() - start_dt_obj.date()).days > 0

            # 3. 构建任务列表
            tasks = []
            for sys_name in valid_systems:
                for sg_name, metas in fan_system[sys_name].items():
                    if subgroup_filters and sg_name not in subgroup_filters:
                        continue
                    for en_key, cn_desc in metas.items():
                        if en_key == "YF_TIMESTAMP":
                            continue
                        tasks.append((sys_name, sg_name, en_key, cn_desc))

            if not tasks:
                return {}

            output_dict = {}
            col_names = ["日期", "个数", "平均值", "中位数", "标准差", "最小值", "最大值",
                         "最早值", "最早值时间", "最新值", "最新值时间", "趋势",
                         "重要变化", "异常值", "稳定阶段"]

            # 4. 逐字段执行查询
            for sys_name, sg_name, en_key, cn_desc in tasks:
                # 构建 value filter
                val_filter = ""
                if value_filters and en_key in value_filters:
                    op, thr = value_filters[en_key]
                    try:
                        thr_float = float(thr)
                        val_filter = f"AND val {op} {thr_float}"
                    except (ValueError, TypeError):
                        pass

                sql = CH_ANALYSIS_SQL.format(
                    FIELD=en_key, START=start_str, END=end_str, VALUE_FILTER=val_filter
                )

                try:
                    result = self.client.query(
                        sql, 
                        settings={
                            "max_execution_time": 300,
                            "max_memory_usage": 10 * 1024 * 1024 * 1024,
                            "max_bytes_before_external_group_by": 2 * 1024 * 1024 * 1024,
                        }
                    )
                    if result.row_count == 0:
                        continue

                    day_data = {}
                    for row in result.result_rows:
                        rec = dict(zip(col_names, row))
                        day_val = rec.get("日期")
                        day_str = day_val.strftime("%Y-%m-%d") if hasattr(day_val, "strftime") else str(day_val)

                        # 稳定阶段数据处理
                        stable_data = []
                        if enable_stable_periods and rec.get("稳定阶段"):
                            stable_data = [
                                {"取值": s[0], "起始时间": s[1], "结束时间": s[2], "持续秒数": int(s[3]) if s[3] else 0}
                                for s in rec["稳定阶段"]
                            ]

                        # 安全获取元组字段
                        min_info = rec.get("最小值")
                        max_info = rec.get("最大值")
                        
                        day_data[day_str] = {
                            "类型": "数值",
                            "个数": int(rec.get("个数", 0)),
                            "平均值": rec.get("平均值"),
                            "中位数": rec.get("中位数"),
                            "标准差": rec.get("标准差"),
                            "最小值": {"数值": min_info[0], "时间": min_info[1]} if min_info and len(min_info) >= 2 else None,
                            "最大值": {"数值": max_info[0], "时间": max_info[1]} if max_info and len(max_info) >= 2 else None,
                            "最早值": rec.get("最早值"),
                            "最早值时间": rec.get("最早值时间"),
                            "最新值": rec.get("最新值"),
                            "最新值时间": rec.get("最新值时间"),
                            "趋势": rec.get("趋势"),
                            "重要变化": [
                                {"时间": c[0], "从": float(c[1]) if c[1] is not None else 0, 
                                 "到": float(c[2]) if c[2] is not None else 0, 
                                 "变化量": float(c[3]) if c[3] is not None else 0}
                                for c in (rec.get("重要变化") or []) if c and c[0] is not None
                            ],
                            "异常值": [
                                {"时间": a[0], "数值": float(a[1]) if a[1] is not None else 0}
                                for a in (rec.get("异常值") or []) if a and a[0] is not None
                            ],
                            "稳定阶段": stable_data
                        }

                    # 组装输出
                    output_dict.setdefault(sys_name, {}).setdefault(sg_name, {})
                    if "监测值" in sg_name and is_multi_day:
                        output_dict[sys_name][sg_name][cn_desc] = {
                            "数据类型": "daily_numeric",
                            "每日数据": day_data,
                            "总共天数": len(day_data)
                        }
                    else:
                        all_days = list(day_data.values())
                        output_dict[sys_name][sg_name][cn_desc] = {
                            "数据": all_days[-1] if all_days else None
                        }

                except Exception as e:
                    print(f"❌ 字段 {en_key} 查询失败: {e}")
                    continue

            return output_dict

        except Exception as e:
            print(f"💥 分析异常: {e}\n{traceback.format_exc()}")
            return {}
        
if __name__ == "__main__":
    client = clickhouse_connect.get_client(
        host="10.11.22.80", port=9120, username="nethouse",
        password="CGC%EVXr.ET10Y_N", secure=True, verify=False
    )
    
    analyzer = YafengAnalyzer(client=client)
    
    # 先测1小时数据
    res = analyzer.print_Yafeng_today_by_yaml_system(
        system_name_filters=["1号空压机"],
        start_date="2023-10-01 00:00:00",
        end_date="2026-10-01 01:00:00",
        value_filters={"YF_GUAN_DAO_1_YA_LI": (">", 0.5)},
        enable_stable_periods=True
    )
    print(res)