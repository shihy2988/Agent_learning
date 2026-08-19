import traceback
import clickhouse_connect
from datetime import datetime, timedelta
import json
import os
import yaml
from typing import List, Dict, Optional, Union
import numpy as np


def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"Type {type(obj)} not serializable")


def fast_rle(values, timestamps, max_groups: int = 80):
    """
    高性能 RLE
    """
    if not values:
        return []
    groups = []
    prev = values[0]
    start_idx = 0
    n = len(values)
    for i in range(1, n):
        if values[i] != prev:
            if len(groups) < max_groups:
                groups.append(
                    {
                        "value": prev,
                        "start_time": timestamps[start_idx],
                        "end_time": timestamps[i - 1],
                        "duration": (
                            timestamps[i - 1] - timestamps[start_idx]
                        ).total_seconds(),
                    }
                )
            prev = values[i]
            start_idx = i
    if len(groups) < max_groups:
        groups.append(
            {
                "value": prev,
                "start_time": timestamps[start_idx],
                "end_time": timestamps[-1],
                "duration": (timestamps[-1] - timestamps[start_idx]).total_seconds(),
            }
        )
    return groups


def analyze_series(
    values: List, timestamps: List[datetime], enable_stable_periods: bool = True
) -> Dict:
    """
    高性能分析函数
    """
    valid = [(t, v) for t, v in zip(timestamps, values) if v is not None]
    if not valid:
        return {"type": "unknown", "message": "无有效数据"}
    ts_clean = [t for t, v in valid]
    val_clean = [v for t, v in valid]
    # ========================= Bool =========================
    if all(str(v).lower() in ("true", "false", "0", "1") for v in val_clean):
        bool_map = {True: "True", False: "False", 1: "True", 0: "False"}
        values_str = [bool_map.get(v, str(v)) for v in val_clean]
        changes = []
        prev = values_str[0]
        for i in range(1, len(values_str)):
            curr = values_str[i]
            if curr != prev:
                changes.append(
                    {
                        "change_time": ts_clean[i].strftime("%Y-%m-%d %H:%M:%S"),
                        "from": prev,
                        "to": curr,
                        "duration_sec": (ts_clean[i] - ts_clean[i - 1]).total_seconds(),
                    }
                )
                prev = curr
        return {
            "type": "bool",
            "current": values_str[-1],
            "changes": changes or "无变化",
            "stable_periods": fast_rle(values_str, ts_clean),
            "total_changes": len(changes),
        }
    # ========================= Numeric =========================
    try:
        values_num = np.asarray(val_clean, dtype=np.float64)
        n = len(values_num)
        if n == 0:
            return {"type": "unknown", "message": "无有效数据"}
        mean_v = float(np.mean(values_num))
        std_v = float(np.std(values_num)) if n > 1 else 0.0
        stats = {
            "count": int(n),
            "avg": round(mean_v, 4),
            "median": round(float(np.median(values_num)), 4),
            "std": round(std_v, 4),
            "min": {
                "value": float(np.min(values_num)),
                "time": ts_clean[int(np.argmin(values_num))],
            },
            "max": {
                "value": float(np.max(values_num)),
                "time": ts_clean[int(np.argmax(values_num))],
            },
        }
        # 显著变化
        diff = np.diff(values_num)
        threshold = max(abs(mean_v) * 0.25, std_v * 1.5 + 1e-8)
        change_idx = np.where(np.abs(diff) > threshold)[0]
        changes = [
            {
                "time": ts_clean[i + 1],
                "from": float(values_num[i]),
                "to": float(values_num[i + 1]),
                "diff": float(diff[i]),
            }
            for i in change_idx[:50]
        ]
        # 异常值
        anomalies_mask = np.abs(values_num - mean_v) > max(3 * std_v, 1e-8)
        anomalies = [
            {"time": ts_clean[i], "value": float(v)}
            for i, v in enumerate(values_num)
            if anomalies_mask[i]
        ][:20]
        stable_periods = []
        if enable_stable_periods and n > 1:
            stable_periods = fast_rle(np.round(values_num, 6).tolist(), ts_clean, 60)
        trend = (
            "上升"
            if values_num[-1] > values_num[0]
            else "下降" if values_num[-1] < values_num[0] else "平稳"
        )
        return {
            "type": "numeric",
            **stats,
            "significant_changes": changes,
            "stable_periods": stable_periods,
            "anomalies": anomalies,
            "trend": trend,
        }
    except Exception as e:
        return {"type": "unknown", "sample": str(val_clean[:5]), "error": str(e)}


class TongfengService:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host="10.11.22.80",
            port=9120,
            username="nethouse",
            password="CGC%EVXr.ET10Y_N",
            secure=True,
            verify=False,
        )
        self._fan_system_cache = None

    # =========================================================
    # YAML
    # =========================================================
    def _load_yaml(self):
        if self._fan_system_cache is None:
            yaml_path = os.path.join(os.path.dirname(__file__), "tongfeng_system.yaml")
            with open(yaml_path, "r", encoding="utf-8") as f:
                self._fan_system_cache = yaml.safe_load(f)
        return self._fan_system_cache

    # =========================================================
    # VALUE FILTER
    # =========================================================
    @staticmethod
    def _match_value_filter(v, op, threshold):
        if v is None:
            return False
        try:
            v = float(v)
            if op == ">":
                return v > threshold
            elif op == ">=":
                return v >= threshold
            elif op == "<":
                return v < threshold
            elif op == "<=":
                return v <= threshold
            elif op == "=":
                return v == threshold
            return False
        except:
            return False

    # =========================================================
    # MAIN
    # =========================================================
    def print_tongfeng_today_by_yaml_system(
            self,
            system_name_filters: Union[List[str], str],
            start_date: Union[str, datetime, None] = None,
            end_date: Union[str, datetime, None] = None,
            subgroup_filters: Union[List[str], str, None] = None,
            value_filters: Optional[Dict[str, tuple]] = None,
            enable_stable_periods: bool = True,
    ) -> Dict:
        try:
            yaml_data = self._load_yaml()
            fan_system = yaml_data.get("fan_system", {})

            if isinstance(system_name_filters, str):
                system_name_filters = [system_name_filters]
            valid_systems = [s for s in system_name_filters if s in fan_system]
            if not valid_systems:
                print("无有效系统名")
                return {}

            if isinstance(subgroup_filters, str):
                subgroup_filters = [subgroup_filters]

            if value_filters and not subgroup_filters:
                raise ValueError("value_filters 必须配合 subgroup_filters 使用")

            # ====================== 时间处理 ======================
            if start_date is None:
                start_date = datetime.now().date()
            if end_date is None:
                end_date = start_date

            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, "%Y-%m-%d").date()

            date_range = []
            current = start_date
            while current <= end_date:
                date_range.append(current)
                current += timedelta(days=1)

            is_multi_day = len(date_range) > 1
            print(f"时间范围: {start_date} ~ {end_date}  (共 {len(date_range)} 天)")

            # ====================== 字段收集 ======================
            need_fields = {"TF_TIMESTAMP"}
            # ...（保持你原来的字段收集逻辑不变）
            if subgroup_filters and value_filters:
                for field_name in value_filters.keys():
                    need_fields.add(field_name)
            elif subgroup_filters:
                for sys_name in valid_systems:
                    for subgroup_name, subgroup in fan_system[sys_name].items():
                        if subgroup_name not in subgroup_filters:
                            continue
                        need_fields.update(subgroup.keys())
            else:
                for sys_name in valid_systems:
                    for subgroup in fan_system[sys_name].values():
                        need_fields.update(subgroup.keys())

            fields_list = list(need_fields)

            # ====================== 主查询（一次拉取全部数据） ======================
            start_str = start_date.strftime("%Y-%m-%d 00:00:00")
            end_str = (end_date + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")

            query = f"""
                SELECT {",".join(fields_list)}
                FROM PS.SDI_TONG_FENG_XI_TONG
                WHERE TF_TIMESTAMP >= toDateTime('{start_str}')
                  AND TF_TIMESTAMP < toDateTime('{end_str}')
                ORDER BY TF_TIMESTAMP ASC
            """

            print(f"开始查询 {len(fields_list)} 个字段...")
            result = self.client.query(query)
            rows = result.result_rows
            if not rows:
                print("无数据")
                return {}

            col_names = result.column_names
            col_index = {col: idx for idx, col in enumerate(col_names)}
            all_timestamps = [row[col_index["TF_TIMESTAMP"]] for row in rows]

            # ====================== 按天分组数据 ======================
            daily_data = {d: [] for d in date_range}
            for row in rows:
                ts = row[col_index["TF_TIMESTAMP"]]
                daily_data[ts.date()].append(row)

            # ====================== 分析 ======================
            output_dict = {}

            for system_name in valid_systems:
                output_dict[system_name] = {}
                subgroups = fan_system[system_name]

                for subgroup_name, metas in subgroups.items():
                    if subgroup_filters and subgroup_name not in subgroup_filters:
                        continue

                    subgroup_result = {}
                    is_numeric_subgroup = (subgroup_name == "数值")

                    print(f"  子组: {subgroup_name} {'[按天统计]' if is_numeric_subgroup and is_multi_day else ''}")

                    for en_key, cn_desc in metas.items():
                        if en_key == "TF_TIMESTAMP":
                            continue
                        if value_filters and en_key not in value_filters:
                            continue

                        idx = col_index.get(en_key)
                        if idx is None:
                            continue

                        # ------------------- 按天统计逻辑 -------------------
                        if is_numeric_subgroup and is_multi_day:
                            daily_analyses = {}
                            for day in date_range:
                                day_rows = daily_data[day]
                                if not day_rows:
                                    continue
                                values = [row[idx] for row in day_rows]
                                day_ts = [row[col_index["TF_TIMESTAMP"]] for row in day_rows]

                                analysis = analyze_series(values, day_ts, enable_stable_periods)
                                daily_analyses[day.strftime("%Y-%m-%d")] = analysis

                            subgroup_result[en_key] = {
                                "desc": cn_desc,
                                "analysis_type": "daily_numeric",
                                "daily": daily_analyses,
                                "total_days": len([d for d in daily_analyses if daily_analyses[d]])
                            }
                        else:
                            # 原有单次分析逻辑
                            values = [row[idx] for row in rows]
                            if value_filters and en_key in value_filters:
                                op, threshold = value_filters[en_key]
                                filtered = [
                                    (t, v) for t, v in zip(all_timestamps, values)
                                    if self._match_value_filter(v, op, threshold)
                                ]
                                if not filtered:
                                    continue
                                f_ts = [t for t, v in filtered]
                                f_val = [v for t, v in filtered]
                                analysis = analyze_series(f_val, f_ts, enable_stable_periods)
                            else:
                                analysis = analyze_series(values, all_timestamps, enable_stable_periods)

                            subgroup_result[en_key] = {
                                "desc": cn_desc,
                                "analysis": analysis,
                            }

                    if subgroup_result:
                        output_dict[system_name][subgroup_name] = subgroup_result

            # ====================== 输出 ======================
            result_json = json.dumps(
                output_dict, ensure_ascii=False, separators=(",", ":"), default=json_serializer
            )
            print(f"\nJSON 大小: {len(result_json) / 1024:.2f} KB")

            with open("tongfeng_analysis.json", "w", encoding="utf-8") as f:
                json.dump(output_dict, f, ensure_ascii=False, indent=2, default=json_serializer)

            return output_dict

        except Exception as e:
            print(f"查询异常: {e}")
            traceback.print_exc()
            return {}

    # =========================================================
    # CLOSE
    # =========================================================
    def close(self):
        self.client.close()


# =============================================================
# TEST
# =============================================================
if __name__ == "__main__":
    service = TongfengService()
    # =========================================================
    # 情况1:
    # subgroup_filters + value_filters
    #
    # 只查询 TF_YHFJ_FENG_LIANG_SHI_JI_ZHI
    # 且只分析 > 6000 的数据
    # =========================================================
    result = service.print_tongfeng_today_by_yaml_system(
        system_name_filters=["一号风机系统", "二号风机系统"],
        start_date="2026-05-01",
        end_date="2026-05-07",
        subgroup_filters=["切换过程"],
        # value_filters={
        #     "TF_YHFJ_FENG_LIANG_SHI_JI_ZHI": (">", 6000)
        # },
        enable_stable_periods=False,
    )
    # =========================================================
    # 情况2:
    # 只传 subgroup_filters
    #
    # 查询 subgroup 下全部字段
    # 并分析所有值
    # =========================================================
    """
    result = service.print_tongfeng_today_by_yaml_system(
        system_name_filters=["一号风机系统"],
        subgroup_filters=["数值"],
        start_date="2026-05-01",
        end_date="2026-05-07",
    )
    """
    # =========================================================
    # 情况3:
    # 两个都不传
    #
    # 查询全部字段
    # =========================================================
    """
    result = service.print_tongfeng_today_by_yaml_system(
        system_name_filters=["一号风机系统"],
        start_date="2026-05-01",
        end_date="2026-05-07",
    )
    """
    service.close()
