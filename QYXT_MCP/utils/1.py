# -*- coding: utf-8 -*-
'''
@File    : 1.py
@IDE     : PyCharm
@Author  : shihongyu
@Date    : 2026/05/09
@Describe: 
'''
import traceback
import clickhouse_connect
from datetime import datetime, timedelta
import json
import os
import yaml
from typing import List, Dict, Optional, Union
import numpy as np
from datetime import timezone
from collections import defaultdict

# 统一时间对象的时区类型为"offset-naive"
def to_naive(dt):
    # 如果是带时区(datetime.tzinfo)，则转为naive（本地/UTC时区由实际场景决定，这里去除tzinfo）
    if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt


fan_monitor_tags = {
    "定子温度": [
        "TF_YH_1_JI_DING_ZI_WEN_DU_A_SHI_JI_ZHI",
        "TF_YH_1_JI_DING_ZI_WEN_DU_B_SHI_JI_ZHI",
        "TF_YH_1_JI_DING_ZI_WEN_DU_C_SHI_JI_ZHI",
        "TF_YH_2_JI_DING_ZI_WEN_DU_A_SHI_JI_ZHI",
        "TF_YH_2_JI_DING_ZI_WEN_DU_B_SHI_JI_ZHI",
        "TF_YH_2_JI_DING_ZI_WEN_DU_C_SHI_JI_ZHI",
        "TF_EH_1_JI_DING_ZI_WEN_DU_A_SHI_JI_ZHI",
        "TF_EH_1_JI_DING_ZI_WEN_DU_B_SHI_JI_ZHI",
        "TF_EH_1_JI_DING_ZI_WEN_DU_C_SHI_JI_ZHI",
        "TF_EH_2_JI_DING_ZI_WEN_DU_A_SHI_JI_ZHI",
        "TF_EH_2_JI_DING_ZI_WEN_DU_B_SHI_JI_ZHI",
        "TF_EH_2_JI_DING_ZI_WEN_DU_C_SHI_JI_ZHI",
    ],
    "轴温度": [
        "TF_YH_1_JI_QIAN_ZHOU_WEN_DU_SHI_JI_ZHI",
        "TF_YH_1_JI_HOU_ZHOU_WEN_DU_SHI_JI_ZHI",
        "TF_YH_2_JI_QIAN_ZHOU_WEN_DU_SHI_JI_ZHI",
        "TF_YH_2_JI_HOU_ZHOU_WEN_DU_SHI_JI_ZHI",
        "TF_EH_1_JI_QIAN_ZHOU_WEN_DU_SHI_JI_ZHI",
        "TF_EH_1_JI_HOU_ZHOU_WEN_DU_SHI_JI_ZHI",
        "TF_EH_2_JI_QIAN_ZHOU_WEN_DU_SHI_JI_ZHI",
        "TF_EH_2_JI_HOU_ZHOU_WEN_DU_SHI_JI_ZHI",
    ],
    "电流": [
        "TF_YH_1_JI_DIAN_LIU_A_SHI_JI_ZHI",
        "TF_YH_2_JI_DIAN_LIU_A_SHI_JI_ZHI",
        "TF_EH_1_JI_DIAN_LIU_A_SHI_JI_ZHI",
        "TF_EH_2_JI_DIAN_LIU_A_SHI_JI_ZHI"
    ],
    "电压": [
        "TF_YH_1_JI_DIAN_YA_A_SHI_JI_ZHI",
        "TF_YH_2_JI_DIAN_YA_A_SHI_JI_ZHI",
        "TF_EH_1_JI_DIAN_YA_A_SHI_JI_ZHI",
        "TF_EH_2_JI_DIAN_YA_A_SHI_JI_ZHI"
    ],
    "振动": [
        "TF_YH_1_JI_CHUI_ZHI_ZHEN_DONG_SHI_JI_ZHI",
        "TF_YH_1_JI_SHUI_PING_ZHEN_DONG_SHI_JI_ZHI",
        "TF_YH_2_JI_CHUI_ZHI_ZHEN_DONG_SHI_JI_ZHI",
        "TF_YH_2_JI_SHUI_PING_ZHEN_DONG_SHI_JI_ZHI",
        "TF_EH_1_JI_CHUI_ZHI_ZHEN_DONG_SHI_JI_ZHI",
        "TF_EH_1_JI_SHUI_PING_ZHEN_DONG_SHI_JI_ZHI",
        "TF_EH_2_JI_CHUI_ZHI_ZHEN_DONG_SHI_JI_ZHI",
        "TF_EH_2_JI_SHUI_PING_ZHEN_DONG_SHI_JI_ZHI"
    ],
    "功率": [
        "TF_YH_1_JI_GONG_LV_SHI_JI_ZHI",
        "TF_YH_2_JI_GONG_LV_SHI_JI_ZHI",
        "TF_EH_1_JI_GONG_LV_SHI_JI_ZHI",
        "TF_EH_2_JI_GONG_LV_SHI_JI_ZHI"
    ],
    "风速": [
        "TF_YHFJ_FENG_SU_SHI_JI_ZHI",
        "TF_EHFJ_FENG_SU_SHI_JI_ZHI"
    ],
    "静压": [
        "TF_YHFJ_JING_YA_SHI_JI_ZHI",
        "TF_EHFJ_JING_YA_SHI_JI_ZHI"
    ],
    "全压": [
        "TF_YHFJ_QUAN_YA_SHI_JI_ZHI",
        "TF_EHFJ_QUAN_YA_SHI_JI_ZHI"
    ],
    "效率": [
        "TF_YHFJ_YUN_XING_XIAO_LV",
        "TF_EHFJ_YUN_XING_XIAO_LV"
    ],
    "风量": [
        "TF_YHFJ_FENG_LIANG_SHI_JI_ZHI",
        "TF_EHFJ_FENG_LIANG_SHI_JI_ZHI"
    ],
    "动压": [
        "TF_YHFJ_DONG_YA_SHI_JI_ZHI",
        "TF_EHFJ_DONG_YA_SHI_JI_ZHI"
    ]
}


def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"Type {type(obj)} not serializable")


def fast_rle(values, timestamps, max_groups: int = 500):
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
            duration = (timestamps[i - 1] - timestamps[start_idx]).total_seconds()
            if len(groups) < max_groups and duration >= 20:
                groups.append(
                    {
                        "取值": prev,
                        "起始时间": timestamps[start_idx],
                        "结束时间": timestamps[i - 1],
                        "持续秒数": duration,
                        "持续小时": round(duration / 3600, 2),
                    }
                )
            prev = values[i]
            start_idx = i
    duration = (timestamps[-1] - timestamps[start_idx]).total_seconds()
    if len(groups) < max_groups and duration >= 20:
        groups.append(
            {
                "取值": prev,
                "起始时间": timestamps[start_idx],
                "结束时间": timestamps[-1],
                "持续秒数": duration,
                "持续小时": round(duration / 3600, 2),
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
        return {"类型": "未知", "信息": "无有效数据"}
    ts_clean = [t for t, v in valid]
    val_clean = [v for t, v in valid]
    # ========================= Bool =========================
    if all(str(v).lower() in ("true", "false", "0", "1") for v in val_clean):
        bool_map = {True: "真", False: "假", 1: "真", 0: "假"}
        values_str = [bool_map.get(v, str(v)) for v in val_clean]
        changes = []
        prev = values_str[0]
        for i in range(1, len(values_str)):
            curr = values_str[i]
            if curr != prev:
                duration = (ts_clean[i] - ts_clean[i - 1]).total_seconds()
                changes.append(
                    {
                        "变化时间": ts_clean[i].strftime("%Y-%m-%d %H:%M:%S"),
                        "从": prev,
                        "到": curr,
                        "持续秒数": duration,
                        "持续小时": round(duration / 3600, 2),
                    }
                )
                prev = curr
        return {
            "类型": "布尔",
            "当前值": values_str[-1],
            "最早值": values_str[0],
            "最新值": values_str[-1],
            "最早值时间": ts_clean[0],
            "最新值时间": ts_clean[-1],
            "变化": changes or "无变化",
            "稳定阶段": fast_rle(values_str, ts_clean),
            "变化次数": len(changes),
        }
    # ========================= Numeric =========================
    try:
        values_num = np.asarray(val_clean, dtype=np.float64)
        n = len(values_num)
        if n == 0:
            return {"类型": "未知", "信息": "无有效数据"}
        mean_v = float(np.mean(values_num))
        std_v = float(np.std(values_num)) if n > 1 else 0.0
        stats = {
            "个数": int(n),
            "平均值": round(mean_v, 4),
            "中位数": round(float(np.median(values_num)), 4),
            "标准差": round(std_v, 4),
            "最小值": {
                "数值": float(np.min(values_num)),
                "时间": ts_clean[int(np.argmin(values_num))],
            },
            "最大值": {
                "数值": float(np.max(values_num)),
                "时间": ts_clean[int(np.argmax(values_num))],
            },
        }
        # 添加最早值和最新值
        stats["最早值"] = float(values_num[0])
        stats["最早值时间"] = ts_clean[0]
        stats["最新值"] = float(values_num[-1])
        stats["最新值时间"] = ts_clean[-1]

        # 显著变化
        diff = np.diff(values_num)
        threshold = max(abs(mean_v) * 0.25, std_v * 1.5 + 1e-8)
        change_idx = np.where(np.abs(diff) > threshold)[0]
        changes = [
            {
                "时间": ts_clean[i + 1],
                "从": float(values_num[i]),
                "到": float(values_num[i + 1]),
                "变化量": float(diff[i]),
            }
            for i in change_idx[:50]
        ]
        # 异常值
        anomalies_mask = np.abs(values_num - mean_v) > max(5 * std_v, 1e-8)
        anomalies = [
            {"时间": ts_clean[i], "数值": float(v)}
            for i, v in enumerate(values_num)
            if anomalies_mask[i]
        ]
        stable_periods = []
        if enable_stable_periods and n > 1:
            stable_periods = fast_rle(np.round(values_num, 6).tolist(), ts_clean, 500)
        trend = (
            "上升"
            if values_num[-1] > values_num[0]
            else "下降" if values_num[-1] < values_num[0] else "平稳"
        )
        return {
            "类型": "数值",
            **stats,
            "重要变化": changes,
            "稳定阶段": stable_periods,
            "异常值": anomalies,
            "趋势": trend,
        }
    except Exception as e:
        return {"类型": "未知", "示例": str(val_clean[:5]), "错误": str(e)}


class TongfengService:
    def __init__(self, client):
        self.client = client
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

            # ====================== 时间处理（精确到时分秒，不自动补00:00:00）======================
            if start_date is None:
                start_date = datetime.now()
            if end_date is None:
                end_date = start_date

            # 允许字符串或datetime两种传入，支持带时分秒
            if isinstance(start_date, str):
                try:
                    start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    start_date = datetime.strptime(start_date, "%Y-%m-%d")
            if isinstance(end_date, str):
                try:
                    end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    end_date = datetime.strptime(end_date, "%Y-%m-%d")

            # 满足 start_date <= end_date
            if start_date > end_date:
                raise ValueError("start_date 不能晚于 end_date")

            # 日期分组：按照天分组
            date_range = []
            current = start_date.date()
            end_day = end_date.date()
            while current <= end_day:
                date_range.append(current)
                current = current + timedelta(days=1)

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
            start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
            # 注意：end_str就是end_date本身，不自动 +1天，左闭右开，end_date为截止点
            end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

            query = f"""
                SELECT {",".join(fields_list)}
                FROM PS.SDI_TONG_FENG_XI_TONG
                WHERE TF_TIMESTAMP >= toDateTime('{start_str}')
                  AND TF_TIMESTAMP < toDateTime('{end_str}')
                ORDER BY TF_TIMESTAMP ASC
            """

            print(f"开始查询 {len(fields_list)} 个字段...{query}")
            result = self.client.query(query)
            rows = result.result_rows
            if not rows:
                print("无数据")
                return {}

            col_names = result.column_names
            col_index = {col: idx for idx, col in enumerate(col_names)}
            all_timestamps = [row[col_index["TF_TIMESTAMP"]] for row in rows]

            # ====================== 按天分组数据 ======================

            naive_start_date = to_naive(start_date)
            naive_end_date = to_naive(end_date)

            daily_data = {d: [] for d in date_range}
            for row in rows:
                ts = row[col_index["TF_TIMESTAMP"]]
                # 保证 ts 是naive
                ts_naive = to_naive(ts)
                # 只统计在开始、结束之间的数据
                if ts_naive < naive_start_date or ts_naive >= naive_end_date:
                    continue
                daily_data[ts_naive.date()].append(row)

            # ====================== 分析 ======================
            output_dict = {}

            for system_name in valid_systems:
                output_dict[system_name] = {}
                subgroups = fan_system[system_name]
                print(system_name)
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

                            subgroup_result[cn_desc] = {

                                "数据类型": "daily_numeric",
                                "每日数据": daily_analyses,
                                "总共天数": len([d for d in daily_analyses if daily_analyses[d]])
                            }
                        else:
                            # 原有单次分析逻辑
                            values = [row[idx] for row in rows]
                            # 合并只统计在范围内的时间
                            if value_filters and en_key in value_filters:
                                op, threshold = value_filters[en_key]
                                # 优化过滤逻辑，保证时间对比和时区一致性（全部转为naive）
                                naive_start = to_naive(start_date)
                                naive_end = to_naive(end_date)
                                filtered = [
                                    (t, v) for t, v in zip(all_timestamps, values)
                                    if (
                                            self._match_value_filter(v, op, threshold)
                                            and (to_naive(t) >= naive_start and to_naive(t) < naive_end)
                                    )
                                ]

                                if not filtered:
                                    continue
                                f_ts = [t for t, v in filtered]
                                f_val = [v for t, v in filtered]
                                analysis = analyze_series(f_val, f_ts, enable_stable_periods)
                            else:
                                # 只分析在范围内的全部数据
                                # 为防止时区问题，先全部转为naive时间进行比较

                                naive_start = to_naive(start_date)
                                naive_end = to_naive(end_date)
                                filt_idx = [i for i, t in enumerate(all_timestamps) if
                                            to_naive(t) >= naive_start and to_naive(t) < naive_end]

                                filt_ts = [all_timestamps[i] for i in filt_idx]
                                filt_vals = [values[i] for i in filt_idx]
                                analysis = analyze_series(filt_vals, filt_ts, enable_stable_periods)

                            subgroup_result[cn_desc] = {
                                "数据": analysis,
                            }

                    if subgroup_result:
                        output_dict[system_name][subgroup_name] = subgroup_result

            return output_dict


        except Exception as e:
            print(f"查询异常: {e} {traceback.format_exc()}")

            return {}

    def calc_gonglv_energy(
            self,
            start_date: Union[str, datetime, None] = None,
            end_date: Union[str, datetime, None] = None,
    ) -> Dict:
        """
        功率能耗计算。@tongfeng_system.yaml (254-259)
        - 如果跨天，计算每天的能耗
        - 每天每小时的能耗
        - 每天的用平均值乘以时间
        - 每小时的用平均值乘以一小时
        """

        power_keys = [
            "TF_YH_1_JI_GONG_LV_SHI_JI_ZHI",
            "TF_YH_2_JI_GONG_LV_SHI_JI_ZHI",
            "TF_EH_1_JI_GONG_LV_SHI_JI_ZHI",
            "TF_EH_2_JI_GONG_LV_SHI_JI_ZHI",
        ]
        # --- 时间处理 ---
        if start_date is None:
            start_date = datetime.now()
        if end_date is None:
            end_date = start_date
        if isinstance(start_date, str):
            try:
                start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            try:
                end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                end_date = datetime.strptime(end_date, "%Y-%m-%d")
        if start_date > end_date:
            raise ValueError("start_date 不能晚于 end_date")
        # 日期分组
        date_range = []
        current = start_date.date()
        end_day = end_date.date()
        while current <= end_day:
            date_range.append(current)
            current = current + timedelta(days=1)
        # 涉及字段
        fields = ["TF_TIMESTAMP"] + power_keys
        # 查询
        start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
        query = f"""
            SELECT {",".join(fields)}
            FROM PS.SDI_TONG_FENG_XI_TONG
            WHERE TF_TIMESTAMP >= toDateTime('{start_str}')
              AND TF_TIMESTAMP < toDateTime('{end_str}')
            ORDER BY TF_TIMESTAMP ASC
        """
        result = self.client.query(query)
        rows = result.result_rows
        if not rows:
            print("无数据")
            return {}
        col_names = result.column_names
        col_index = {col: idx for idx, col in enumerate(col_names)}
        # 按天分组
        daily_data = defaultdict(list)
        hourly_data = defaultdict(lambda: defaultdict(list))  # day -> hour -> rows
        for row in rows:
            ts = row[col_index["TF_TIMESTAMP"]]
            ts_naive = to_naive(ts)
            day = ts_naive.date()
            hour = ts_naive.hour
            daily_data[day].append(row)
            hourly_data[day][hour].append(row)
        # 计算能耗
        out = {}
        for day in daily_data:
            rows_day = daily_data[day]
            if not rows_day:
                continue
            out_day = {"总平均功率": {}, "总能耗kWh": {}, "逐小时能耗kWh": {}}
            day_start = datetime.combine(day, datetime.min.time())
            day_end = day_start + timedelta(days=1)
            seconds = (min(day_end, end_date) - max(day_start, start_date)).total_seconds()
            # 每台设备平均功率
            for pk in power_keys:
                idx = col_index.get(pk)
                vals = [
                    float(row[idx]) for row in rows_day
                    if row[idx] is not None and str(row[idx]).strip() != ""
                ]
                avg_power = float(np.mean(vals)) if vals else 0.0
                out_day["总平均功率"][pk] = avg_power
                # day能耗 = 平均功率(千瓦) * 实际小时数
                total_hours = seconds / 3600.0
                out_day["总能耗kWh"][pk] = round(avg_power * total_hours, 2)
                # 小时能耗
                hourly = {}
                for h in range(24):
                    ts_h = datetime.combine(day, datetime.min.time()) + timedelta(hours=h)
                    ts_h_end = ts_h + timedelta(hours=1)
                    # 判断本小时是否在整体统计时间内
                    stat_start = max(ts_h, start_date)
                    stat_end = min(ts_h_end, end_date)
                    if stat_start >= stat_end:
                        continue
                    hour_rows = [row for row in hourly_data[day].get(h, []) if
                                 to_naive(row[col_index["TF_TIMESTAMP"]]) >= stat_start and
                                 to_naive(row[col_index["TF_TIMESTAMP"]]) < stat_end
                                 ]
                    vals_h = [
                        float(row[idx]) for row in hour_rows
                        if row[idx] is not None and str(row[idx]).strip() != ""
                    ]
                    # 如果没有这个小时的数据则为0
                    avg_h = float(np.mean(vals_h)) if vals_h else 0.0
                    # 本小时有效时间（秒），考虑跨天首尾裁剪
                    seconds_h = (stat_end - stat_start).total_seconds()
                    kwh = round(avg_h * (seconds_h / 3600.0), 2)
                    if seconds_h > 0:
                        hourly[f"{h:02d}:00-{h + 1:02d}:00"] = {"平均功率": avg_h, "能耗_kWh": kwh}
                out_day["逐小时能耗kWh"][pk] = hourly
            out[day.strftime("%Y-%m-%d")] = out_day
        return out
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
        start_date="2026-05-01 00:00:00",
        end_date="2026-05-10 12:00:00",
        # subgroup_filters=["切换过程"],
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
