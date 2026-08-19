import traceback
import clickhouse_connect
from datetime import datetime, timedelta
import os
import yaml
from typing import List, Dict, Optional, Union
import numpy as np
import pandas as pd
from tqdm import tqdm

# 统一时间对象的时区类型为"offset-naive"
def to_naive(dt):
    if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

def fast_rle_numpy(values: np.ndarray, timestamps: pd.DatetimeIndex, max_groups: int = 500):
    """
    使用 NumPy 向量化重写的高性能 RLE (游程编码)
    速度比 Python 的 for 循环逐个对比快数百倍
    """
    if len(values) == 0:
        return []
        
    # 找到值发生变化的索引位置
    changes = np.where(values[:-1] != values[1:])[0] + 1
    # 拼接首尾索引
    split_idx = np.concatenate(([0], changes, [len(values)]))
    
    groups = []
    # 这里循环次数极少（仅等于变化次数），所以 Python 循环开销可忽略
    for i in range(len(split_idx) - 1):
        start = split_idx[i]
        end = split_idx[i + 1] - 1
        
        duration = (timestamps[end] - timestamps[start]).total_seconds()
        if duration >= 20:
            groups.append({
                "取值": float(values[start]) if isinstance(values[start], (np.floating, float)) else values[start],
                "起始时间": timestamps[start].to_pydatetime(),
                "结束时间": timestamps[end].to_pydatetime(),
                "持续秒数": duration,
                "持续小时": round(duration / 3600, 2),
            })
            if len(groups) >= max_groups:
                break
    return groups

def analyze_series_vectorized(
    series: pd.Series, enable_stable_periods: bool = True
) -> Dict:
    """
    使用 Pandas/NumPy 向量化重写的高性能分析函数
    """
    # 剔除空值
    series = series.dropna()
    if series.empty:
        return {"类型": "未知", "信息": "无有效数据"}

    ts_clean = series.index
    val_clean = series.values

    # ========================= Bool / String =========================
    # 简单判定：如果去重后的值极少且包含常见布尔字符
    unique_vals = pd.unique(val_clean)
    is_bool = False
    if len(unique_vals) <= 4:
        str_vals = [str(v).lower() for v in unique_vals]
        if all(v in ("true", "false", "0", "1", "0.0", "1.0") for v in str_vals):
            is_bool = True

    if is_bool:
        bool_map = {True: "真", False: "假", 1: "真", 0: "假", "1.0": "真", "0.0": "假", "1": "真", "0": "假"}
        values_str = np.array([bool_map.get(v, str(v)) for v in val_clean])
        
        # 寻找变化点
        changes_idx = np.where(values_str[:-1] != values_str[1:])[0] + 1
        changes = []
        
        for idx in changes_idx:
            prev = values_str[idx - 1]
            curr = values_str[idx]
            duration = (ts_clean[idx] - ts_clean[idx - 1]).total_seconds()
            changes.append({
                "变化时间": ts_clean[idx].strftime("%Y-%m-%d %H:%M:%S"),
                "从": prev,
                "到": curr,
                "持续秒数": duration,
                "持续小时": round(duration / 3600, 2),
            })
            
        return {
            "类型": "布尔",
            "当前值": values_str[-1],
            "最早值": values_str[0],
            "最新值": values_str[-1],
            "最早值时间": ts_clean[0].to_pydatetime(),
            "最新值时间": ts_clean[-1].to_pydatetime(),
            "变化": changes or "无变化",
            "稳定阶段": fast_rle_numpy(values_str, ts_clean) if enable_stable_periods else [],
            "变化次数": len(changes),
        }

    # ========================= Numeric =========================
    try:
        # 强制转为 float64
        values_num = np.asarray(val_clean, dtype=np.float64)
        n = len(values_num)
        
        mean_v = float(np.mean(values_num))
        std_v = float(np.std(values_num)) if n > 1 else 0.0
        min_idx = int(np.argmin(values_num))
        max_idx = int(np.argmax(values_num))

        stats = {
            "个数": int(n),
            "平均值": round(mean_v, 4),
            "中位数": round(float(np.median(values_num)), 4),
            "标准差": round(std_v, 4),
            "最小值": {
                "数值": float(values_num[min_idx]),
                "时间": ts_clean[min_idx].to_pydatetime(),
            },
            "最大值": {
                "数值": float(values_num[max_idx]),
                "时间": ts_clean[max_idx].to_pydatetime(),
            },
            "最早值": float(values_num[0]),
            "最早值时间": ts_clean[0].to_pydatetime(),
            "最新值": float(values_num[-1]),
            "最新值时间": ts_clean[-1].to_pydatetime(),
        }

        # 显著变化 (向量化计算)
        diff = np.diff(values_num)
        threshold = max(abs(mean_v) * 0.25, std_v * 1.5 + 1e-8)
        change_idx = np.where(np.abs(diff) > threshold)[0]
        
        changes = [
            {
                "时间": ts_clean[i + 1].to_pydatetime(),
                "从": float(values_num[i]),
                "到": float(values_num[i + 1]),
                "变化量": float(diff[i]),
            }
            for i in change_idx[:50]  # 限制前50个避免刷屏
        ]

        # 异常值 (向量化掩码)
        anomalies_mask = np.abs(values_num - mean_v) > max(5 * std_v, 1e-8)
        anomalies_idx = np.where(anomalies_mask)[0]
        anomalies = [
            {"时间": ts_clean[i].to_pydatetime(), "数值": float(values_num[i])}
            for i in anomalies_idx[:50]
        ]

        stable_periods = []
        if enable_stable_periods and n > 1:
            # 舍入后做 RLE
            rounded_vals = np.round(values_num, 6)
            stable_periods = fast_rle_numpy(rounded_vals, ts_clean, 500)

        trend = "平稳"
        if values_num[-1] > values_num[0]: trend = "上升"
        elif values_num[-1] < values_num[0]: trend = "下降"

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

    def _load_yaml(self):
        if self._fan_system_cache is None:
            yaml_path = os.path.join(os.path.dirname(__file__), "tongfeng_system.yaml")
            with open(yaml_path, "r", encoding="utf-8") as f:
                self._fan_system_cache = yaml.safe_load(f)
        return self._fan_system_cache

    @staticmethod
    def _apply_pandas_filter(series: pd.Series, op: str, threshold: float) -> pd.Series:
        """Pandas 向量化条件过滤"""
        series = pd.to_numeric(series, errors='coerce').dropna()
        if op == ">": return series[series > threshold]
        elif op == ">=": return series[series >= threshold]
        elif op == "<": return series[series < threshold]
        elif op == "<=": return series[series <= threshold]
        elif op == "=": return series[series == threshold]
        return series

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

            # 时间处理
            if start_date is None: start_date = datetime.now()
            if end_date is None: end_date = start_date

            if isinstance(start_date, str):
                start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S") if ":" in start_date else datetime.strptime(start_date, "%Y-%m-%d")
            if isinstance(end_date, str):
                end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S") if ":" in end_date else datetime.strptime(end_date, "%Y-%m-%d")

            if start_date > end_date:
                raise ValueError("start_date 不能晚于 end_date")

            start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
            end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

            # 收集字段
            need_fields = {"TF_TIMESTAMP"}
            if subgroup_filters and value_filters:
                need_fields.update(value_filters.keys())
            elif subgroup_filters:
                for sys_name in valid_systems:
                    for subgroup_name, subgroup in fan_system[sys_name].items():
                        if subgroup_name in subgroup_filters:
                            need_fields.update(subgroup.keys())
            else:
                for sys_name in valid_systems:
                    for subgroup in fan_system[sys_name].values():
                        need_fields.update(subgroup.keys())

            fields_list = list(need_fields)

            # ====================== 核心优化 1: query_df 直接获取 DataFrame ======================
            query = f"""
                SELECT {",".join(fields_list)}
                FROM PS.SDI_TONG_FENG_XI_TONG
                WHERE TF_TIMESTAMP >= toDateTime('{start_str}')
                  AND TF_TIMESTAMP < toDateTime('{end_str}')
                ORDER BY TF_TIMESTAMP ASC
            """

            print(f"开始使用向量化引擎查询 {len(fields_list)} 个字段...")
            df = self.client.query_df(query)
            print('查询完成')
            if df.empty:
                print("无数据")
                return {}

            # 清理时间轴并设为索引
            if df['TF_TIMESTAMP'].dt.tz is not None:
                df['TF_TIMESTAMP'] = df['TF_TIMESTAMP'].dt.tz_localize(None)
            df.set_index("TF_TIMESTAMP", inplace=True)

            # 判断是否跨天
            date_range = pd.date_range(start=start_date.date(), end=end_date.date(), freq='D')
            is_multi_day = len(date_range) > 1

            output_dict = {}

            for system_name in valid_systems:
                output_dict[system_name] = {}
                subgroups = fan_system[system_name]
                print(f"处理系统: {system_name}")

                for subgroup_name, metas in subgroups.items():
                    if subgroup_filters and subgroup_name not in subgroup_filters:
                        continue

                    subgroup_result = {}
                    is_numeric_subgroup = (subgroup_name == "数值")
                    print(f"  子组: {subgroup_name} {'[按天统计]' if is_numeric_subgroup and is_multi_day else ''}")

                    for en_key, cn_desc in metas.items():
                        if en_key == "TF_TIMESTAMP" or en_key not in df.columns:
                            continue
                        if value_filters and en_key not in value_filters:
                            continue

                        # 提取单列时间序列
                        series = df[en_key]

                        # 按天统计逻辑
                        if is_numeric_subgroup and is_multi_day:
                            daily_analyses = {}
                            # ====================== 核心优化 2: Pandas 高速按天分组 ======================
                            for day_date, group in series.groupby(series.index.date):
                                if value_filters and en_key in value_filters:
                                    op, threshold = value_filters[en_key]
                                    group = self._apply_pandas_filter(group, op, threshold)

                                if not group.empty:
                                    analysis = analyze_series_vectorized(group, enable_stable_periods)
                                    daily_analyses[day_date.strftime("%Y-%m-%d")] = analysis

                            subgroup_result[cn_desc] = {
                                "数据类型": "daily_numeric",
                                "每日数据": daily_analyses,
                                "总共天数": len(daily_analyses)
                            }
                        else:
                            # 整体统计
                            if value_filters and en_key in value_filters:
                                op, threshold = value_filters[en_key]
                                series = self._apply_pandas_filter(series, op, threshold)

                            if not series.empty:
                                analysis = analyze_series_vectorized(series, enable_stable_periods)
                                subgroup_result[cn_desc] = {"数据": analysis}

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
        利用 ClickHouse 引擎进行底层聚合，极速返回能耗结果
        原 Python 循环耗时 O(N)，现为 O(1) 网络传输级别
        """
        power_keys = [
            "TF_YH_1_JI_GONG_LV_SHI_JI_ZHI",
            "TF_YH_2_JI_GONG_LV_SHI_JI_ZHI",
            "TF_EH_1_JI_GONG_LV_SHI_JI_ZHI",
            "TF_EH_2_JI_GONG_LV_SHI_JI_ZHI",
        ]
        power_keyvalues = {
            'TF_YH_1_JI_GONG_LV_SHI_JI_ZHI': '一号风机1级设备',
            'TF_YH_2_JI_GONG_LV_SHI_JI_ZHI': '一号风机2级设备',
            'TF_EH_1_JI_GONG_LV_SHI_JI_ZHI': '二号风机1级设备',
            'TF_EH_2_JI_GONG_LV_SHI_JI_ZHI': '二号风机2级设备',
        }
        if start_date is None: start_date = datetime.now()
        if end_date is None: end_date = start_date
        
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S") if ":" in start_date else datetime.strptime(start_date, "%Y-%m-%d")
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S") if ":" in end_date else datetime.strptime(end_date, "%Y-%m-%d")

        if start_date > end_date:
            raise ValueError("start_date 不能晚于 end_date")

        start_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
        end_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

        # ====================== 核心优化 3: ClickHouse 引擎层按天、按小时聚合 ======================


        avg_fields = []

        for field in power_keys:
            avg_fields.append(
                f"""
                avg(
                    toFloat64OrNull({field})
                ) as {field}
                """
            )

        query = f"""
        SELECT
            toStartOfDay(TF_TIMESTAMP) as day_ts,
            toStartOfHour(TF_TIMESTAMP) as hour_ts,
            {",".join(avg_fields)}
        FROM PS.SDI_TONG_FENG_XI_TONG
        WHERE TF_TIMESTAMP >= toDateTime('{start_str}')
          AND TF_TIMESTAMP < toDateTime('{end_str}')
        GROUP BY day_ts, hour_ts
        ORDER BY day_ts ASC, hour_ts ASC
        """
        
        result = self.client.query(query)
        rows = result.result_rows
        if not rows:
            print("无数据")
            return {}

        out = {}
        # 此时的 rows 是已经聚合过的高密度小数据（每天最多 24 行），极速遍历
        for row in tqdm(rows, desc="装载聚合数据"):
            day_ts = to_naive(row[0]).date()
            hour_ts = to_naive(row[1])
            hour = hour_ts.hour
            day_str = day_ts.strftime("%Y-%m-%d")

            if day_str not in out:
                out[day_str] = {
                    "总平均功率_kw": {},
                    "总能耗kWh": {},
                    "逐小时能耗kWh": {
                        power_keyvalues.get(k, k): {}
                        for k in power_keys
                    }
                }

            # ClickHouse 查出的每一行代表一个小时的平均值
            # 合并同一天同设备|变量下相邻小时“平均功率_kw”前后一致的区间
            for i, pk in enumerate(power_keys):
                avg_power = float(row[i+2]) if row[i+2] is not None else 0.0
                avg_power = round(avg_power, 2)
                pk_value = power_keyvalues.get(pk, pk)
                hour_start = max(hour_ts, start_date)
                hour_end = min(hour_ts + timedelta(hours=1), end_date)
                seconds_h = max(0, (hour_end - hour_start).total_seconds())
                kwh = round(avg_power * (seconds_h / 3600.0), 2)
                if seconds_h > 0:
                    merge_dict = out[day_str]["逐小时能耗kWh"][pk_value]
                    hour_label = f"{hour:02d}:00-{hour+1:02d}:00"
                    if not merge_dict:
                        # 第一个小时直接写
                        merge_dict[hour_label] = {
                            "平均功率_kw": avg_power,
                            "能耗_KWh": kwh,
                            "开始小时": hour,
                            "结束小时": hour+1,
                            "累计能耗_kWh": kwh,
                            "累计秒数": seconds_h
                        }
                    else:
                        # 取上一个合并条目的key
                        last_key = list(merge_dict.keys())[-1]
                        last_item = merge_dict[last_key]
                        if "结束小时" not in last_item:
                            last_item["结束小时"] = int(last_key[6:8])  # 如 "01:00-02:00"
                            last_item["能耗_KWh"] = kwh
                        # 合并条件：平均功率_kw相等，且小时连续
                        if last_item["平均功率_kw"] == avg_power and last_item["结束小时"] == hour:
                            # 合并，扩展累计时间和累计能耗
                            last_item["结束小时"] = hour + 1
                            last_item["能耗_KWh"] = kwh
                            last_item["累计能耗_kWh"] += kwh
                            last_item["累计秒数"] += seconds_h
                            # 更新合并时段的key
                            new_label = f"{last_item['开始小时']:02d}:00-{last_item['结束小时']:02d}:00"
                            # 重命名key
                            merge_dict[new_label] = merge_dict.pop(last_key)
                        else:
                            # 不相等，新开一段
                            merge_dict[hour_label] = {
                                "平均功率_kw": avg_power,
                                "能耗_KWh": kwh,
                                "开始小时": hour,
                                "结束小时": hour+1,
                                "累计能耗_kWh": kwh,
                                "累计秒数": seconds_h
                            }
                
        # 按照上面结构，整理当日总能耗与总平均功率
        for day_str, day_data in out.items():
            for pk in power_keys:
                pk_value = power_keyvalues.get(pk, pk)
                hourly_dict = day_data["逐小时能耗kWh"].get(pk_value, {})
                if hourly_dict:
                    avg_powers = [h.get("平均功率_kw", 0) for h in hourly_dict.values()]
                    # INSERT_YOUR_CODE
                    # 只保留 "平均功率_kw" 和 "能耗_KWh" 字段
                    for key, val in list(hourly_dict.items()):

                        if isinstance(val, dict):
                            keys_to_keep = {"平均功率_kw", "能耗_KWh"}
                            keys_to_delete = [k for k in val.keys() if k not in keys_to_keep]
                         
                            for k in keys_to_delete:
                                del val[k]
           
                    total_kwh = sum(h.get("能耗_kWh", h.get("能耗_KWh", 0)) for h in hourly_dict.values())
                    if avg_powers:
                        day_data["总平均功率_kw"][pk_value] = round(sum(avg_powers) / len(avg_powers), 2)
                    else:
                        day_data["总平均功率_kw"][pk_value] = 0.0
                    day_data["总能耗kWh"][pk_value] = round(total_kwh, 2)
 

        return out

    def close(self):
        self.client.close()

if __name__ == "__main__":
    import json


    def json_serializer(obj):
        if isinstance(obj, datetime):
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        raise TypeError(f"Type {type(obj)} not serializable")

    client = clickhouse_connect.get_client(
        host="10.11.22.80",
        port=9120,
        username="nethouse",
        password="CGC%EVXr.ET10Y_N",
        secure=True,
        verify=False,
    )
    service = TongfengService(client)
    
    # 极速测试 1: YAML配置向量化检索分析
    # print("----- 测试: print_tongfeng_today_by_yaml_system -----")
    # result_yaml = service.print_tongfeng_today_by_yaml_system(
    #     system_name_filters=["一号风机系统"],
    #     start_date="2026-04-01 00:00:00",
    #     end_date="2026-05-10 12:00:00",
    #     enable_stable_periods=True,
    # )
    # result_json = json.dumps(result_yaml, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
    # print("YAML 字典构建完成，Keys:", result_yaml.keys(),'返回数据长度--',len(result_json))

    # 极速测试 2: SQL下推能耗计算
    print("\n----- 测试: calc_gonglv_energy -----")
    result_energy = service.calc_gonglv_energy(
        start_date="2026-04-01 00:00:00",
        end_date="2026-05-11 12:00:00",
    )
    result_energy_json = json.dumps(result_energy, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
    print(f"能耗计算完成，共涉及天数:{len(result_energy)} 返回数据长度{len(result_energy_json)}")
    
    service.close()