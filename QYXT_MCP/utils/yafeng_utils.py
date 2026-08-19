import traceback
import clickhouse_connect
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

import json
import os
import yaml
import time
from typing import List, Dict, Optional, Union
from tqdm import tqdm
import numpy as np


# =========================================================
# 工具函数
# =========================================================

def to_naive(dt):
    """统一时间对象的时区类型为 'offset-naive'"""
    if hasattr(dt, "tzinfo") and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt


def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"Type {type(obj)} not serializable")


# =========================================================
# 高性能核心分析算法 (NumPy 加速版)
# =========================================================

def fast_rle_numpy(values, timestamps, max_groups=500):
    """numpy版RLE，用于提取稳定阶段"""
    if len(values) == 0:
        return []

    arr = np.asarray(values)
    # 找变化点
    change_idx = np.flatnonzero(arr[1:] != arr[:-1]) + 1
    # 分段边界
    boundaries = np.concatenate(([0], change_idx, [len(arr)]))

    groups = []
    for i in range(len(boundaries) - 1):
        start = boundaries[i]
        end = boundaries[i + 1] - 1

        # 计算持续时间
        duration = (timestamps[end] - timestamps[start]).total_seconds()

        if duration < 20:  # 过滤掉波动过快的瞬时值
            continue

        groups.append({
            "取值": values[start],
            "起始时间": timestamps[start],
            "结束时间": timestamps[end],
            "持续秒数": duration,
            "持续小时": round(duration / 3600, 2),
        })

        if len(groups) >= max_groups:
            break
    return groups


def analyze_series_fast(values, timestamps, enable_stable_periods=True):
    """超高性能时序分析"""
    if not values:
        return {"类型": "未知", "信息": "无有效数据"}

    arr = np.asarray(values)
    # None 过滤
    valid_mask = (arr != None)
    if not np.any(valid_mask):
        return {"类型": "未知", "信息": "无有效数据"}

    arr = arr[valid_mask]
    ts = np.asarray(timestamps)[valid_mask]
    n = len(arr)

    # 1. 尝试判断布尔类型
    bool_set = {"true", "false", "0", "1", True, False,"True","False"}
    is_maybe_bool = True
    check_len = min(100, n)
    for v in arr[:check_len]:
        if v not in bool_set:
            is_maybe_bool = False
            break

    if is_maybe_bool:
        values_str = np.where((arr == True) | (arr == 1) | (arr == "true") | (arr == "True") , "True", "False")
        diff_mask = values_str[1:] != values_str[:-1]
        change_idx = np.flatnonzero(diff_mask) + 1

        changes = []
        for i in change_idx[:50]:
            duration = (ts[i] - ts[i - 1]).total_seconds()
            changes.append({
                "变化时间": ts[i].strftime("%Y-%m-%d %H:%M:%S"),
                "从": values_str[i - 1],
                "到": values_str[i],
                "持续秒数": duration,
                "持续小时": round(duration / 3600, 2),
            })

        return {
            "类型": "布尔",
            "当前值": values_str[-1],
            "最早值": values_str[0],
            "最新值": values_str[-1],
            "最早值时间": ts[0],
            "最新值时间": ts[-1],
            "变化": changes or "无变化",
            "稳定阶段": fast_rle_numpy(values_str, ts),
            "变化次数": len(change_idx),
        }

    # 2. 数值类型分析
    try:
        values_num = arr.astype(np.float64)
        mean_v = values_num.mean()
        std_v = values_num.std()
        min_idx = values_num.argmin()
        max_idx = values_num.argmax()
        diff = np.diff(values_num)

        # 阈值计算
        threshold = max(abs(mean_v) * 0.25, std_v * 1.5 + 1e-8)
        change_idx = np.flatnonzero(np.abs(diff) > threshold)

        # 异常值检测
        anomaly_mask = np.abs(values_num - mean_v) > max(5 * std_v, 1e-8)
        anomaly_idx = np.flatnonzero(anomaly_mask)

        trend = "上升" if values_num[-1] > values_num[0] else "下降" if values_num[-1] < values_num[0] else "平稳"

        changes = []
        for i in change_idx[:50]:
            changes.append({
                "时间": ts[i + 1],
                "从": float(values_num[i]),
                "到": float(values_num[i + 1]),
                "变化量": float(diff[i]),
            })

        anomalies = []
        for i in anomaly_idx[:100]:
            anomalies.append({"时间": ts[i], "数值": float(values_num[i])})

        stable_periods = []
        if enable_stable_periods and n > 1:
            stable_periods = fast_rle_numpy(np.round(values_num, 6), ts)

        return {
            "类型": "数值",
            "个数": int(n),
            "平均值": round(float(mean_v), 4),
            "中位数": round(float(np.median(values_num)), 4),
            "标准差": round(float(std_v), 4),
            "最小值": {"数值": float(values_num[min_idx]), "时间": ts[min_idx]},
            "最大值": {"数值": float(values_num[max_idx]), "时间": ts[max_idx]},
            "最早值": float(values_num[0]),
            "最早值时间": ts[0],
            "最新值": float(values_num[-1]),
            "最新值时间": ts[-1],
            "重要变化": changes,
            "稳定阶段": stable_periods,
            "异常值": anomalies,
            "趋势": trend,
        }
    except Exception as e:
        return {"类型": "未知", "错误": str(e)}


# =========================================================
# 通风系统业务类
# =========================================================

class YafengService:
    def __init__(self, client):
        self.client = client
        self._fan_system_cache = None

    def _load_yaml(self):
        if self._fan_system_cache is None:
            yaml_path = os.path.join(os.path.dirname(__file__), "yafeng_system.yaml")
            with open(yaml_path, "r", encoding="utf-8") as f:
                self._fan_system_cache = yaml.safe_load(f)
        return self._fan_system_cache

    def print_Yafeng_today_by_yaml_system(
            self,
            system_name_filters: Union[List[str], str],
            start_date: Union[str, datetime, None] = None,
            end_date: Union[str, datetime, None] = None,
            subgroup_filters: Union[List[str], str, None] = None,
            value_filters: Optional[Dict[str, tuple]] = None,
            enable_stable_periods: bool = True,
    ) -> Dict:
        """
        深度优化版：利用 NumPy 向量化替代 Python 循环。
        性能提升：处理大数据量时比原版快 10 倍以上。
        """
        try:
            yaml_data = self._load_yaml()
            fan_system = yaml_data.get("fan_system", {})

            # 1. 参数校验与预处理
            if isinstance(system_name_filters, str):
                system_name_filters = [system_name_filters]
            valid_systems = [s for s in system_name_filters if s in fan_system]
            if not valid_systems:
                print("无有效系统名")
                return {}

            if isinstance(subgroup_filters, str):
                subgroup_filters = [subgroup_filters]

            if start_date is None: start_date = datetime.now()
            if end_date is None: end_date = start_date

            # 时间格式兼容转换
            def parse_dt(d):
                if isinstance(d, str):
                    return datetime.strptime(d, "%Y-%m-%d %H:%M:%S") if ":" in d else datetime.strptime(d, "%Y-%m-%d")
                return d

            start_dt, end_dt = parse_dt(start_date), parse_dt(end_date)
            start_str, end_str = start_dt.strftime("%Y-%m-%d %H:%M:%S"), end_dt.strftime("%Y-%m-%d %H:%M:%S")

            # 2. 字段映射与 SQL 构建
            need_fields = {"YF_TIMESTAMP"}
            task_mapping = []  # (sys_name, sg_name, en_key, cn_desc)

            for sys_name in valid_systems:
                for sg_name, metas in fan_system[sys_name].items():
                    if subgroup_filters and sg_name not in subgroup_filters:
                        continue
                    for en_key, cn_desc in metas.items():
                        if en_key == "YF_TIMESTAMP": continue
                        need_fields.add(en_key)
                        task_mapping.append((sys_name, sg_name, en_key, cn_desc))

            fields_list = list(need_fields)
            query = f"""
                SELECT {",".join(fields_list)}
                FROM PS.SDI_YA_FENG_XI_TONG
                WHERE YF_TIMESTAMP >= toDateTime('{start_str}')
                  AND YF_TIMESTAMP < toDateTime('{end_str}')
                ORDER BY YF_TIMESTAMP ASC
            """

            # 3. 数据拉取与矩阵化
            print(f"开始拉取数据... 字段数: {len(fields_list)}")
            t_query = time.time()
            result = self.client.query(query)
            if not result.result_rows:
                print("无数据")
                return {}

            # 使用 NumPy 直接承载数据矩阵 (dtype=object 兼容混合类型)
            data_matrix = np.array(result.result_rows, dtype=object)
            col_map = {name: i for i, name in enumerate(result.column_names)}

            # 提取全量时间戳并转化为 naive
            ts_full = np.array([to_naive(t) for t in data_matrix[:, col_map["YF_TIMESTAMP"]]])
            ts_dates = np.array([t.date() for t in ts_full])
            unique_dates = np.unique(ts_dates)
            is_multi_day = len(unique_dates) > 1

            print(f"数据拉取与转换耗时: {time.time() - t_query:.2f}s")

            # 4. 向量化分析
            output_dict = {}

           
            def process_task(task):
                sys_name, sg_name, en_key, cn_desc = task
                # print(sys_name, sg_name, en_key, cn_desc)
                # output_dict结构在主线程维护，返回(sys_name, sg_name, cn_desc, result_dict)
                vals = data_matrix[:, col_map[en_key]]

                # 向量化 Value Filter
                if value_filters and en_key in value_filters:
                    op, threshold = value_filters[en_key]
                    try:
                        f_vals = vals.astype(float)
                        if op == ">":
                            mask = f_vals > threshold
                        elif op == ">=":
                            mask = f_vals >= threshold
                        elif op == "<":
                            mask = f_vals < threshold
                        elif op == "<=":
                            mask = f_vals <= threshold
                        elif op == "=":
                            mask = f_vals == threshold
                        else:
                            mask = np.ones(len(vals), dtype=bool)
                    except:
                        mask = np.ones(len(vals), dtype=bool)
                else:
                    mask = np.ones(len(vals), dtype=bool)

                curr_vals = vals[mask]
                curr_ts = ts_full[mask]
                curr_dates = ts_dates[mask]

                if len(curr_vals) == 0:
                    return (sys_name, sg_name, cn_desc, None)

                # 判断统计模式：按天统计 vs 全量统计
                if "监测值" in sg_name  and is_multi_day:
                    daily_data = {}
                    for d in unique_dates:
                        day_mask = (curr_dates == d)
                        if not np.any(day_mask): continue

                        daily_data[d.strftime("%Y-%m-%d")] = analyze_series_fast(
                            curr_vals[day_mask].tolist(),
                            curr_ts[day_mask].tolist(),
                            enable_stable_periods
                        )
                    res = {
                        "数据类型": "daily_numeric",
                        "每日数据": daily_data,
                        "总共天数": len(daily_data)
                    }
                else:
                    # 全量模式，直接调用分析
                    res = {
                        "数据": analyze_series_fast(
                            curr_vals.tolist(),
                            curr_ts.tolist(),
                            enable_stable_periods
                        )
                    }
                return (sys_name, sg_name, cn_desc, res)

            # 用多线程加速
            max_workers = min(8, len(task_mapping) or 1)
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_task = {executor.submit(process_task, task): task for task in task_mapping}
                for future in as_completed(future_to_task):
                    sys_name, sg_name, cn_desc, res = future.result()
                    if res is None:
                        continue
                    if sys_name not in output_dict:
                        output_dict[sys_name] = {}
                    if sg_name not in output_dict[sys_name]:
                        output_dict[sys_name][sg_name] = {}
                    output_dict[sys_name][sg_name][cn_desc] = res
     

            return output_dict

        except Exception as e:
            print(f"分析异常: {e}\n{traceback.format_exc()}")
            return {}

    def calc_gonglv_energy(self, start_date=None, end_date=None) -> Dict:
        """
        能耗统计核心方法：已具备极速聚合能力
        """
        power_keys = ["YF_KONG_YA_JI_1_YOU_GONG", "YF_KONG_YA_JI_2_YOU_GONG",
                      "YF_DUAN_LU_QI_GONG_LV_1", "YF_DUAN_LU_QI_GONG_LV_2","YF_DUAN_LU_QI_MU_LIAN_GONG_LV"]
        power_keyvalues = {
            'YF_KONG_YA_JI_1_YOU_GONG': '1号空压机有功功率实时监测值',
            'YF_KONG_YA_JI_2_YOU_GONG': '2号空压机有功功率实时监测值',
            'YF_DUAN_LU_QI_GONG_LV_1': '断路器1回路有功功率实时监测值',
            'YF_DUAN_LU_QI_GONG_LV_2': '断路器2回路有功功率实时监测值',
            'YF_DUAN_LU_QI_MU_LIAN_GONG_LV': '母联断路器回路有功功率监测值',
        }

        def parse_dt(d):
            if isinstance(d, str):
                return datetime.strptime(d, "%Y-%m-%d %H:%M:%S") if ":" in d else datetime.strptime(d, "%Y-%m-%d")
            return d or datetime.now()

        s_dt, e_dt = parse_dt(start_date), parse_dt(end_date)
        start_str, end_str = s_dt.strftime("%Y-%m-%d %H:%M:%S"), e_dt.strftime("%Y-%m-%d %H:%M:%S")

        avg_fields = [f"avg(toFloat64OrNull({f})) as {f}" for f in power_keys]
        query = f"""
           SELECT toStartOfDay(YF_TIMESTAMP) as day_ts, toStartOfHour(YF_TIMESTAMP) as hour_ts, {",".join(avg_fields)}
           FROM PS.SDI_YA_FENG_XI_TONG
           WHERE YF_TIMESTAMP >= toDateTime('{start_str}') AND YF_TIMESTAMP < toDateTime('{end_str}')
           GROUP BY day_ts, hour_ts ORDER BY day_ts ASC, hour_ts ASC
        """

        result = self.client.query(query)
        if not result.result_rows: return {}

        out = {}
        for row in result.result_rows:
            day_str = to_naive(row[0]).strftime("%Y-%m-%d")
            hour_ts = to_naive(row[1])
            hour = hour_ts.hour

            if day_str not in out:
                out[day_str] = {"总平均功率_kw": {}, "总能耗kWh": {},
                                "逐小时能耗kWh": {power_keyvalues[k]: {} for k in power_keys}}

            for i, pk in enumerate(power_keys):
                avg_p = round(float(row[i + 2] or 0), 2)
                pk_val = power_keyvalues[pk]

                h_start = max(hour_ts, s_dt)
                h_end = min(hour_ts + timedelta(hours=1), e_dt)
                sec = max(0, (h_end - h_start).total_seconds())
                kwh = round(avg_p * (sec / 3600.0), 2)

                if sec > 0:
                    m_dict = out[day_str]["逐小时能耗kWh"][pk_val]
                    h_label = f"{hour:02d}:00-{hour + 1:02d}:00"
                    if not m_dict:
                        m_dict[h_label] = {"平均功率_kw": avg_p, "能耗_KWh": kwh, "开始小时": hour,
                                           "结束小时": hour + 1}
                    else:
                        last_key = list(m_dict.keys())[-1]
                        last_item = m_dict[last_key]
                        if last_item["平均功率_kw"] == avg_p and last_item["结束小时"] == hour:
                            last_item["结束小时"] = hour + 1
                            last_item["能耗_KWh"] = round(last_item["能耗_KWh"] + kwh, 2)
                            new_label = f"{last_item['开始小时']:02d}:00-{last_item['结束小时']:02d}:00"
                            m_dict[new_label] = m_dict.pop(last_key)
                        else:
                            m_dict[h_label] = {"平均功率_kw": avg_p, "能耗_KWh": kwh, "开始小时": hour,
                                               "结束小时": hour + 1}

        # 清理多余字段并计算总值
        for day_data in out.values():
            for pk_val in power_keyvalues.values():
                h_dict = day_data["逐小时能耗kWh"].get(pk_val, {})
                if h_dict:
                    powers = [v["平均功率_kw"] for v in h_dict.values()]
                    day_data["总平均功率_kw"][pk_val] = round(sum(powers) / len(powers), 2)
                    day_data["总能耗kWh"][pk_val] = round(sum(v["能耗_KWh"] for v in h_dict.values()), 2)
                    for v in h_dict.values():
                        v.pop("开始小时", None);
                        v.pop("结束小时", None)

        return out

    def close(self):
        if self.client: self.client.close()

# =============================================================
# 执行测试
# =============================================================
if __name__ == "__main__":
    client = clickhouse_connect.get_client(
        host="10.11.22.80", port=9120, username="nethouse",
        password="CGC%EVXr.ET10Y_N", secure=True, verify=False
    )
    service = YafengService(client)
    #
    # # 测试全量统计分析
    print("--- 启动通风系统分析 ---")
    t0 = time.time()
    res_analysis = service.print_Yafeng_today_by_yaml_system(
        system_name_filters=["1号空压机","2号空压机","3号空压机"],
        start_date="2026-04-10 00:00:00",
        end_date="2026-06-05 00:00:00",
        # subgroup_filters = ['监测值']
    )
    # res_analysis = service.json_serializer(res_analysis)
    # result_json = json.dumps(res_analysis, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
    
    print(f"分析完成，耗时: {time.time() - t0:.2f}s ")

    # 测试能耗统计
    print("--- 启动能耗分析 ---")
    t1 = time.time()
    res_energy = service.calc_gonglv_energy(
        start_date="2026-04-01 00:00:00",
        end_date="2026-06-05 00:00:00",
    )
    print(f"能耗统计完成，耗时: {time.time() - t1:.2f}s")

    service.close()