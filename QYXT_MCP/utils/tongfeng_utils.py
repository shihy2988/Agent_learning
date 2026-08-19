import traceback
import clickhouse_connect
from datetime import datetime, timedelta
import concurrent.futures
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

def fast_rle_numpy(arr, timestamps, max_groups=500):
    """numpy版RLE，直接接收处理高能NumPy数组"""
    if len(arr) == 0:
        return []

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

        val = arr[start]
        groups.append({
            "取值": val.item() if hasattr(val, "item") else val,
            "起始时间": timestamps[start],
            "结束时间": timestamps[end],
            "持续秒数": duration,
            "持续小时": round(duration / 3600, 2),
        })

        if len(groups) >= max_groups:
            break
    return groups


def analyze_series_fast(arr, ts, enable_stable_periods=True):
    """超高性能时序分析 - 纯管道化输入"""
    n = len(arr)
    if n == 0:
        return {"类型": "未知", "信息": "无有效数据"}

    # 1. 尝试判断布尔类型
    bool_set = {"true", "false", True, False, "True", "False"}
    is_maybe_bool = True
    check_len = min(100, n)
    for v in arr[:check_len]:
        if v not in bool_set:
            is_maybe_bool = False
            break

    if is_maybe_bool:
        def to_str_bool(v):
            return "True" if v in (True, "true", "True") else "False"
        v_to_str = np.vectorize(to_str_bool, otypes=[object])
        values_str = v_to_str(arr)
        
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

def parse_dt(d):
    if isinstance(d, str):
        return datetime.strptime(d, "%Y-%m-%d %H:%M:%S") if ":" in d else datetime.strptime(d, "%Y-%m-%d")
    return d
            
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

    def print_tongfeng_today_by_yaml_system(
            self,
            system_name_filters: Union[List[str], str],
            start_date: Union[str, datetime, None] = None,
            end_date: Union[str, datetime, None] = None,
            subgroup_filters: Union[List[str], str, None] = None,
            value_filters: Optional[Dict[str, tuple]] = None,
            enable_stable_periods: bool = True,
    ) -> Dict:
        """
        极致吞吐设计：绕过 Result Rows 封装，采用全列数据矩阵化计算。
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

            

            start_dt, end_dt = parse_dt(start_date), parse_dt(end_date)
            start_str, end_str = start_dt.strftime("%Y-%m-%d %H:%M:%S"), end_dt.strftime("%Y-%m-%d %H:%M:%S")
            print(f"分析时间段: {start_str} - {end_str}")
            # 2. 字段映射与 SQL 构建
            need_fields = {"TF_TIMESTAMP"}
            task_mapping = []  # (sys_name, sg_name, en_key, cn_desc)

            for sys_name in valid_systems:
                for sg_name, metas in fan_system[sys_name].items():
                    if subgroup_filters and sg_name not in subgroup_filters:
                        continue
                    for en_key, cn_desc in metas.items():
                        if en_key == "TF_TIMESTAMP": continue
                        need_fields.add(en_key)
                        task_mapping.append((sys_name, sg_name, en_key, cn_desc))

            fields_list = list(need_fields)
            query = f"""
                SELECT {",".join(fields_list)}
                FROM PS.SDI_TONG_FENG_XI_TONG
                WHERE TF_TIMESTAMP >= toDateTime('{start_str}')
                  AND TF_TIMESTAMP < toDateTime('{end_str}')
                ORDER BY TF_TIMESTAMP ASC
            """

            # 3. 列式高速拉取
            print(f"开始拉取数据... 字段数: {len(fields_list)}")
            t_query = time.time()
            result = self.client.query(query)
            
            # 使用 result_columns 替代 result_rows 避免行列重组，提速明显
            if not result.result_columns or len(result.result_columns[0]) == 0:
                print("无数据")
                return {}

            col_map = {name: i for i, name in enumerate(result.column_names)}

            # 直接提取全量时间戳列表
            raw_ts = result.result_columns[col_map["TF_TIMESTAMP"]]
            ts_full = np.array([to_naive(t) for t in raw_ts])
            ts_dates = np.array([t.date() for t in ts_full])
            unique_dates = np.unique(ts_dates)
            is_multi_day = len(unique_dates) > 1

            print(f"数据拉取与转换耗时: {time.time() - t_query:.2f}s")

            # 4. 向量化管道分析
            output_dict = {}

            def process_task(args):
                sys_name, sg_name, en_key, cn_desc = args
                
                # 绕过行矩阵，直接获取单列元数据
                raw_vals = result.result_columns[col_map[en_key]]
                vals = np.asarray(raw_vals, dtype=object)

                # 快速过滤 None 值
                valid_mask = (vals != None)
                if not np.any(valid_mask):
                    return (sys_name, sg_name, en_key, cn_desc, None)

                curr_vals = vals[valid_mask]
                curr_ts = ts_full[valid_mask]
                curr_dates = ts_dates[valid_mask]

                # 向量化过滤
                if value_filters and en_key in value_filters:
                    op, threshold = value_filters[en_key]
                    try:
                        f_vals = curr_vals.astype(float)
                        if op == ">": mask = f_vals > threshold
                        elif op == ">=": mask = f_vals >= threshold
                        elif op == "<": mask = f_vals < threshold
                        elif op == "<=": mask = f_vals <= threshold
                        elif op == "=": mask = f_vals == threshold
                        else: mask = np.ones(len(curr_vals), dtype=bool)
                    except:
                        mask = np.ones(len(curr_vals), dtype=bool)
                    
                    curr_vals = curr_vals[mask]
                    curr_ts = curr_ts[mask]
                    curr_dates = curr_dates[mask]

                if len(curr_vals) == 0:
                    return (sys_name, sg_name, en_key, cn_desc, None)

              
                daily_data = {}
                for d in unique_dates:
                    day_mask = (curr_dates == d)
                    if not np.any(day_mask): continue

                    daily_data[d.strftime("%Y-%m-%d")] = analyze_series_fast(
                        curr_vals[day_mask],
                        curr_ts[day_mask],
                        enable_stable_periods
                    )

                local_output = {
                    "数据类型": "daily_numeric",
                    "每日数据": daily_data,
                    "总共天数": len(daily_data)
                }
               
                return (sys_name, sg_name, cn_desc, local_output)

            # 多线程执行
            max_workers = min(16, len(task_mapping) or 1)
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_task = {executor.submit(process_task, args): args for args in task_mapping}
                for future in concurrent.futures.as_completed(future_to_task):
                    res_tuple = future.result()
                    if res_tuple is None or res_tuple[-1] is None:
                        continue
                    sys_name, sg_name, cn_desc, local_output = res_tuple
                    if sys_name not in output_dict:
                        output_dict[sys_name] = {}
                    if sg_name not in output_dict[sys_name]:
                        output_dict[sys_name][sg_name] = {}
                    output_dict[sys_name][sg_name][cn_desc] = local_output

            return output_dict

        except Exception as e:
            print(f"分析异常: {e}\n{traceback.format_exc()}")
            return {}

    def calc_gonglv_energy(self, start_date=None, end_date=None) -> Dict:
        """
        能耗统计优化版：引入 RLE 缓存加速器 + 时间窗口快路径判定
        """
        power_keys = ["TF_YH_1_JI_GONG_LV_SHI_JI_ZHI", "TF_YH_2_JI_GONG_LV_SHI_JI_ZHI",
                      "TF_EH_1_JI_GONG_LV_SHI_JI_ZHI", "TF_EH_2_JI_GONG_LV_SHI_JI_ZHI"]
        power_keyvalues = {
            'TF_YH_1_JI_GONG_LV_SHI_JI_ZHI': '一号风机1级设备',
            'TF_YH_2_JI_GONG_LV_SHI_JI_ZHI': '一号风机2级设备',
            'TF_EH_1_JI_GONG_LV_SHI_JI_ZHI': '二号风机1级设备',
            'TF_EH_2_JI_GONG_LV_SHI_JI_ZHI': '二号风机2级设备',
        }

        def parse_dt(d):
            if isinstance(d, str):
                return datetime.strptime(d, "%Y-%m-%d %H:%M:%S") if ":" in d else datetime.strptime(d, "%Y-%m-%d")
            return d or datetime.now()

        s_dt, e_dt = parse_dt(start_date), parse_dt(end_date)
        start_str, end_str = s_dt.strftime("%Y-%m-%d %H:%M:%S"), e_dt.strftime("%Y-%m-%d %H:%M:%S")

        avg_fields = [f"avg(toFloat64OrNull({f})) as {f}" for f in power_keys]
        query = f"""
           SELECT toStartOfDay(TF_TIMESTAMP) as day_ts, toStartOfHour(TF_TIMESTAMP) as hour_ts, {",".join(avg_fields)}
           FROM PS.SDI_TONG_FENG_XI_TONG
           WHERE TF_TIMESTAMP >= toDateTime('{start_str}') AND TF_TIMESTAMP < toDateTime('{end_str}')
           GROUP BY day_ts, hour_ts ORDER BY day_ts ASC, hour_ts ASC
        """

        result = self.client.query(query)
        if not result.result_columns or len(result.result_columns[0]) == 0: 
            return {}

        cols = result.result_columns
        day_ts_col = cols[0]
        hour_ts_col = cols[1]
        num_rows = len(day_ts_col)

        out = {}
        # 缓存状态机追踪器：key: (day_str, pk_val) -> (last_label, last_item_dict)
        # 用此机制彻底杜绝原始代码中 list(m_dict.keys())[-1] 造成的严重 O(N^2) 性能衰退缺陷
        last_inserted = {}

        # 预计算快路径边界条件
        s_dt_plus_1h = s_dt + timedelta(hours=1)
        e_dt_minus_1h = e_dt - timedelta(hours=1)

        for idx in range(num_rows):
            day_str = to_naive(day_ts_col[idx]).strftime("%Y-%m-%d")
            hour_ts = to_naive(hour_ts_col[idx])
            hour = hour_ts.hour

            if day_str not in out:
                out[day_str] = {"总平均功率_kw": {}, "总能耗kWh": {},
                                "逐小时能耗kWh": {power_keyvalues[k]: {} for k in power_keys}}

            # 快速时间截取计算（避免 99% 的重叠计算开销）
            if hour_ts >= s_dt_plus_1h and hour_ts <= e_dt_minus_1h:
                sec = 3600.0
            else:
                h_start = max(hour_ts, s_dt)
                h_end = min(hour_ts + timedelta(hours=1), e_dt)
                sec = max(0.0, (h_end - h_start).total_seconds())

            if sec <= 0:
                continue

            sec_ratio = sec / 3600.0

            for i, pk in enumerate(power_keys):
                avg_p = round(float(cols[i + 2][idx] or 0), 2)
                pk_val = power_keyvalues[pk]
                kwh = round(avg_p * sec_ratio, 2)

                m_dict = out[day_str]["逐小时能耗kWh"][pk_val]
                state_key = (day_str, pk_val)

                if state_key not in last_inserted:
                    h_label = f"{hour:02d}:00-{hour + 1:02d}:00"
                    item = {"平均功率_kw": avg_p, "能耗_KWh": kwh, "开始小时": hour, "结束小时": hour + 1}
                    m_dict[h_label] = item
                    last_inserted[state_key] = (h_label, item)
                else:
                    last_key, last_item = last_inserted[state_key]
                    if last_item["平均功率_kw"] == avg_p and last_item["结束小时"] == hour:
                        last_item["结束小时"] = hour + 1
                        last_item["能耗_KWh"] = round(last_item["能耗_KWh"] + kwh, 2)
                        new_label = f"{last_item['开始小时']:02d}:00-{last_item['结束小时']:02d}:00"
                        if new_label != last_key:
                            m_dict[new_label] = m_dict.pop(last_key)
                            last_inserted[state_key] = (new_label, last_item)
                    else:
                        h_label = f"{hour:02d}:00-{hour + 1:02d}:00"
                        item = {"平均功率_kw": avg_p, "能耗_KWh": kwh, "开始小时": hour, "结束小时": hour + 1}
                        m_dict[h_label] = item
                        last_inserted[state_key] = (h_label, item)

        # 清理多余临时字段并归纳总体均值与能耗总和
        for day_data in out.values():
            for pk_val in power_keyvalues.values():
                h_dict = day_data["逐小时能耗kWh"].get(pk_val, {})
                if h_dict:
                    powers = [v["平均功率_kw"] for v in h_dict.values()]
                    day_data["总平均功率_kw"][pk_val] = round(sum(powers) / len(powers), 2)
                    day_data["总能耗kWh"][pk_val] = round(sum(v["能耗_KWh"] for v in h_dict.values()), 2)
                    for v in h_dict.values():
                        v.pop("开始小时", None)
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
    service = TongfengService(client)

    # 测试全量统计分析
    print("--- 启动通风系统分析 ---")
    t0 = time.time()
    res_analysis = service.print_tongfeng_today_by_yaml_system(
        system_name_filters=["一号风机系统","二号风机系统"],
        start_date="2026-06-01 00:00:00",
        end_date="2026-06-05 00:00:00",
    )
    print(f"分析完成，耗时: {time.time() - t0:.2f}s")

    # 测试能耗统计
    print("--- 启动能耗分析 ---")
    t1 = time.time()
    res_energy = service.calc_gonglv_energy(
        start_date="2026-04-01 00:00:00",
        end_date="2026-06-05 00:00:00",
    )
    print(f"能耗统计完成 {res_energy}，耗时: {time.time() - t1:.2f}s")

    service.close()