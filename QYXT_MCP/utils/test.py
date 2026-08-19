import traceback
import clickhouse_connect
from datetime import datetime, timedelta
import concurrent.futures
import json
import os
import yaml
import time
from typing import List, Dict, Optional, Union, Tuple
import numpy as np
import sqlite3
import pickle

# =========================================================
# 工具函数
# =========================================================

fan_monitor_tags = {
    "定子温度": [
        "TF_YH_1_JI_DING_ZI_WEN_DU_A_SHI_JI_ZHI", "TF_YH_1_JI_DING_ZI_WEN_DU_B_SHI_JI_ZHI",
        "TF_YH_1_JI_DING_ZI_WEN_DU_C_SHI_JI_ZHI", "TF_YH_2_JI_DING_ZI_WEN_DU_A_SHI_JI_ZHI",
        "TF_YH_2_JI_DING_ZI_WEN_DU_B_SHI_JI_ZHI", "TF_YH_2_JI_DING_ZI_WEN_DU_C_SHI_JI_ZHI",
        "TF_EH_1_JI_DING_ZI_WEN_DU_A_SHI_JI_ZHI", "TF_EH_1_JI_DING_ZI_WEN_DU_B_SHI_JI_ZHI",
        "TF_EH_1_JI_DING_ZI_WEN_DU_C_SHI_JI_ZHI", "TF_EH_2_JI_DING_ZI_WEN_DU_A_SHI_JI_ZHI",
        "TF_EH_2_JI_DING_ZI_WEN_DU_B_SHI_JI_ZHI", "TF_EH_2_JI_DING_ZI_WEN_DU_C_SHI_JI_ZHI",
    ],
    "轴温度": [
        "TF_YH_1_JI_QIAN_ZHOU_WEN_DU_SHI_JI_ZHI", "TF_YH_1_JI_HOU_ZHOU_WEN_DU_SHI_JI_ZHI",
        "TF_YH_2_JI_QIAN_ZHOU_WEN_DU_SHI_JI_ZHI", "TF_YH_2_JI_HOU_ZHOU_WEN_DU_SHI_JI_ZHI",
        "TF_EH_1_JI_QIAN_ZHOU_WEN_DU_SHI_JI_ZHI", "TF_EH_1_JI_HOU_ZHOU_WEN_DU_SHI_JI_ZHI",
        "TF_EH_2_JI_QIAN_ZHOU_WEN_DU_SHI_JI_ZHI", "TF_EH_2_JI_HOU_ZHOU_WEN_DU_SHI_JI_ZHI",
    ],
    "电流": ["TF_YH_1_JI_DIAN_LIU_A_SHI_JI_ZHI", "TF_YH_2_JI_DIAN_LIU_A_SHI_JI_ZHI",
              "TF_EH_1_JI_DIAN_LIU_A_SHI_JI_ZHI", "TF_EH_2_JI_DIAN_LIU_A_SHI_JI_ZHI"],
    "电压": ["TF_YH_1_JI_DIAN_YA_A_SHI_JI_ZHI", "TF_YH_2_JI_DIAN_YA_A_SHI_JI_ZHI",
              "TF_EH_1_JI_DIAN_YA_A_SHI_JI_ZHI", "TF_EH_2_JI_DIAN_YA_A_SHI_JI_ZHI"],
    "振动": [
        "TF_YH_1_JI_CHUI_ZHI_ZHEN_DONG_SHI_JI_ZHI", "TF_YH_1_JI_SHUI_PING_ZHEN_DONG_SHI_JI_ZHI",
        "TF_YH_2_JI_CHUI_ZHI_ZHEN_DONG_SHI_JI_ZHI", "TF_YH_2_JI_SHUI_PING_ZHEN_DONG_SHI_JI_ZHI",
        "TF_EH_1_JI_CHUI_ZHI_ZHEN_DONG_SHI_JI_ZHI", "TF_EH_1_JI_SHUI_PING_ZHEN_DONG_SHI_JI_ZHI",
        "TF_EH_2_JI_CHUI_ZHI_ZHEN_DONG_SHI_JI_ZHI", "TF_EH_2_JI_SHUI_PING_ZHEN_DONG_SHI_JI_ZHI"
    ],
    "功率": ["TF_YH_1_JI_GONG_LV_SHI_JI_ZHI", "TF_YH_2_JI_GONG_LV_SHI_JI_ZHI",
              "TF_EH_1_JI_GONG_LV_SHI_JI_ZHI", "TF_EH_2_JI_GONG_LV_SHI_JI_ZHI"],
    "风速": ["TF_YHFJ_FENG_SU_SHI_JI_ZHI", "TF_EHFJ_FENG_SU_SHI_JI_ZHI"],
    "静压": ["TF_YHFJ_JING_YA_SHI_JI_ZHI", "TF_EHFJ_JING_YA_SHI_JI_ZHI"],
    "全压": ["TF_YHFJ_QUAN_YA_SHI_JI_ZHI", "TF_EHFJ_QUAN_YA_SHI_JI_ZHI"],
    "效率": ["TF_YHFJ_YUN_XING_XIAO_LV", "TF_EHFJ_YUN_XING_XIAO_LV"],
    "风量": ["TF_YHFJ_FENG_LIANG_SHI_JI_ZHI", "TF_EHFJ_FENG_LIANG_SHI_JI_ZHI"],
    "动压": ["TF_YHFJ_DONG_YA_SHI_JI_ZHI", "TF_EHFJ_DONG_YA_SHI_JI_ZHI"]
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


def parse_dt(d):
    if isinstance(d, str):
        return datetime.strptime(d, "%Y-%m-%d %H:%M:%S") if ":" in d else datetime.strptime(d, "%Y-%m-%d")
    return d or datetime.now()


# =========================================================
# 新增：SQLite 缓存功能（两个核心函数）
# =========================================================

def get_sqlite_conn():
    """获取 SQLite 连接"""
    db_path = os.path.join(os.path.dirname(__file__), "tongfeng_cache.db")
    conn = sqlite3.connect(db_path, timeout=30)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def init_cache_table():
    """初始化缓存表"""
    conn = get_sqlite_conn()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS data_cache (
            cache_key TEXT PRIMARY KEY,
            start_time TEXT NOT NULL,
            end_time TEXT NOT NULL,
            fields TEXT NOT NULL,
            data_blob BLOB NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        )
    """)
    conn.commit()
    conn.close()


def get_cache_key(start_dt: datetime, end_dt: datetime, fields: List[str], prefix: str = "general") -> str:
    """生成缓存键"""
    fields_str = ",".join(sorted(fields))
    return f"{prefix}:{start_dt.strftime('%Y%m%d%H%M%S')}:{end_dt.strftime('%Y%m%d%H%M%S')}:{hash(fields_str)}"


def get_cached_data(start_dt: datetime, end_dt: datetime, fields: List[str], prefix: str = "general") -> Optional[Tuple[dict, None]]:
    """
    新增函数1：从 SQLite 缓存获取数据
    """
    init_cache_table()
    conn = get_sqlite_conn()
    cache_key = get_cache_key(start_dt, end_dt, fields, prefix)
    
    cursor = conn.execute("SELECT data_blob FROM data_cache WHERE cache_key = ?", (cache_key,))
    row = cursor.fetchone()
    conn.close()
    
    if row:
        try:
            data = pickle.loads(row[0])
            print(f"✅ SQLite 缓存命中: {start_dt.date()} ~ {end_dt.date()} [{prefix}]")
            return data
        except Exception as e:
            print(f"缓存反序列化失败: {e}")
    return None


def save_to_cache(start_dt: datetime, end_dt: datetime, data, fields: List[str], prefix: str = "general"):
    """
    新增函数2：将数据保存到 SQLite 缓存
    """
    init_cache_table()
    conn = get_sqlite_conn()
    cache_key = get_cache_key(start_dt, end_dt, fields, prefix)
    data_blob = pickle.dumps(data)
    
    conn.execute("""
        INSERT OR REPLACE INTO data_cache 
        (cache_key, start_time, end_time, fields, data_blob)
        VALUES (?, ?, ?, ?, ?)
    """, (
        cache_key,
        start_dt.strftime("%Y-%m-%d %H:%M:%S"),
        end_dt.strftime("%Y-%m-%d %H:%M:%S"),
        ",".join(fields),
        data_blob
    ))
    conn.commit()
    conn.close()
    print(f"💾 数据已缓存到 SQLite: {start_dt.date()} ~ {end_dt.date()} [{prefix}]")


# =========================================================
# 高性能核心分析算法 (NumPy 加速版)
# =========================================================

def fast_rle_numpy(arr, timestamps, max_groups=500):
    if len(arr) == 0:
        return []
    change_idx = np.flatnonzero(arr[1:] != arr[:-1]) + 1
    boundaries = np.concatenate(([0], change_idx, [len(arr)]))
    groups = []
    for i in range(len(boundaries) - 1):
        start = boundaries[i]
        end = boundaries[i + 1] - 1
        duration = (timestamps[end] - timestamps[start]).total_seconds()
        if duration < 20:
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
    n = len(arr)
    if n == 0:
        return {"类型": "未知", "信息": "无有效数据"}
    # ...（保持您原有的完整分析逻辑不变）...
    bool_set = {"true", "false", True, False, "True", "False"}
    is_maybe_bool = all(v in bool_set for v in arr[:min(100, n)])
    
    if is_maybe_bool:
        # 布尔类型处理逻辑（保持原样）
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
            "类型": "布尔", "当前值": values_str[-1], "最早值": values_str[0],
            "最新值": values_str[-1], "最早值时间": ts[0], "最新值时间": ts[-1],
            "变化": changes or "无变化", "稳定阶段": fast_rle_numpy(values_str, ts),
            "变化次数": len(change_idx),
        }
    
    # 数值类型分析（保持原样）
    try:
        values_num = arr.astype(np.float64)
        mean_v = values_num.mean()
        std_v = values_num.std()
        min_idx = values_num.argmin()
        max_idx = values_num.argmax()
        diff = np.diff(values_num)
        threshold = max(abs(mean_v) * 0.25, std_v * 1.5 + 1e-8)
        change_idx = np.flatnonzero(np.abs(diff) > threshold)
        anomaly_mask = np.abs(values_num - mean_v) > max(5 * std_v, 1e-8)
        anomaly_idx = np.flatnonzero(anomaly_mask)
        trend = "上升" if values_num[-1] > values_num[0] else "下降" if values_num[-1] < values_num[0] else "平稳"

        changes = [{"时间": ts[i + 1], "从": float(values_num[i]), "到": float(values_num[i + 1]), 
                   "变化量": float(diff[i])} for i in change_idx[:50]]
        anomalies = [{"时间": ts[i], "数值": float(values_num[i])} for i in anomaly_idx[:100]]
        stable_periods = fast_rle_numpy(np.round(values_num, 6), ts) if enable_stable_periods and n > 1 else []

        return {
            "类型": "数值", "个数": int(n), "平均值": round(float(mean_v), 4),
            "中位数": round(float(np.median(values_num)), 4), "标准差": round(float(std_v), 4),
            "最小值": {"数值": float(values_num[min_idx]), "时间": ts[min_idx]},
            "最大值": {"数值": float(values_num[max_idx]), "时间": ts[max_idx]},
            "最早值": float(values_num[0]), "最早值时间": ts[0],
            "最新值": float(values_num[-1]), "最新值时间": ts[-1],
            "重要变化": changes, "稳定阶段": stable_periods,
            "异常值": anomalies, "趋势": trend,
        }
    except Exception as e:
        return {"类型": "未知", "错误": str(e)}


# =========================================================
# 通风系统业务类
# =========================================================

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

            start_dt = parse_dt(start_date)
            end_dt = parse_dt(end_date)

            # ==================== 缓存检查 ====================
            need_fields = ["TF_TIMESTAMP"]
            for sys_name in valid_systems:
                for sg_name, metas in fan_system[sys_name].items():
                    if subgroup_filters and sg_name not in subgroup_filters:
                        continue
                    for en_key in metas.keys():
                        if en_key != "TF_TIMESTAMP":
                            need_fields.append(en_key)

            cached = get_cached_data(start_dt, end_dt, need_fields, prefix="analysis")
            if cached:
                return cached
            # =================================================

            start_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
            end_str = end_dt.strftime("%Y-%m-%d %H:%M:%S")
            print(f"分析时间段: {start_str} - {end_str}")

            fields_list = list(set(need_fields))
            query = f"""
                SELECT {",".join(fields_list)}
                FROM PS.SDI_TONG_FENG_XI_TONG
                WHERE TF_TIMESTAMP >= toDateTime('{start_str}')
                  AND TF_TIMESTAMP < toDateTime('{end_str}')
                ORDER BY TF_TIMESTAMP ASC
            """

            print(f"开始拉取数据... 字段数: {len(fields_list)}")
            t_query = time.time()
            result = self.client.query(query)
            print(f"数据拉取与转换耗时: {time.time() - t_query:.2f}s")

            if not result.result_columns or len(result.result_columns[0]) == 0:
                print("无数据")
                return {}

            col_map = {name: i for i, name in enumerate(result.column_names)}
            raw_ts = result.result_columns[col_map["TF_TIMESTAMP"]]
            ts_full = np.array([to_naive(t) for t in raw_ts])
            ts_dates = np.array([t.date() for t in ts_full])
            unique_dates = np.unique(ts_dates)
            is_multi_day = len(unique_dates) > 1

            output_dict = {}

            def process_task(args):
                sys_name, sg_name, en_key, cn_desc = args
                raw_vals = result.result_columns[col_map[en_key]]
                vals = np.asarray(raw_vals, dtype=object)
                valid_mask = (vals != None)
                if not np.any(valid_mask):
                    return None

                curr_vals = vals[valid_mask]
                curr_ts = ts_full[valid_mask]
                curr_dates = ts_dates[valid_mask]

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
                        curr_vals = curr_vals[mask]
                        curr_ts = curr_ts[mask]
                        curr_dates = curr_dates[mask]
                    except:
                        pass

                if len(curr_vals) == 0:
                    return None

                if sg_name == "数值" and is_multi_day:
                    daily_data = {}
                    for d in unique_dates:
                        day_mask = (curr_dates == d)
                        if np.any(day_mask):
                            daily_data[d.strftime("%Y-%m-%d")] = analyze_series_fast(
                                curr_vals[day_mask], curr_ts[day_mask], enable_stable_periods)
                    local_output = {"数据类型": "daily_numeric", "每日数据": daily_data, "总共天数": len(daily_data)}
                else:
                    local_output = {"数据": analyze_series_fast(curr_vals, curr_ts, enable_stable_periods)}

                return (sys_name, sg_name, cn_desc, local_output)

            task_mapping = []
            for sys_name in valid_systems:
                for sg_name, metas in fan_system[sys_name].items():
                    if subgroup_filters and sg_name not in subgroup_filters:
                        continue
                    for en_key, cn_desc in metas.items():
                        if en_key == "TF_TIMESTAMP": continue
                        task_mapping.append((sys_name, sg_name, en_key, cn_desc))

            max_workers = min(16, len(task_mapping) or 1)
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_task = {executor.submit(process_task, args): args for args in task_mapping}
                for future in concurrent.futures.as_completed(future_to_task):
                    res = future.result()
                    if res is None:
                        continue
                    sys_name, sg_name, cn_desc, local_output = res
                    if sys_name not in output_dict:
                        output_dict[sys_name] = {}
                    if sg_name not in output_dict[sys_name]:
                        output_dict[sys_name][sg_name] = {}
                    output_dict[sys_name][sg_name][cn_desc] = local_output

            # ==================== 保存缓存 ====================
            save_to_cache(start_dt, end_dt, output_dict, need_fields, prefix="analysis")
            # =================================================

            return output_dict

        except Exception as e:
            print(f"分析异常: {e}\n{traceback.format_exc()}")
            return {}

    def calc_gonglv_energy(self, start_date=None, end_date=None) -> Dict:
        power_keys = ["TF_YH_1_JI_GONG_LV_SHI_JI_ZHI", "TF_YH_2_JI_GONG_LV_SHI_JI_ZHI",
                      "TF_EH_1_JI_GONG_LV_SHI_JI_ZHI", "TF_EH_2_JI_GONG_LV_SHI_JI_ZHI"]
        power_keyvalues = {
            'TF_YH_1_JI_GONG_LV_SHI_JI_ZHI': '一号风机1级设备',
            'TF_YH_2_JI_GONG_LV_SHI_JI_ZHI': '一号风机2级设备',
            'TF_EH_1_JI_GONG_LV_SHI_JI_ZHI': '二号风机1级设备',
            'TF_EH_2_JI_GONG_LV_SHI_JI_ZHI': '二号风机2级设备',
        }

        s_dt = parse_dt(start_date)
        e_dt = parse_dt(end_date)

        # ==================== 缓存检查 ====================
        cached = get_cached_data(s_dt, e_dt, power_keys, prefix="energy")
        if cached:
            return cached
        # =================================================

        start_str = s_dt.strftime("%Y-%m-%d %H:%M:%S")
        end_str = e_dt.strftime("%Y-%m-%d %H:%M:%S")

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
        last_inserted = {}
        s_dt_plus_1h = s_dt + timedelta(hours=1)
        e_dt_minus_1h = e_dt - timedelta(hours=1)

        for idx in range(num_rows):
            day_str = to_naive(day_ts_col[idx]).strftime("%Y-%m-%d")
            hour_ts = to_naive(hour_ts_col[idx])
            hour = hour_ts.hour

            if day_str not in out:
                out[day_str] = {"总平均功率_kw": {}, "总能耗kWh": {},
                                "逐小时能耗kWh": {power_keyvalues[k]: {} for k in power_keys}}

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

        # ==================== 保存缓存 ====================
        save_to_cache(s_dt, e_dt, out, power_keys, prefix="energy")
        # =================================================

        return out

    def close(self):
        if self.client:
            self.client.close()


# =============================================================
# 执行测试
# =============================================================
if __name__ == "__main__":
    client = clickhouse_connect.get_client(
        host="10.11.22.80", port=9120, username="nethouse",
        password="CGC%EVXr.ET10Y_N", secure=True, verify=False
    )
    service = TongfengService(client)

    print("--- 启动通风系统分析 ---")
    t0 = time.time()
    res_analysis = service.print_tongfeng_today_by_yaml_system(
        system_name_filters=["一号风机系统", "二号风机系统"],
        start_date="2026-04-10 00:00:00",
        end_date="2026-06-05 00:00:00",
    )
    print(f"分析完成，耗时: {time.time() - t0:.2f}s")

    print("--- 启动能耗分析 ---")
    t1 = time.time()
    res_energy = service.calc_gonglv_energy(
        start_date="2026-04-01 00:00:00",
        end_date="2026-06-05 00:00:00",
    )
    print(f"能耗统计完成，耗时: {time.time() - t1:.2f}s")

    service.close()