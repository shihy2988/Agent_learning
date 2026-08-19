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
# 先对 missed_days 排序，并分段连续区间
from itertools import groupby
# 引入 SQLite 支持
import sqlite3
 # INSERT_YOUR_CODE
import threading



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
            
class YafengService:
    def __init__(self, client,logger=None):
        if logger is None:
            self.logger = logging.getLogger("tongfeng_bases")
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
            if not self.logger.handlers:
                self.logger.addHandler(handler)
            self.logger.setLevel(logging.INFO)
        else:
            self.logger = logger
        self.client = client
        self._fan_system_cache  = None
        self.start_auto_analysis_thread()

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
            self.logger.info(f"分析时间段: {start_str} - {end_str}")
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

            # 3. 列式高速拉取
            self.logger.info(f"开始拉取{system_name_filters}数据... 字段数: {len(fields_list)}")
            t_query = time.time()
            result = self.client.query(query)
            
            # 使用 result_columns 替代 result_rows 避免行列重组，提速明显
            if not result.result_columns or len(result.result_columns[0]) == 0:
                print("无数据")
                return {}

            col_map = {name: i for i, name in enumerate(result.column_names)}

            # 直接提取全量时间戳列表
            raw_ts = result.result_columns[col_map["YF_TIMESTAMP"]]
            ts_full = np.array([to_naive(t) for t in raw_ts])
            ts_dates = np.array([t.date() for t in ts_full])
            unique_dates = np.unique(ts_dates)
           
            t2 = time.time()
            self.logger.info(f"数据拉取与转换耗时: {t2 - t_query:.2f}s")

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
                    
            elapsed = time.time() - t2 if 't2' in locals() else None
            if elapsed is not None:
                self.logger.info(f"处理结束，耗时: {elapsed:.2f}秒")
            else:
                self.logger.info("处理结束")
       
            return output_dict

        except Exception as e:
            print(f"分析异常: {e}\n{traceback.format_exc()}")
            return {}

    def calc_gonglv_energy(self, start_date=None, end_date=None) -> Dict:
        """
        能耗统计优化版：引入 RLE 缓存加速器 + 时间窗口快路径判定
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

        return out

 
    def start_auto_analysis_thread(self):
        """
        启动一个后台线程，定时执行 _run_daily_auto_analysis
        """
        thread = threading.Thread(target=self._run_daily_auto_analysis, daemon=True)
        thread.start()
        return thread
    
    def _run_daily_auto_analysis(self):
        """
        每天定时自动分析前一月全量数据，并用 print_yafeng_today_with_cache 和 calc_gonglv_energy_with_cache 预热缓存。
        【新增】每次执行完毕后自动清理 yafeng_analysis_cache.db 中60天以上的过期缓存数据。
        """
        systems = [
            "1号空压机", "2号空压机", "3号空压机", "断路器系统",
            "系统级", "机房配电室操作室环境烟雾温度系统", "振动系统", "逻辑控制系统"
        ]

        # 注意：此处使用 yafeng_analysis_cache.db，与 tongfeng 版本不同
        db_path = os.path.join(os.path.dirname(__file__), "yafeng_analysis_cache.db")

        while True:
            now = datetime.now()
            next_time = (now + timedelta(days=1)).replace(hour=2, minute=0, second=0, microsecond=0)
            wait_seconds = (next_time - now).total_seconds()

            # 首次立即执行，后续按天等待
            if hasattr(self, '_ran_auto_analysis'):
                if wait_seconds > 0:
                    time.sleep(wait_seconds)
            else:
                self._ran_auto_analysis = True

            try:
                end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                start_date = end_date - timedelta(days=30)

                # ========== 1. 预热缓存 ==========
                for sys in systems:
                    try:
                        self.print_yafeng_today_with_cache(
                            system_name_filters=[sys],
                            start_date=start_date,
                            end_date=end_date,
                            enable_stable_periods=True
                        )
                        self.logger.info(f"自动分析: 系统 {sys} print_yafeng_today_with_cache 完成")
                    except Exception as e:
                        self.logger.error(f"自动分析: 系统 {sys} print_yafeng_today_with_cache 异常: {e}")

                try:
                    self.calc_gonglv_energy_with_cache(
                        start_date=start_date,
                        end_date=end_date
                    )
                    self.logger.info("自动能耗分析: calc_gonglv_energy_with_cache 完成")
                except Exception as e:
                    self.logger.error(f"自动分析: calc_gonglv_energy_with_cache 异常: {e}")

                # ========== 2. 【新增】清理60天以上的过期缓存 ==========
                cutoff_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")
                try:
                    conn = sqlite3.connect(db_path)
                    cursor = conn.cursor()

                    # 清理压风系统原子缓存
                    cursor.execute(
                        "DELETE FROM yafeng_atom_cache WHERE day_or_interval < ?",
                        (cutoff_date,)
                    )
                    yf_deleted = cursor.rowcount

                    # 清理能耗日级缓存（yafeng库中的 energy_daily_cache）
                    cursor.execute(
                        "DELETE FROM energy_daily_cache WHERE day_str < ?",
                        (cutoff_date,)
                    )
                    energy_deleted = cursor.rowcount

                    conn.commit()
                    conn.close()

                    self.logger.info(
                        f"压风缓存清理完成: yafeng_atom_cache 删除 {yf_deleted} 条, "
                        f"energy_daily_cache 删除 {energy_deleted} 条 (阈值: {cutoff_date})"
                    )
                except Exception as e:
                    self.logger.error(f"压风缓存清理异常: {e}")

            except Exception as e:
                self.logger.error(f"自动分析: 总体异常: {e}")

   
    
    def print_yafeng_today_with_cache(
            self,
            system_name_filters: Union[List[str], str],
            start_date: Union[str, datetime, None] = None,
            end_date: Union[str, datetime, None] = None,
            subgroup_filters: Union[List[str], str, None] = None,
            value_filters: Optional[Dict[str, tuple]] = None,
            enable_stable_periods: bool = True,
    ) -> Dict:
        """
        带细粒度原子级缓存的压风系统数据分析。
        返回值统一为每日数据模式，每天一个键，所有数据点作为子键。
        【更新】最近3天（含当天）的数据强制走数据库实时查询，不使用缓存。
        """
        db_path = os.path.join(os.path.dirname(__file__), "yafeng_analysis_cache.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS yafeng_atom_cache (
                sys_name TEXT,
                sg_name TEXT,
                cn_desc TEXT,
                day_or_interval TEXT,
                value_filter_str TEXT,
                enable_stable_periods INTEGER,
                result_json TEXT,
                PRIMARY KEY (sys_name, sg_name, cn_desc, day_or_interval, value_filter_str, enable_stable_periods)
            )
        ''')
        conn.commit()

        yaml_data = self._load_yaml()
        fan_system = yaml_data.get("fan_system", {})

        if isinstance(system_name_filters, str):
            system_name_filters = [system_name_filters]
        valid_systems = [s for s in system_name_filters if s in fan_system]
        if not valid_systems:
            conn.close()
            return {}

        if isinstance(subgroup_filters, str):
            subgroup_filters = [subgroup_filters]

        # 日期准备
        s_dt = parse_dt(start_date) if start_date else datetime.now()
        e_dt = parse_dt(end_date) if end_date else s_dt
        if isinstance(end_date, str):
            dt_parts = end_date.strip().split(' ')
            if len(dt_parts) == 2 and dt_parts[1] in ("00:00:00", "00:00:00.000"):
                try:
                    day = datetime.strptime(dt_parts[0], "%Y-%m-%d").date()
                    prev_day = day - timedelta(days=1)
                    end_date_new = f"{prev_day.strftime('%Y-%m-%d')} 23:59:59"
                    e_dt = parse_dt(end_date_new)
                except Exception:
                    pass

        now = datetime.now()
        if e_dt > now:
            e_dt = now
        if s_dt > e_dt:
            s_dt = e_dt

        req_dates = []
        curr_d = s_dt.date()
        while curr_d <= e_dt.date():
            req_dates.append(curr_d.strftime("%Y-%m-%d"))
            curr_d += timedelta(days=1)

        # --- 【新增】计算最近3天的日期集合（含今天） ---
        today_date = datetime.now().date()
        recent_7_days_set = {
            (today_date - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(3)
        }
        # ------------------------------------------

        missed_tasks = []
        for sys_name in valid_systems:
            for sg_name, metas in fan_system[sys_name].items():
                if subgroup_filters and sg_name not in subgroup_filters:
                    continue
                for en_key, cn_desc in metas.items():
                    if en_key == "YF_TIMESTAMP":
                        continue
                    v_filter = value_filters.get(en_key) if value_filters else None
                    v_filter_str = str(v_filter)
                    for day in req_dates:
                        # 【修改】最近3天（含当天）强制 miss
                        if day in recent_7_days_set:
                            missed_tasks.append((sys_name, sg_name, en_key, cn_desc, day, v_filter_str))
                            continue
                        # 超过3天的历史数据才查缓存
                        cursor.execute('''
                            SELECT result_json FROM yafeng_atom_cache 
                            WHERE sys_name=? AND sg_name=? AND cn_desc=? AND day_or_interval=? 
                            AND value_filter_str=? AND enable_stable_periods=?
                        ''', (sys_name, sg_name, cn_desc, day, v_filter_str, int(enable_stable_periods)))
                        row = cursor.fetchone()
                        if not row:
                            missed_tasks.append((sys_name, sg_name, en_key, cn_desc, day, v_filter_str))

        if missed_tasks:
            missed_systems = set([t[0] for t in missed_tasks])
            missed_subgroups = set([t[1] for t in missed_tasks])
            missed_days = set([t[4] for t in missed_tasks])
            self.logger.info(f"【SQLite 缓存局部未命中/近3天实时查询】系统: {list(missed_systems)}, 子组: {list(missed_subgroups)}, 触发 ClickHouse 增量式分析...")
            self.logger.info(f"本次 missed_tasks 覆盖的所有 day: {sorted(missed_days)}")

            def days_to_ranges(sorted_days):
                dates = [datetime.strptime(d, "%Y-%m-%d").date() for d in sorted_days]
                ranges = []
                for _, g in groupby(enumerate(dates), lambda x: (x[1] - timedelta(days=x[0]))):
                    group = list(g)
                    start = group[0][1]
                    end = group[-1][1]
                    ranges.append((start, end))
                return ranges

            missed_days_sorted = sorted(list(missed_days))
            missed_ranges = days_to_ranges(missed_days_sorted)

            for start_day, end_day in missed_ranges:
                this_start_str = f"{start_day.strftime('%Y-%m-%d')} 00:00:00"
                this_end_str = f"{end_day.strftime('%Y-%m-%d')} 23:59:59"
                part_data = self.print_Yafeng_today_by_yaml_system(
                    system_name_filters=list(missed_systems),
                    start_date=this_start_str,
                    end_date=this_end_str,
                    subgroup_filters=list(missed_subgroups),
                    value_filters=value_filters,
                    enable_stable_periods=enable_stable_periods,
                )

                for s_name, s_val in part_data.items():
                    for g_name, g_val in s_val.items():
                        for c_desc, local_out in g_val.items():
                            en_key = None
                            if s_name in fan_system and g_name in fan_system[s_name]:
                                for ek, cd in fan_system[s_name][g_name].items():
                                    if cd == c_desc:
                                        en_key = ek
                                        break
                            v_filter = value_filters.get(en_key) if (value_filters and en_key) else None
                            v_filter_str = str(v_filter)
                            if isinstance(local_out, dict) and "每日数据" in local_out:
                                for day_str, day_analysis in local_out["每日数据"].items():
                                    cursor.execute('''
                                        INSERT OR REPLACE INTO yafeng_atom_cache 
                                        (sys_name, sg_name, cn_desc, day_or_interval, value_filter_str, enable_stable_periods, result_json)
                                        VALUES (?, ?, ?, ?, ?, ?, ?)
                                    ''', (s_name, g_name, c_desc, day_str, v_filter_str, int(enable_stable_periods),
                                        json.dumps(day_analysis, default=json_serializer, ensure_ascii=False)))
                            else:
                                curr_day = start_day
                                while curr_day <= end_day:
                                    day_str = curr_day.strftime("%Y-%m-%d")
                                    cursor.execute('''
                                        INSERT OR REPLACE INTO yafeng_atom_cache 
                                        (sys_name, sg_name, cn_desc, day_or_interval, value_filter_str, enable_stable_periods, result_json)
                                        VALUES (?, ?, ?, ?, ?, ?, ?)
                                    ''', (s_name, g_name, c_desc, day_str, v_filter_str, int(enable_stable_periods),
                                        json.dumps(local_out, default=json_serializer, ensure_ascii=False)))
                                    curr_day += timedelta(days=1)
                conn.commit()
        else:
            self.logger.info("【SQLite 细粒度缓存全命中】完美匹配所需字段子集，跳过数据库拉取！")

        # 汇总输出
        final_output: Dict[str, Dict] = {}
        for sys_name in valid_systems:
            for sg_name, metas in fan_system[sys_name].items():
                if subgroup_filters and sg_name not in subgroup_filters:
                    continue
                for en_key, cn_desc in metas.items():
                    if en_key == "YF_TIMESTAMP":
                        continue
                    v_filter = value_filters.get(en_key) if value_filters else None
                    v_filter_str = str(v_filter)
                    for day in req_dates:
                        # 统一从缓存读取（近3天数据已在上一步写入）
                        cursor.execute('''
                            SELECT result_json FROM yafeng_atom_cache 
                            WHERE sys_name=? AND sg_name=? AND cn_desc=? AND day_or_interval=? 
                            AND value_filter_str=? AND enable_stable_periods=?
                        ''', (sys_name, sg_name, cn_desc, day, v_filter_str, int(enable_stable_periods)))
                        row = cursor.fetchone()

                        if row:
                            day_data = json.loads(row[0])
                            if sys_name not in final_output:
                                final_output[sys_name] = {}
                            if sg_name not in final_output[sys_name]:
                                final_output[sys_name][sg_name] = {}
                            if cn_desc not in final_output[sys_name][sg_name]:
                                final_output[sys_name][sg_name][cn_desc] = {"每日数据": {}}
                            final_output[sys_name][sg_name][cn_desc]["每日数据"][day] = day_data

        for sys_name in final_output:
            for sg_name in final_output[sys_name]:
                for cn_desc in final_output[sys_name][sg_name]:
                    daily_data = final_output[sys_name][sg_name][cn_desc].get("每日数据", {})
                    final_output[sys_name][sg_name][cn_desc]["数据类型"] = "daily_numeric"
                    final_output[sys_name][sg_name][cn_desc]["每日数据"] = daily_data
                    final_output[sys_name][sg_name][cn_desc]["总共天数"] = len(daily_data)

        conn.close()
        return final_output

    def calc_gonglv_energy_with_cache(self, start_date=None, end_date=None) -> Dict:
        """
        带 SQLite 日级分片缓存的能耗统计分析（压风系统）。
        自动匹配跨度内的全部单天数据，仅对不存在的天进行 ClickHouse 计算。
        【更新】最近3天（含当天）的数据强制走数据库实时查询，不使用缓存。
        """
        db_path = os.path.join(os.path.dirname(__file__), "yafeng_analysis_cache.db")
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS energy_daily_cache (
                day_str TEXT PRIMARY KEY,
                result_json TEXT
            )
        ''')
        conn.commit()

        s_dt = parse_dt(start_date) if start_date else datetime.now()
        e_dt = parse_dt(end_date) if end_date else s_dt

        now = datetime.now()
        if e_dt > now:
            e_dt = now
        if s_dt > e_dt:
            s_dt = e_dt

        requested_days = []
        curr_d = s_dt.date()
        while curr_d <= e_dt.date():
            requested_days.append(curr_d.strftime("%Y-%m-%d"))
            curr_d += timedelta(days=1)

        # --- 【新增】计算最近3天的日期集合（含今天） ---
        today_date = datetime.now().date()
        recent_7_days_set = {
            (today_date - timedelta(days=i)).strftime("%Y-%m-%d")
            for i in range(3)
        }
        # ------------------------------------------

        hit_data = {}
        missing_days = []
        for day in requested_days:
            # 【修改】最近3天（含当天）强制 miss
            if day in recent_7_days_set:
                missing_days.append(day)
                continue
            # 超过3天的历史数据才查缓存
            cursor.execute("SELECT result_json FROM energy_daily_cache WHERE day_str=?", (day,))
            row = cursor.fetchone()
            if row:
                hit_data[day] = json.loads(row[0])
            else:
                missing_days.append(day)

        if not missing_days:
            self.logger.info("【SQLite 每日能耗缓存全命中（近3天除外）】直接重组内存数据。")
            conn.close()
            return hit_data

        self.logger.info(f"【SQLite 每日能耗缓存部分未命中/近3天实时查询】缺失天: {missing_days}，开始增量拉取...")

        min_missing = min(missing_days)
        max_missing = max(missing_days)
        q_start = datetime.strptime(min_missing, "%Y-%m-%d")
        q_end = datetime.strptime(max_missing, "%Y-%m-%d") + timedelta(days=1)

        new_data = self.calc_gonglv_energy(q_start, q_end)

        for day_str, day_res in new_data.items():
            cursor.execute(
                "INSERT OR REPLACE INTO energy_daily_cache (day_str, result_json) VALUES (?, ?)",
                (day_str, json.dumps(day_res, default=json_serializer, ensure_ascii=False))
            )
        conn.commit()

        # 汇总：统一从缓存读取
        for day in requested_days:
            if day not in hit_data:
                cursor.execute("SELECT result_json FROM energy_daily_cache WHERE day_str=?", (day,))
                row = cursor.fetchone()
                if row:
                    hit_data[day] = json.loads(row[0])

        conn.close()
        return hit_data

    def close(self):
        if self.client: self.client.close()


# =============================================================
# 执行测试验证
# =============================================================
if __name__ == "__main__":
     # INSERT_YOUR_CODE
    import logging

    client = clickhouse_connect.get_client(
        host="10.11.22.80", port=9120, username="nethouse",
        password="CGC%EVXr.ET10Y_N", secure=True, verify=False
    )
   
    # 设置日志
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    logger = logging.getLogger("yafengTest")

    logger.info("已连接到ClickHouse进行风机系统测试。")
    service = YafengService(client,logger)

    # 模拟第一次大范围深度查询 (包含一号和二号系统)
    print("--- 第一次查询：两套风机全量数据 (写入大面积底层缓存) ---")
    t0 = time.time()
    res1 = service.print_yafeng_today_with_cache(
        system_name_filters=["1号空压机", "2号空压机"],
        # subgroup_filters = ["数值","报警"],
        start_date="2026-08-08 00:00:00",
        end_date="2026-08-10 00:00:00",
    )
    daytype_json = json.dumps(res1, ensure_ascii=False, indent=2)
    with open("压风_data.txt", "w", encoding="utf-8") as f:
        f.write(daytype_json)
        print("全部数据已成功写入 压风_data.txt")
    # print(daytype_json)
    print("json长度:", len(daytype_json))
    print(f"耗时: {time.time() - t0:.2f}s\n")

    # # 模拟用户下一次只追问“一号风机系统”（即子集提取测试）
    # print("--- 第二次查询：仅查询一号风机系统 (期望 100% 细粒度缓存命中) ---")
    # t1 = time.time()
    # res2 = service.print_tongfeng_today_with_cache(
    #     system_name_filters=["一号风机系统"],
    #     start_date="2026-06-01 00:00:00",
    #     end_date="2026-06-02 00:00:00",
    # )
    # print(f"耗时: {time.time() - t1:.2f}s\n")

    # 模拟能耗日级分片测试
    print("--- 启动能耗分片分析 ---")
    t2 = time.time()
    res_energy = service.calc_gonglv_energy_with_cache(
        start_date="2026-08-08 00:00:00",
        end_date="2026-08-09 00:00:00",
    )
    daytype_json = json.dumps(res_energy, ensure_ascii=False, indent=2)
    with open("压风energy_data.txt", "w", encoding="utf-8") as f:
        f.write(daytype_json)
        print("全部数据已成功写入 压风energy_data.txt")
    print(f"能耗拉取完成，{res_energy} 耗时: {time.time() - t2:.2f}s")

    service.close()