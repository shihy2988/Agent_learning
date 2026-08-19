from collections import defaultdict
import clickhouse_connect
from datetime import datetime, timedelta
from .base_info_sqls import (
    query_area_info,
    query_base_station_info,
    query_person_info
)

PERSON_TRAJ_FIELDS = [
    "MINE_CODE",
    "MINE_NAME",
    "CARD_ID",
    "EMPLOYEE_NAME",
    "ENTRY_EXIT_MARK",
    "ENTRY_WELL_TIME",
    "EXIT_WELL_TIME",
    "AREA_CODE",
    "ENTRY_AREA_TIME",
    "BASE_STATION_CODE",
    "ENTRY_BASE_TIME",
    "WORK_MODE",
    "BASE_STATION_DISTANCE",
    "EMPLOYEE_WORK_STATE",
    "IS_LEADER",
    "IS_SPECIAL",
    "TRANSIT_BASE_TIME",
    "TIME",
]

MARK_MAP = {               
    "0": "井口", "1": "井下", "2": "已出井",
    0: "井口", 1: "井下", 2: "已出井",
}
STRFTIME_FMT = "%Y-%m-%d %H:%M:%S"

def format_time(dt):
    if dt is None:
        return None
    if isinstance(dt, str):
        # 简单处理时区后缀
        return str(dt).replace("+08:00", "")
    if isinstance(dt, datetime):
        return dt.strftime(STRFTIME_FMT)
    return dt

def format_duration_seconds(start_str, end_str):
    """计算秒数差，用于后续格式化"""
    if not start_str :
        return None
    if not end_str:
        s1 = str(start_str).replace("+08:00", "")
        t2 = datetime.now()
        t1 = datetime.strptime(s1, STRFTIME_FMT)
      
        diff = int((t2 - t1).total_seconds())
        return max(0, diff)
    try:
        # 假设格式统一，如果包含时区需先处理
        s1 = str(start_str).replace("+08:00", "")
        s2 = str(end_str).replace("+08:00", "")
        t1 = datetime.strptime(s1, STRFTIME_FMT)
        t2 = datetime.strptime(s2, STRFTIME_FMT)
        diff = int((t2 - t1).total_seconds())
        return max(0, diff)
    except:
        return None

def seconds_to_hms(seconds):
    if seconds is None:
        return None
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"

def entry_exit_mark(value):
    return MARK_MAP.get(str(value), value)

def leader_flag(value):
    return "是" if str(value) == "1" else "否"

def special_flag(value):
    return "是" if str(value) == "1" else "否"

def get_name_safe(info_dict, code, default_key_name="名称"):
    if not info_dict or not code:
        return code
    item = info_dict.get(code)
    if item:
        name = item.get("区域名称") or item.get("基站名称") or item.get("名称")
        if name:
            return name
    return code

def group_person_traj_optimized(rows, area_info, station_info, person_infos):
    """
    接收来自 ClickHouse 预聚合后的数据行。
    每行代表一个“班次”或“出入井片段”，其中包含该片段内的所有轨迹点数组。
    """
    result = {}
    
    # 字段顺序对应 SQL SELECT 的顺序
    # MINE_CODE, MINE_NAME, CARD_ID, EMPLOYEE_NAME, 
    # ENTRY_WELL_TIME, EXIT_WELL_TIME, IS_LEADER, IS_SPECIAL, EMPLOYEE_WORK_STATE,
    # traj_points_array (Array of Structs)
    a = 0
    b= []
    for row in rows:
        mine_code, mine_name, card_id, emp_name, \
        entry_time, exit_time, entry_mark,is_leader, is_special, work_state, \
        traj_points = row
        
        person_key = f"{emp_name}_{card_id}"
        
        # 获取人员基础信息
        field_map = person_infos.get(person_key, {})
        
        # 确定当日日期 (从入井时间提取)
        day_str = str(entry_time)[:10] if entry_time else "Unknown"
        
        if day_str not in result:
            result[day_str] = {}
        if person_key not in result[day_str]:
            result[day_str][person_key] = {
                "姓名": emp_name,
                "卡号": card_id,
                "工作状态":work_state,
                "当日是否已出井": entry_exit_mark(entry_mark) if not exit_time else "已出井", # 简化判断，有出井时间即为已出
                "工种": field_map.get("工种", ""),
                "职务": field_map.get("职务", ""),
                "队组班组/部门": field_map.get("队组班组/部门", ""),
                "出生年月": field_map.get("出生年月", ""),
                "学历": field_map.get("学历", ""),
                "是否矿领导": leader_flag(is_leader),

                "是否特种人员": special_flag(is_special),
                "工作状态": work_state,
                "出入井记录": []
            }
        dd = entry_exit_mark(entry_mark) if not exit_time else "已出井",
  
        if dd == ('井下',):
            b.append([day_str,person_key])
            a += 1
        # 处理当前班次的轨迹点
        # traj_points 是一个列表，每个元素是一个元组或字典，取决于 ClickHouse 驱动返回格式
        # 假设返回的是 list of tuples: (area_code, station_code, base_time, distance)
        
        shift_segments = []
        if traj_points:
            # 对轨迹点按时间排序 (虽然 SQL 里可能已经排过，但保险起见)
            # 注意：ClickHouse Array 保持插入顺序，如果 SQL 里用了 groupArrayInsertAt 或类似保证顺序的操作则无需排序
            
            p = 0
            n = len(traj_points)
            
            while p < n:
                pt = traj_points[p]
                #print("traj_points---",pt)
                #{'AREA_CODE': '6108020042959999', 'BASE_STATION_CODE': '6108020042959999000079', 'ENTRY_BASE_TIME': '2026-07-13 15:13:25', 'BASE_STATION_DISTANCE': '9.36'}
                # 假设 pt 结构: (area_code, station_code, entry_base_time, distance)
                # 具体索引取决于 SQL 中 tuple 的定义
                
                if isinstance(pt, tuple):
                    new_pt = {}
                    new_pt['AREA_CODE'] = pt[0]
                    new_pt['BASE_STATION_CODE'] = pt[1]
                    new_pt['ENTRY_BASE_TIME'] = pt[2]
                    new_pt['BASE_STATION_DISTANCE'] = pt[3]
                    pt = new_pt
                
                r_area_code = pt['AREA_CODE']
                r_station_code = pt['BASE_STATION_CODE']
                r_base_time = pt['ENTRY_BASE_TIME']
                r_distance = pt['BASE_STATION_DISTANCE']
               
                
                area_name = get_name_safe(area_info, r_area_code, "区域名称")
                station_name = get_name_safe(station_info, r_station_code, "基站名称")
                
                # 寻找连续相同区域和基站的段
                m = p + 1
                while m < n:
                    next_pt = traj_points[m]
                    if isinstance(next_pt, tuple):
                        new_pt = {}
                        new_pt['AREA_CODE'] = next_pt[0]
                        new_pt['BASE_STATION_CODE'] = next_pt[1]
                        new_pt['AREA_ENTRY_BASE_TIMECODE'] = next_pt[2]
                        new_pt['BASE_STATION_DISTANCE'] = next_pt[3]
                        # new_pt['TIME'] = next_pt[4]
                        next_pt = new_pt
                    if next_pt['AREA_CODE'] != r_area_code or next_pt['BASE_STATION_CODE'] != r_station_code:
                        break
                    m += 1
                
                # 构建段信息
                seg_start_time = r_base_time
                if m < n :
                    traj1 =  traj_points[m]
                    if isinstance(traj1, tuple):
                        seg_end_time = traj1[2]
                    else:
                        seg_end_time = traj1['ENTRY_BASE_TIME'] # 下一段的开始时间作为本段结束
                else:
                    traj1 = traj_points[m-1]
                    if isinstance(traj1, tuple):
                        seg_end_time = traj1[2]
                    else:
                        seg_end_time = traj1['ENTRY_BASE_TIME'] # 下一段的开始时间作为本段结束

                if m==n and  not exit_time:
            
                    now = datetime.now()
                    seg_end_time = now.strftime("%Y-%m-%d %H:%M:%S")
                # 提取距离变化
                # 假设你想取的是每个元组里的第4个字段（索引为3）
                
                distances = []
                for point in traj_points[p:m] :
                    data = point[3]   if isinstance(point, tuple) else point['BASE_STATION_DISTANCE']
                    distances.append(data)

                seg_duration_sec = format_duration_seconds(seg_start_time, seg_end_time)
                
                seg = {
                    "区域": area_name,
                    "基站": station_name,
                    "轨迹开始时间": format_time(seg_start_time),
                    "轨迹结束时间": format_time(seg_end_time),
                    "轨迹距离变化": distances,
                    "轨迹持续时间": seconds_to_hms(seg_duration_sec)
                }
                shift_segments.append(seg)
                p = m
        
        # 构建班次对象
        total_duration_sec = format_duration_seconds(entry_time, exit_time)
        shift_obj = {
            "入井时间": format_time(entry_time),
            "出井时间": format_time(exit_time),
            "入井时长": seconds_to_hms(total_duration_sec),
            "具体轨迹变化": shift_segments
        }
        
        result[day_str][person_key]["出入井记录"].append(shift_obj)
    
    for day_str,person_key in b:
        data = result[day_str][person_key]
        if data['当日是否已出井'] != "井下":
            result[day_str][person_key]['当日是否已出井'] = "井下"
    return result

def query_person_traj(
    client,
    table_name,
    card_id=None,
    start_time=None,
    end_time=None,
    where_clause=None,
):
    """
    使用 ClickHouse 高级特性进行预聚合
    """
    area_info = query_area_info(client)
    station_info = query_base_station_info(client)
    person_info = query_person_info(client)
    
    # 构建基础过滤条件
    wheres = []
    if card_id:
        wheres.append(f"CARD_ID='{card_id}'")
    if start_time:
        try:
            start_dt = datetime.strptime(start_time, STRFTIME_FMT)
            end_dt = datetime.strptime(end_time, STRFTIME_FMT)
            wheres.append(f"(TIME>='{start_time}' AND TIME<'{end_time}')")
        except Exception:
            wheres.append(f"TIME>='{start_time}'")
    if where_clause:
        wheres.append(f"({where_clause})")
        
    where_sql = "WHERE " + " AND ".join(wheres) if wheres else ""

    # 核心 SQL：利用 ENTRY_WELL_TIME 作为天然周期标识，并按 Python 解包顺序排列字段
    sql = f"""
        SELECT
            MINE_CODE,
            MINE_NAME,
            CARD_ID,
            EMPLOYEE_NAME,
            ENTRY_WELL_TIME as entry_well_time,
            maxIf(EXIT_WELL_TIME, ENTRY_EXIT_MARK = '2') as exit_well_time,
            any(ENTRY_EXIT_MARK) as entry_exit_mark,
            any(IS_LEADER) as is_leader,
            any(IS_SPECIAL) as is_special,
            any(EMPLOYEE_WORK_STATE) as work_state,
            groupArray(tuple(AREA_CODE, BASE_STATION_CODE, ENTRY_BASE_TIME, BASE_STATION_DISTANCE)) as traj_points
        FROM {table_name}

        {where_sql}
        GROUP BY
            MINE_CODE,
            MINE_NAME,
            CARD_ID,
            EMPLOYEE_NAME,
            ENTRY_WELL_TIME
        ORDER BY
            CARD_ID,
            entry_well_time
        """

    query_result = client.query(sql)
    rows = query_result.result_rows

    return group_person_traj_optimized(
        rows,
        area_info,
        station_info,
        person_info 
    )

def query_person_history(
    client,
    card_id=None,
    start_time=None,
    end_time=None,
    where_clause=None,
):
    return query_person_traj(
        client,
        "HJL_RYDW_RYSS",
        card_id,
        start_time,
        end_time,
        where_clause,
    )

def query_person_realtime(
    client,
    card_id=None,
    where_clause=None,
):
    return query_person_traj(
        client,
        "HJL_RYDW_RYSS_REAL_TIME",
        card_id,
        None,
        None,
        where_clause,
    )

if __name__ == "__main__":
    import json
    client = clickhouse_connect.get_client(
        host="172.16.28.7",
        port=9120,
        user="nethouse",
        password="whcJv__y_LmWGC.6",
        database="PS",
        secure=True,verify=False
    )
    # client = clickhouse_connect.get_client(
    #     host="10.11.3.210",
    #     port=8123,
    #     database="PS",
    #     user="default",
    #     password="xt123456",
    #     secure=False,verify=False
    # )

    # 历史轨迹示例
    history_data = query_person_history(
        client,
        start_time="2026-07-29 00:00:00",
        end_time="2026-07-32 23:59:59",
    )
    
    print("历史轨迹查询完成")

    # 将全部数据写入 txt 文件
    if history_data:
        with open("history_data.txt", "w", encoding="utf-8") as f:
            f.write(json.dumps(history_data, ensure_ascii=False, indent=2))
        print("全部数据已成功写入 history_data.txt")
    n = 0
    for day,data in history_data.items():
        for name,data_name in data.items():
            carname = data_name['当日是否已出井']
            n1 = data_name['姓名']
            if carname =='井下':
                n += 1
                # print(n1)
    print(n)
        
    print("历史轨迹查询完成")
    # # 打印少量数据验证结构
    # if history_data:
    #     first_day = list(history_data.keys())[0]
    #     first_person = list(history_data[first_day].keys())[0]
    #     print(json.dumps(history_data[first_day][first_person], ensure_ascii=False, indent=2))


