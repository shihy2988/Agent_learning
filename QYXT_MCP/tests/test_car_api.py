import requests
import json
from pprint import pp, pprint
import datetime

from collections import defaultdict

def time_to_seconds(t):
    print('-----------',t)
    h, m, s = map(int, t.split(':'))
    return h * 3600 + m * 60 + s

def merge_adjacent_segments(segments):
    if not segments:
        return []
    
    merged = []
    i = 0
    n = len(segments)
    
    while i < n:
        current = segments[i]
        start_time = current['S_E_Time'][0]
        end_time = current['S_E_Time'][1]
        main_area = current['area']          # 保留第一个出现的 area
        
        j = i + 1
        is_jumping = False
        
        while j < n:
            next_seg = segments[j]
            prev_end = end_time
            next_start = next_seg['S_E_Time'][0]
            print('prev_end:',prev_end)
            print('next_start:',next_start)
            prev_end = prev_end[11:] 
            next_start = next_start[11:] 
            # 时间间隔超过10秒，停止合并
            if time_to_seconds(next_start) - time_to_seconds(prev_end) > 10:
                break
            
            # 判断是否为来回跳变（与main_area不同，且后续会再跳回来）
            if next_seg['area'] != main_area:
                is_jumping = True
                end_time = next_seg['S_E_Time'][1]   # 扩展结束时间
                j += 1
                continue
            else:
                # 又跳回 main_area，继续扩展
                end_time = next_seg['S_E_Time'][1]
                j += 1
                continue
        
        # 如果发生了来回跳变，则只保留第一个 area，并合并时间区间
        if is_jumping:
            merged.append({
                'S_E_Time': [start_time, end_time],
                'area': main_area
            })
        else:
            # 没有跳变，保留原始 segment
            merged.append(current)
        
        i = j
    
    return merged

def merge_consecutive_same_area(segments):
    """
    合并前后相邻且 area 相同的区间
    :param segments: List[dict]，每个元素如 {'S_E_Time': [start, end], 'area': xxx}
    :return: 合并后的 segments
    """
    if not segments:
        return []

    merged = []
    prev = segments[0].copy()

    for seg in segments[1:]:
        if seg['area'] == prev['area'] :
            prev['S_E_Time'][1] = seg['S_E_Time'][1]
        else:
            merged.append(prev)
            prev = seg.copy()
    merged.append(prev)
    return merged

def fetch_and_process_car_history(card_id="0099", begin_time="2026-04-23 00:00:00", end_time="2026-04-23 20:00:00"):
    url = "https://10.11.22.81:28701/apiaccess/api/rydw_getCarHistoryLocation_n"

    payload = json.dumps({
        "cardID": card_id,
        "beginTime": begin_time,
        "endTime": end_time
    })
    headers = {
        'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
        'Content-Type': 'application/json',
        'Accept': '*/*',
        'Host': '10.11.22.81:28701',
        'Connection': 'keep-alive'
    }

    response = requests.request("POST", url, headers=headers, data=payload,verify=False)
    text = response.content.decode("utf-8-sig")
    car_records = json.loads(text).get("data", []) if response.status_code == 200 else []

    simple_segments = []
    prev_area = None
    segment_start = None
    segment_end = None

    first_car_name = None
    first_card_id = None
    first_department = None
    first_found = False

    for seg in car_records:
        current_area = seg.get("mainStationID", "") or seg.get("areaName", "")
        seg_time = seg.get("mainStationTime", "")

        if not first_found:
            first_car_name = seg.get("carName", "")
            first_card_id = seg.get("cardId", "")
            first_department = seg.get("department", "")
            first_found = True

        if prev_area != current_area:
            if prev_area is not None:
                simple_segments.append({
                    "area": prev_area,
                    "S_E_Time": [segment_start,segment_end],
                })
            prev_area = current_area
            segment_start = seg_time
            segment_end = seg_time
        else:
            if segment_start is None or seg_time < segment_start:
                segment_start = seg_time
            if segment_end is None or seg_time > segment_end:
                segment_end = seg_time

    if prev_area is not None:
        simple_segments.append({
            "area": prev_area,
            "S_E_Time": [segment_start,segment_end],
        })

    car_info = {
        "carName": first_car_name, 
        "cardId": first_card_id,
        "department": first_department,
        "total_count": len(simple_segments),
        "query_date": json.loads(payload).get("findDate", ""),
        "time_now": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    result = {
        "carInfo": car_info,
        "segments": simple_segments
    }

    merged_segments = merge_adjacent_segments(result['segments'])
    merged_segments = merge_consecutive_same_area(merged_segments)
    result['segments'] = merged_segments
    result["carInfo"]['total_count'] = len(merged_segments)
    return result, len(simple_segments)

if __name__ == "__main__":
    result, simple_len = fetch_and_process_car_history()
    pprint(result)
    print("这是长度:", len(str(result)))
    pprint(simple_len)
