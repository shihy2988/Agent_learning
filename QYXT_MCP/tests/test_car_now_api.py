import requests
from pprint import pprint
import json
import clickhouse_connect
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

url = "https://10.11.22.81:28701/apiaccess/api/SingleDataFactotyWebHttp/getCarLocationWeb"
from utils.person_utils import (
    get_time_stats, get_type_data_from_redis, set_type_data_to_redis
)
from sqls.person_sqls import (
    GET_PERSON_LATEST_SQL,
    GET_PERSONS_LATEST_SQL,
    GET_PERSON_TRAJECTORY_SQL,
    GET_TODAY_PERSONS_SQL,
    GET_REALTIME_STATION_HEAD_INFO_SQL,
    GET_AREA_LIMITS_SQL
)

query = GET_REALTIME_STATION_HEAD_INFO_SQL
db_config = {
            "host": "10.11.22.80",
            "port": 9120,
            "username": "nethouse",
            "password": "CGC%EVXr.ET10Y_N",
            "database": "PS",
       
            "secure": True,
            "verify": False,
            "connect_timeout": 10,
        }

client = clickhouse_connect.get_client(**db_config)
            
rows = client.query(query).result_rows
names =  {
    row[0]: {
        'name':row[1], 
        'type':row[2]
        }
    
    for row in rows
}


payload={}
headers = {
   'User-Agent': 'Apifox/1.0.0 (https://apifox.com)',
   'Accept': '*/*',
   'Host': '10.11.22.81:28701',
   'Connection': 'keep-alive'
}

response = requests.request("POST", url, headers=headers, data=payload,verify=False)
text = response.content.decode("utf-8-sig")
car_now_raw = json.loads(text).get("data", []) if response.status_code == 200 else []

# 过滤并处理字段：去除 '_sortTime'，mainStationID/subStationID 替换为 names 的中文名
def resolve_station_name(station_id_key, row):
    sid = row.get(station_id_key)
    if sid is not None and str(sid).isdigit():  # 数字字符串或数字
        sid_int = int(sid)
        return names.get(sid_int, {}).get('name', sid)
    return sid

car_now = []
for raw in car_now_raw:
    if not isinstance(raw, dict):
        continue
    row = raw.copy()
    row.pop('_sortTime', None)
    row.pop('mainStationHeadPlace', None)
    row.pop('subStationHeadPlace', None)
    row.pop('otherInfo', None)
    row.pop('tunnelDistance', None)
    row.pop('tunnelID', None)
    row.pop('tunnelName', None)
    # 替换 mainStationID 和 subStationID 的值为中文名（如果可映射）
    row['mainStationID'] = resolve_station_name('mainStationID', raw)
    row['subStationID'] = resolve_station_name('subStationID', raw)
    car_now.append(row)

pprint(car_now)
pprint(names)