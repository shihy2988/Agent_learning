import clickhouse_connect

# 1. 建立连接
client = clickhouse_connect.get_client(
    host="10.11.22.80",
    port=9120,
    username="nethouse",
    password="CGC%EVXr.ET10Y_N",
    secure=True,
    verify=False,
)

database = "PS"
table = "SDI_TONG_FENG_XI_TONG"

# 2. 获取字段名（ClickHouse 用 system.columns）
columns_sql = f"""
SELECT name
FROM system.columns
WHERE database = '{database}'
  AND table = '{table}'
"""

start_date = '2026-04-01 00:00:00'
end_date = '2026-04-31 00:00:00'
time_name = 'TF_TIMESTAMP'
import yaml

# 获取 ClickHouse 表的字段名
columns = [row[0] for row in client.query(columns_sql).result_rows]
print(columns)
print(f"字段数量: {len(columns)}")

# 从通风.yaml 加载所有 key
with open("通风.yaml", "r", encoding="utf-8") as f:
    yaml_keys = list(yaml.safe_load(f).keys())

print(f"通风.yaml 字段数量: {len(yaml_keys)}")

# 检查 yaml key 是否都在 columns 中
notfound = [k for k in yaml_keys if k not in columns]
if not notfound:
    print("通风.yaml 中所有 key 都存在于 ClickHouse 字段中。")
else:
    print(f"以下键在 ClickHouse 字段中没有找到: {notfound}")

# 增加判断 yafeng_system.yaml 第4级字段数量是否都在 sql 字段中
import collections

def get_level4_keys(d, path=None):
    if path is None:
        path = []
    res = []
    if isinstance(d, dict):
        for k, v in d.items():
            if len(path) == 3:
                # 进入第4层，只要key，不管value
                res.append(k)
            else:
                res.extend(get_level4_keys(v, path + [k]))
    return res

with open("tongfeng_system.yaml", "r", encoding="utf-8") as f:
    yafeng_yaml = yaml.safe_load(f)

level4_keys = get_level4_keys(yafeng_yaml)
print(f"tongfeng_system.yaml 第4级（最内层）key 数量: {len(level4_keys)}")

notfound_yafeng = [k for k in level4_keys if k not in columns]
notfound_in_yaml = [col for col in columns if col not in level4_keys]

if not notfound_yafeng:
    print("tongfeng_system.yaml 所有第4级字段都存在于 ClickHouse 字段中。")
else:
    print(f"tongfeng_system.yaml的第4级key未出现在 ClickHouse 字段中: {notfound_yafeng}")

if not notfound_in_yaml:
    print("所有 ClickHouse 字段都能在 tongfeng_system.yaml 第4级字段中找到。")
else:
    print(f"以下 ClickHouse 字段在 tongfeng_system.yaml 第4级字段中没有找到: {notfound_in_yaml}")

# 查询最新时间和最早时间（max/min 时间戳）
time_range_sql = f"""
SELECT min({time_name}), max({time_name})
FROM {database}.{table}
"""
min_time, max_time = client.query(time_range_sql).result_rows[0]
print(f"最早时间: {min_time}")
print(f"最新时间: {max_time}")



# # 3. 逐列统计 distinct，同时打印独特值（除了 JBTFXT_ENTRYTIME）
# for col in columns:
#     sql_count = f"""
#     SELECT count(DISTINCT {col})
#     FROM {database}.{table}
#     WHERE {time_name} >= toDateTime('{start_date}')
#       AND {time_name} <  toDateTime('{end_date}')
#     """
#     try:
#         count_result = client.query(sql_count).result_rows[0][0]
#         print(f"{col}: {count_result}")
#         if col != "{time_name}":
#             sql_distinct = f"""
#             SELECT DISTINCT {col}
#             FROM {database}.{table}
#             WHERE {time_name} >= toDateTime('{start_date}')
#               AND {time_name} <  toDateTime('{end_date}')
#             LIMIT 1000
#             """
#             distinct_values = client.query(sql_distinct).result_rows
#             print(f"{col} 的独特值（最多1000个）: {distinct_values} ")
#     except Exception as e:
#         print(f"{col}: ERROR -> {e}")
   
        
        
# for col in columns:
#     sql = f"""
#     SELECT count(DISTINCT {col})
#     FROM {database}.{table}
#     WHERE TF_TIMESTAMP >= toDateTime('2026-04-25 00:00:00')
#       AND TF_TIMESTAMP <  toDateTime('2026-04-30 00:00:00')
#     """
#     try:
#         result = client.query(sql).result_rows[0][0]
#         print(f"{col}: {result}")
#     except Exception as e:
#         print(f"{col}: ERROR -> {e}")