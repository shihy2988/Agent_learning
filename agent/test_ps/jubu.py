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
table = "SDI_JU_BU_TONG_FENG_XI_TONG_YI_BU"

# 2. 获取字段名（ClickHouse 用 system.columns）
columns_sql = f"""
SELECT name
FROM system.columns
WHERE database = '{database}'
  AND table = '{table}'
"""

start_date = '2026-04-01 00:00:00'
end_date = '2026-05-31 00:00:00'
time_name = 'JBTFXT_ENTRYTIME'
columns = [row[0] for row in client.query(columns_sql).result_rows]

print(f"字段数量: {len(columns)}")

# 3. 逐列统计 distinct，同时打印独特值（除了 JBTFXT_ENTRYTIME）
for col in columns:
    sql_count = f"""
    SELECT count(DISTINCT {col})
    FROM {database}.{table}
    WHERE {time_name} >= toDateTime('{start_date}')
      AND {time_name} <  toDateTime('{end_date}')
    """
    try:
        count_result = client.query(sql_count).result_rows[0][0]
        print(f"{col}: {count_result}")
        if col != "{time_name}":
            sql_distinct = f"""
            SELECT DISTINCT {col}
            FROM {database}.{table}
            WHERE {time_name} >= toDateTime('{start_date}')
              AND {time_name} <  toDateTime('{end_date}')
            LIMIT 1000
            """
            distinct_values = client.query(sql_distinct).result_rows
            print(f"{col} 的独特值（最多1000个）: {distinct_values} ")
    except Exception as e:
        print(f"{col}: ERROR -> {e}")
   
        
        
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