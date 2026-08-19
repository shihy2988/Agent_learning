sql = """
SELECT *
FROM PS.HISTORY_PERSONNEL_LOCATION
WHERE UPDATE_TIME >= now() - INTERVAL 1 DAY  -- 仅查最近1天，根据实际数据密度调整
 AND NAME = '温建群'
ORDER BY UPDATE_TIME DESC
LIMIT 20
"""

if __name__ == "__main__":
    import json
    import clickhouse_connect
    client = clickhouse_connect.get_client(
        host="10.11.22.80",
        port=9120,
        user="nethouse",
        password="CGC%EVXr.ET10Y_N",
        database="PS",
        secure=True,verify=False 
    )
    result = client.query(sql)
    columns = result.column_names
    rows = [dict(zip(columns, row)) for row in result.result_rows]
   
    print(json.dumps(rows, ensure_ascii=False, indent=2, default=str))
