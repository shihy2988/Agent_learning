import clickhouse_connect

# 连接配置（根据你的实际情况调整）
HOST = "10.11.22.80"
PORT = 9120  # 或 9121
USERNAME = "nethouse"
PASSWORD = "CGC%EVXr.ET10Y_N"  # ← 替换成你的真实密码

try:
    client = clickhouse_connect.get_client(
        host=HOST,
        port=PORT,
        secure=True,
        verify=False,
        username=USERNAME,
        password=PASSWORD,
    )

    # 查询 PS 数据库中的所有表
    query = "SHOW TABLES FROM PS"
    tables = client.query(query).result_rows

    print("📊 数据库 'PS' 中的数据表：")
    if tables:
        for (table_name,) in tables:
            print(f" - {table_name}")
    else:
        print("   （无表）")

    query = """
    SELECT *
    FROM PS.HISTORY_PERSONNEL_LOCATION
    WHERE NAME = %(name)s
    AND UPDATE_TIME BETWEEN toDateTime(%(start)s)
                        AND toDateTime(%(end)s)
    """

    result = client.query(
        query,
        parameters={
            "name": "石小龙",
            "start": "2026-02-30 08:00:00",
            "end": "2026-03-30 18:00:00"
        }
    )

    for row in result.result_rows:
        print(row)
    
    query = """
        SELECT DISTINCT NAME
        FROM PS.HISTORY_PERSONNEL_LOCATION
        WHERE UPDATE_TIME >= today()
        AND UPDATE_TIME < today() + 1
        """

    result = client.query(query)

    names = [row[0] for row in result.result_rows]
    print(names)

    query = """
    SELECT *
    FROM PS.HISTORY_PERSONNEL_LOCATION
    WHERE NAME = %(name)s
    ORDER BY UPDATE_TIME DESC
    LIMIT 1
    """

    result = client.query(
        query,
        parameters={"name": "李利雄"}
    )

    if result.result_rows:
        print("最新记录:", result.result_rows[0])
    else:
        print("未找到该人员数据")
    
    query = """
    SELECT DISTINCT NAME
    FROM PS.HISTORY_PERSONNEL_LOCATION
    WHERE AREANAME = %(area)s
    AND UPDATE_TIME >= %(start)s
    AND UPDATE_TIME < %(end)s
    """

    params = {
        "area": "43204综采工作面",
        "start": "2026-02-01 08:00:00",
        "end": "2026-04-01 18:00:00"
    }

    result = client.query(query, parameters=params)

    names = [row[0] for row in result.result_rows]
    print("出现过的人员:", names)

    
    query = """
    SELECT DISTINCT NAME
    FROM PS.HISTORY_PERSONNEL_LOCATION
    WHERE CLASSTIMENAME = %(class_name)s
    AND UPDATE_TIME >= %(start)s
    AND UPDATE_TIME < %(end)s
    """

    params = {
        "class_name": "早班",
        "start": "2026-03-01 00:00:00",
        "end": "2026-04-02 00:00:00"
    }

    result = client.query(query, parameters=params)

    names = [row[0] for row in result.result_rows]
    print("班次人员:", names)

    client.close()

except Exception as e:
    print(f"❌ 查询失败: {e}")
