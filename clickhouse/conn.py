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
        # 注意：这里不指定 database，因为我们要查 PS 库
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

    client.close()

except Exception as e:
    print(f"❌ 查询失败: {e}")
