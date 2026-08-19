import clickhouse_connect

# 1. 建立连接到指定的 ClickHouse 实例
client = clickhouse_connect.get_client(
    host="10.11.3.210",
    port=8123,
    database="PS",
    username="default",
    password="xt123456"
)

# 查询 default 数据库下所有表
tables_sql = "SHOW TABLES"

tables_result = client.query(tables_sql)
tables = tables_result.result_rows

print("default 数据库下的表数量:", len(tables))
import yaml

hjl_rydw_tables = {}

print("表名列表:")
for row in tables:
    table_name = row[0]
    if table_name.startswith("HJL_RYDW") and not table_name.endswith("_LOC"):
        # 查询表结构
        desc_sql = f"DESCRIBE TABLE `{table_name}`"
        desc_result = client.query(desc_sql)
        # 收集表的字段信息
        hjl_rydw_tables[table_name] = []
        for desc_row in desc_result.result_rows:
            # 将字段名和类型等信息以字典保存
            # desc_row 格式: (name, type, default_type, default_expression, comment, codec_expression, ttl_expression)
            hjl_rydw_tables[table_name].append({
                "name": desc_row[0],
                "type": desc_row[1],
            })

# 保存到 yaml
with open("HJL_RYDW_tables.yaml", "w", encoding="utf-8") as f:
    yaml.dump(hjl_rydw_tables, f, allow_unicode=True, sort_keys=False)

print(f"\n已将所有 HJL_RYDW* 表结构保存到 HJL_RYDW_tables.yaml")
