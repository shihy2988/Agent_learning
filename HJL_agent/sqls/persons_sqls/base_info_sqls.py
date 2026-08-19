import clickhouse_connect
from collections import Counter



def query_person_info(client,where_clause=None):
    """
    查询人员信息，可根据 where_clause 进行条件筛选。
    :param where_clause: str, 可选的SQL WHERE子句（不含WHERE关键字）
    :return: 查询结果列表
    """
    base_sql = """
        SELECT
            CARD_ID,
            MINE_CODE,
            MINE_NAME,
            EMPLOYEE_NAME,
            EMPLOYEE_JOB,
            EMPLOYEE_DUTE,
            EMPLOYEE_DEPT,
            EMPLOYEE_BIR,
            EMPLOYEE_EDU,
            IS_LEADER,
            IS_SPECIAL,
            TIME
        FROM HJL_RYDW_RYXX
    """
    if where_clause:
        base_sql += f" WHERE {where_clause}\n"

    base_sql += """
        GROUP BY
            CARD_ID,
            MINE_CODE,
            MINE_NAME,
            EMPLOYEE_NAME,
            EMPLOYEE_JOB,
            EMPLOYEE_DUTE,
            EMPLOYEE_DEPT,
            EMPLOYEE_BIR,
            EMPLOYEE_EDU,
            IS_LEADER,
            IS_SPECIAL,
            TIME
        """
    tables_result = client.query(base_sql)
    rows = tables_result.result_rows

    # 中文key按照 HJL_RYDW_tables_desc.yaml (298-322) 约定，去掉"时间"
    keys = [
        "人员卡编码",    # CARD_ID
        "煤矿编码",      # MINE_CODE
        "煤矿名称",      # MINE_NAME
        "姓名",          # EMPLOYEE_NAME
        "工种",          # EMPLOYEE_JOB
        "职务",          # EMPLOYEE_DUTE
        "队组班组/部门",  # EMPLOYEE_DEPT
        "出生年月",       # EMPLOYEE_BIR
        "学历",          # EMPLOYEE_EDU
        "是否矿领导",     # IS_LEADER
        "是否特种人员"    # IS_SPECIAL
    ]

    # SQL字段顺序
    field_order = [
        "CARD_ID",
        "MINE_CODE",
        "MINE_NAME",
        "EMPLOYEE_NAME",
        "EMPLOYEE_JOB",
        "EMPLOYEE_DUTE",
        "EMPLOYEE_DEPT",
        "EMPLOYEE_BIR",
        "EMPLOYEE_EDU",
        "IS_LEADER",
        "IS_SPECIAL",
        "TIME"
    ]

    person_dict = {}
    for row in rows:
        field_map = dict(zip(field_order, row))
        # 构造中文key的dict，去掉"时间"
        zh_map = {
            "人员卡编码": field_map["CARD_ID"],
            "姓名": field_map["EMPLOYEE_NAME"],
            "工种": field_map["EMPLOYEE_JOB"],
            "职务": field_map["EMPLOYEE_DUTE"],
            "队组班组/部门": field_map["EMPLOYEE_DEPT"],
            "出生年月": field_map["EMPLOYEE_BIR"],
            "学历": field_map["EMPLOYEE_EDU"],
            "是否矿领导": "否" if field_map["IS_LEADER"]=='0' else "是",
            "是否特种人员": "否" if field_map["IS_SPECIAL"] else "是"
        }
        key = f"{zh_map['姓名']}_{zh_map['人员卡编码']}"
        person_dict[key] = zh_map
    return person_dict

def query_area_info(client):
    """
    查询区域信息，返回以区域编码为key的dict，value为中文字段名的dict
    参考SQL（见HJL_RYDW_MCP_SQL.sql第8-17行）:
        SELECT
            AREA_CODE,
            argMax(MINE_CODE, TIME) AS MINE_CODE,
            argMax(MINE_NAME, TIME) AS MINE_NAME,
            argMax(AREA_NAME, TIME) AS AREA_NAME,
            argMax(AREA_TYPE, TIME) AS AREA_TYPE,
            argMax(NUM_VALUE, TIME) AS NUM_VALUE,
            max(TIME) AS TIME
        FROM HJL_RYDW_RYQY
        GROUP BY AREA_CODE;

    :param client: clickhouse_connect client
    :return: dict {区域编码: {...中文字段:值...}}
    """
    area_sql = """
        SELECT
            AREA_CODE,
            MINE_CODE,
            MINE_NAME,
            AREA_NAME,
            AREA_TYPE,
            NUM_VALUE,
            TIME
        FROM HJL_RYDW_RYQY
        GROUP BY
            AREA_CODE,
            MINE_CODE,
            MINE_NAME,
            AREA_NAME,
            AREA_TYPE,
            NUM_VALUE,
            TIME
   
    """
    rows = client.query(area_sql).result_rows
    field_order = [
        "AREA_CODE",
        "MINE_CODE",
        "MINE_NAME",
        "AREA_NAME",
        "AREA_TYPE",
        "NUM_VALUE",
        "TIME"
    ]
    zh_fields = [
        "区域编码",    # AREA_CODE
        "煤矿编码",   # MINE_CODE
        "煤矿名称",   # MINE_NAME
        "区域名称",   # AREA_NAME
        "区域类型",   # AREA_TYPE
        "区域核定人数" # NUM_VALUE
        # 时间不映射
    ]
    area_dict = {}
    for row in rows:
        field_map = dict(zip(field_order, row))
        zh_map = {
            "区域编码": field_map["AREA_CODE"],
            "区域名称": field_map["AREA_NAME"],
            "区域类型": field_map["AREA_TYPE"],
            "区域核定人数": field_map["NUM_VALUE"]
        }
        area_dict[field_map["AREA_CODE"]] = zh_map
    return area_dict

def query_base_station_info(client):
    """
    查询基站信息表（HJL_RYDW_RYJZ），按基站编码聚合，返回字典形式，中文字段名
    无 argMax，直接取表字段
    """
    base_station_sql = """
        SELECT
            BASE_STATION_CODE,
            MINE_CODE,
            MINE_NAME,
            BASE_STATION_NAME,
            COORDINATE_X,
            COORDINATE_Y,
            COORDINATE_Z,
            LOCATION_NOTE,
            TIME
        FROM HJL_RYDW_RYJZ
        GROUP BY
            BASE_STATION_CODE,
            MINE_CODE,
            MINE_NAME,
            BASE_STATION_NAME,
            COORDINATE_X,
            COORDINATE_Y,
            COORDINATE_Z,
            LOCATION_NOTE,
            TIME
    """
    rows = client.query(base_station_sql).result_rows
    field_order = [
        "BASE_STATION_CODE",
        "MINE_CODE",
        "MINE_NAME",
        "BASE_STATION_NAME",
        "COORDINATE_X",
        "COORDINATE_Y",
        "COORDINATE_Z",
        "LOCATION_NOTE",
        "TIME"
    ]
    base_station_dict = {}
    for row in rows:
        field_map = dict(zip(field_order, row))
        zh_map = {
            "基站编码": field_map["BASE_STATION_CODE"],
            "基站名称": field_map["BASE_STATION_NAME"],
            "X坐标": field_map["COORDINATE_X"],
            "Y坐标": field_map["COORDINATE_Y"],
            "Z坐标": field_map["COORDINATE_Z"],
            "位置注释": field_map["LOCATION_NOTE"],
            # "时间": field_map["TIME"]
        }
        base_station_dict[field_map["BASE_STATION_CODE"]] = zh_map

    return base_station_dict



if __name__ == "__main__":
    client = clickhouse_connect.get_client(
        host="10.11.3.210",
        port=8123,
        database="PS",
        username="default",
        password="xt123456"
        )
    data = query_base_station_info(client)
    print(data)
