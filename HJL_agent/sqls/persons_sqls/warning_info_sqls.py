import clickhouse_connect
from collections import defaultdict
from .base_info_sqls import  query_base_station_info,query_area_info

def _group_rows_by_key(rows, field_order, unique_keys):
    """
    根据unique_keys去重(rows为list of tuple)，返回list of dict
    保留每组的最新一条（以TIME最大为准）
    针对同一个人（CARD_ID）多条记录开始时间一样但有一条有结束时间，有结束时间的优先；都没有结束时间则保留开始
    """
    grouped = {}
    # 用于保存同一个{CARD_ID, BEGIN_XXX_TIME}下有无END_XXX_TIME
    check_fields = [
        ("BEGIN_ALARM_TIME", "END_ALARM_TIME"),
        ("BEGIN_SOS_TIME", "END_SOS_TIME")
    ]
    # 只对报警类型相关表有效，所以简化成下面针对超时/限制区报警和求救报警两种
    # key: (CARD_ID, BEGIN_XXX_TIME) value: {'with_end': record, 'without_end': record}
    special_group = {}

    for row in rows:
        record = dict(zip(field_order, row))
        # 先普通处理，便于兼容所有类型
        key = tuple(record.get(k) for k in unique_keys)
        cur_time = record.get("TIME")
        # 是否为报警或求救型
        handled = False
        for begin_field, end_field in check_fields:
            if begin_field in record:
                # CARD_ID 有，且 BEGIN_XXX_TIME 有
                person_key = (record.get("CARD_ID"), record.get(begin_field))
                if person_key not in special_group:
                    special_group[person_key] = {"with_end": None, "without_end": None}
                if record.get(end_field):
                    # 有结束时间，保留TIME最大
                    current = special_group[person_key]["with_end"]
                    if (current is None) or (cur_time and (not current.get("TIME") or current.get("TIME") < cur_time)):
                        special_group[person_key]["with_end"] = record
                else:
                    # 没有结束时间，保留TIME最大
                    current = special_group[person_key]["without_end"]
                    if (current is None) or (cur_time and (not current.get("TIME") or current.get("TIME") < cur_time)):
                        special_group[person_key]["without_end"] = record
                handled = True
                break  # 只会命中一种
        if not handled:
            # 普通唯一去重（如超员报警）
            if key not in grouped or (cur_time and grouped[key].get("TIME") < cur_time):
                grouped[key] = record

    # 汇总整理
    result_records = []

    # 有特殊处理的报警类型
    for val in special_group.values():
        # 如果有with_end优先用with_end，否则用without_end
        if val["with_end"] is not None:
            result_records.append(val["with_end"])
        elif val["without_end"] is not None:
            result_records.append(val["without_end"])

    # 其它类型
    result_records.extend(grouped.values())

    return result_records

def query_warning_history(client, where_clause=None):
    """
    查询4类报警历史信息（超时、超员、求救、限制区），返回key为中文类型的dict，自动去除同一信息（按关键信息分组，取TIME最大/最新记录）
    特别处理：同一卡号同开始时间，优先保留有结束时间的，否则仅保留没有结束时间的
    """
    tables = [
        {
            "table": "HJL_RYDW_RYCS",
            "key": "超时报警",
            "fields": [
                 "CARD_ID", "EMPLOYEE_NAME", "ENTRY_WELL_TIME",
                "BEGIN_ALARM_TIME", "END_ALARM_TIME", "AREA_CODE", "ENTRY_AREA_TIME",
                "BASE_STATION_CODE", "ENTRY_BASE_TIME", "TIME"
            ],
            "fields_zh": [
                 "人员卡编码", "姓名", "入井时刻",
                "报警开始时间", "报警结束时间", "区域名称", "进入当前区域时间",
                "基站名称", "进入当前基站时间", "时间"
            ],
            "unique_keys": ["CARD_ID", "ENTRY_WELL_TIME", "BEGIN_ALARM_TIME", "END_ALARM_TIME"]
        },
        {
            "table": "HJL_RYDW_RYCY",
            "key": "超员报警",
            "fields": [
                 "ALARM_TYPE", "CAPACITY_NUM", "TOTAL_PEOPLE_NOW",
                "AREA_CODE", "AREA_NAME", "BEGIN_ALARM_TIME", "END_ALARM_TIME", "AREA_PEOPLE_COLLECT", "TIME"
            ],
            "fields_zh": [
                 "报警类型", "定员数", "当前总人数",
                "区域名称", "区域名称", "报警开始时间", "报警结束时间", "区域人员集合", "时间"
            ],
            "unique_keys": ["AREA_CODE", "BEGIN_ALARM_TIME", "END_ALARM_TIME"]
        },
        {
            "table": "HJL_RYDW_RYQJ",
            "key": "求救报警",
            "fields": [
                 "CARD_ID", "EMPLOYEE_NAME", "BEGIN_SOS_TIME", "END_SOS_TIME",
                "ENTRY_WELL_TIME", "AREA_CODE", "ENTRY_AREA_TIME", "BASE_STATION_CODE", "ENTRY_BASE_TIME", "TIME"
            ],
            "fields_zh": [
                 "人员卡编码", "姓名", "求救开始时间", "求救结束时间",
                "入井时间", "当前所在区域名称", "进入当前区域时间", "当前所在基站名称", "进入基站时间", "时间"
            ],
            "unique_keys": ["CARD_ID", "BEGIN_SOS_TIME", "END_SOS_TIME"]
        },
        {
            "table": "HJL_RYDW_RYXZ",
            "key": "限制区报警",
            "fields": [
                 "CARD_ID", "EMPLOYEE_NAME", "BEGIN_ALARM_TIME", "END_ALARM_TIME",
                "ENTRY_WELL_TIME", "AREA_CODE", "ENTRY_AREA_TIME", "BASE_STATION_CODE", "ENTRY_BASE_TIME", "TIME"
            ],
            "fields_zh": [
                 "人员卡编码", "姓名", "报警开始时间", "报警结束时间",
                "入井时间", "当前所在区域名称", "进入当前区域时间", "当前所在基站名称", "进入当前基站时间", "时间"
            ],
            "unique_keys": ["CARD_ID", "AREA_CODE", "BEGIN_ALARM_TIME", "END_ALARM_TIME"]
        }
    ]
    area_info = query_area_info(client)
    jizhen_info =  query_base_station_info(client)
    result = {}
    for t in tables:
        sql = f"SELECT {','.join(t['fields'])} FROM {t['table']}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        sql += " ORDER BY TIME"
        rows = client.query(sql).result_rows
        # 分组去重，自动处理有无结束时间
        grouped = _group_rows_by_key(rows, t["fields"], t["unique_keys"])
        records = []
        for record in grouped:
            record_zh = {}
            for k, f in zip(t["fields_zh"], t["fields"]):
                if f == "TIME":
                    continue
                value = record.get(f, None)
                # 匹配区域名称和基站编码对应的名称
                if f == "AREA_CODE":
                    # 返回区域名称（若存在），否则保持原编码
                    value = area_info.get(value, {}).get("区域名称", value)
                elif f == "BASE_STATION_CODE":
                    # 返回基站名称（若存在），否则保持原编码
                    value = jizhen_info.get(value, {}).get("基站名称", value)
                record_zh[k] = value
            records.append(record_zh)
     
        result[t["key"]] = records
    return result


def query_warning_realtime(client, where_clause=None):
    """
    查询4类报警实时信息（超时、超员、求救、限制区），返回key为中文类型的dict，自动去除同一信息（按关键信息分组，取TIME最大/最新记录）
    特别处理：同一卡号同开始时间，优先保留有结束时间的，否则仅保留没有结束时间的
    """
    tables = [
        {
            "table": "HJL_RYDW_RYCS_REAL_TIME",
            "key": "超时报警",
            "fields": [
                 "CARD_ID", "EMPLOYEE_NAME", "ENTRY_WELL_TIME",
                "BEGIN_ALARM_TIME", "END_ALARM_TIME", "AREA_CODE", "ENTRY_AREA_TIME",
                "BASE_STATION_CODE", "ENTRY_BASE_TIME", "TIME"
            ],
            "fields_zh": [
                 "人员卡编码", "姓名", "入井时刻",
                "报警开始时间", "报警结束时间", "区域名称", "进入当前区域时间",
                "基站名称", "进入当前基站时间", "时间"
            ],
            "unique_keys": ["CARD_ID", "ENTRY_WELL_TIME", "BEGIN_ALARM_TIME", "END_ALARM_TIME"]
        },
        {
            "table": "HJL_RYDW_RYCY_REAL_TIME",
            "key": "超员报警",
            "fields": [
                 "ALARM_TYPE", "CAPACITY_NUM", "TOTAL_PEOPLE_NOW",
                "AREA_CODE", "AREA_NAME", "BEGIN_ALARM_TIME", "END_ALARM_TIME", "AREA_PEOPLE_COLLECT", "TIME"
            ],
            "fields_zh": [
                 "报警类型", "定员数", "当前总人数",
                "区域名称", "区域名称", "报警开始时间", "报警结束时间", "区域人员集合", "时间"
            ],
            "unique_keys": ["AREA_CODE", "BEGIN_ALARM_TIME", "END_ALARM_TIME"]
        },
        {
            "table": "HJL_RYDW_RYQJ_REAL_TIME",
            "key": "求救报警",
            "fields": [
                 "CARD_ID", "EMPLOYEE_NAME", "BEGIN_SOS_TIME", "END_SOS_TIME",
                "ENTRY_WELL_TIME", "AREA_CODE", "ENTRY_AREA_TIME", "BASE_STATION_CODE", "ENTRY_BASE_TIME", "TIME"
            ],
            "fields_zh": [
                 "人员卡编码", "姓名", "求救开始时间", "求救结束时间",
                "入井时间", "当前所在区域名称", "进入当前区域时间", "当前所在基站名称", "进入基站时间", "时间"
            ],
            "unique_keys": ["CARD_ID", "BEGIN_SOS_TIME", "END_SOS_TIME"]
        },
        {
            "table": "HJL_RYDW_RYXZ_REAL_TIME",
            "key": "限制区报警",
            "fields": [
                 "CARD_ID", "EMPLOYEE_NAME", "BEGIN_ALARM_TIME", "END_ALARM_TIME",
                "ENTRY_WELL_TIME", "AREA_CODE", "ENTRY_AREA_TIME", "BASE_STATION_CODE", "ENTRY_BASE_TIME", "TIME"
            ],
            "fields_zh": [
                 "人员卡编码", "姓名", "报警开始时间", "报警结束时间",
                "入井时间", "当前所在区域名称", "进入当前区域时间", "当前所在基站名称", "进入当前基站时间", "时间"
            ],
            "unique_keys": ["CARD_ID", "AREA_CODE", "BEGIN_ALARM_TIME", "END_ALARM_TIME"]
        }
    ]
    result = {}
    area_info = query_area_info(client)
    jizhen_info =  query_base_station_info(client)
    for t in tables:
        sql = f'SELECT {",".join(t["fields"])} FROM {t["table"]}'
        if where_clause:
            sql += f" WHERE {where_clause}"
        sql += " ORDER BY TIME"
        rows = client.query(sql).result_rows
        # 分组去重，自动处理有无结束时间
        grouped = _group_rows_by_key(rows, t["fields"], t["unique_keys"])
        records = []
        for record in grouped:
            record_zh = {}
            for k, f in zip(t["fields_zh"], t["fields"]):
                if f == "TIME":
                    continue
                value = record.get(f, None)
                # 匹配区域名称和基站编码对应的名称
                if f == "AREA_CODE":
                    # 返回区域名称（若存在），否则保持原编码
                    value = area_info.get(value, {}).get("区域名称", value)
                elif f == "BASE_STATION_CODE":
                    # 返回基站名称（若存在），否则保持原编码
                    value = jizhen_info.get(value, {}).get("基站名称", value)
                record_zh[k] = value
            records.append(record_zh)
 
        result[t["key"]] = records
    return result


if __name__ == "__main__":
    client = clickhouse_connect.get_client(
        host="10.11.3.210",
        port=8123,
        database="PS",
        username="default",
        password="xt123456"
    )
    # 查询4种历史报警信息
    history_data = query_warning_history(client)
    print("历史报警：", history_data)
    # 查询4种实时报警信息
    realtime_data = query_warning_realtime(client)
    print("实时报警：", realtime_data)
