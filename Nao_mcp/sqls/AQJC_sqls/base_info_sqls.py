# -*- coding: utf-8 -*-
'''
@File    : base_info_sqls.py
@Describe: 淖尔壕煤矿 安全监控 —— 基础字典信息(分站/测点)
           对应 NAO_AQJK_tables_desc.yaml:
             ODS_AQJK_FZDY_*  分站基本信息(1D/30D/2Y)
             ODS_AQJK_CDDY_*  测点基本信息(1D/30D/2Y)
             ODS_AQJK_CDSS_*  测点实时数据(1D/30D/2Y) —— 取 SENSOR_TYPE_NAME
           数据源: ClickHouse, database = PS_NAO
'''
import clickhouse_connect
from typing import Optional


# ============================================================
# 编码 -> 中文 映射
# ============================================================

# SYSTEM_CODE	01，安全监控系统；02，瓦斯抽放系统；
SYSTEM_CODE_MAP = {
    "01": "安全监控系统",
    "02": "瓦斯抽放系统",
}

# POINT_VALUE_TYPE	MN,模拟量；LJ，累计量；KG，开关量；DT,多态量
POINT_VALUE_TYPE_MAP = {
    "MN": "模拟量",
    "LJ": "累计量",
    "KG": "开关量",
    "DT": "多态量",
}

# SENSOR_TYPE	传感器类型编码 -> 中文名称
SENSOR_TYPE_MAP = {
    # ---- 模拟量/累计量类 0001~0053 ----
    "0001": "环境瓦斯",
    "0002": "风速",
    "0003": "环境温度",
    "0004": "一氧化碳",
    "0005": "风压",
    "0006": "负压",
    "0007": "水池水位",
    "0008": "煤位",
    "0009": "硫化氢",
    "0010": "水温度",
    "0011": "高低浓度瓦斯",
    "0012": "氧气",
    "0013": "二氧化碳",
    "0014": "粉尘",
    "0015": "电压",
    "0016": "频率",
    "0017": "电流",
    "0018": "湿度",
    "0019": "风量",
    "0020": "顶板离层位移",
    "0021": "坝体位移",
    "0022": "管道瓦斯",
    "0023": "管道温度",
    "0024": "水质",
    "0025": "管道压力",
    "0026": "轴承温度",
    "0027": "噪声",
    "0028": "电机温度",
    "0029": "水库水位",
    "0030": "浸润线",
    "0031": "降雨量",
    "0032": "液压压力",
    "0033": "围岩应力",
    "0034": "钻孔应力",
    "0035": "锚杆应力",
    "0036": "混合瓦斯流量",
    "0037": "纯瓦斯流量",
    "0038": "管道一氧化碳",
    "0039": "氢气",
    "0040": "管道流量",
    "0041": "二氧化氮",
    "0042": "二氧化硫",
    "0043": "激光甲烷",
    "0044": "氨气",
    "0045": "氮气",
    "0046": "乙烯",
    "0047": "乙烷",
    "0048": "压强",
    "0049": "液位",
    "0050": "物位",
    "0051": "开度",
    "0052": "高度",
    "0053": "流量",

    # ---- 开关量类 1001~1015 ----
    "1001": "局部通风机",
    "1002": "风门",
    "1003": "风筒状态",
    "1004": "设备开停",
    "1005": "开关",
    "1006": "风向",
    "1007": "煤仓空满",
    "1008": "烟雾",
    "1009": "断电器",
    "1010": "主通风机",
    "1011": "馈电器",
    "1012": "声光报警器",
    "1013": "计量开停控制器",
    "1014": "控制量",
    "1015": "馈电",

    # ---- 累计量类 3001~3005 ----
    "3001": "产量",
    "3002": "瓦斯抽放量",
    "3003": "排水量",
    "3004": "钩数",
    "3005": "水流量",

    # ---- 状态类 4001~4002 ----
    "4001": "分站",
    "4002": "电源状态",
}

# SENSOR_TYPE 状态含义备注（可选使用，用于展示 0/1/2 的具体含义）
# SENSOR_TYPE 状态含义备注（key 使用传感器中文名称，value 为状态字典）
SENSOR_TYPE_DESC_MAP = {
    "局部通风机": {"0": "停", "1": "开"},
    "风门": {"0": "风门关闭", "1": "风门打开"},
    "风筒状态": {"0": "风筒无风", "1": "风筒有风"},
    "设备开停": {"0": "停止", "1": "开"},
    "开关": {"0": "关", "1": "开"},
    "风向": {"0": "逆风", "1": "顺风"},
    "煤仓空满": {"0": "空仓", "1": "满仓"},
    "烟雾": {"0": "无烟雾", "1": "有烟雾"},
    "断电器": {"0": "断电", "1": "复电"},
    "主通风机": {"0": "停", "1": "开"},
    "馈电器": {"0": "负荷侧无电压", "1": "负荷侧有电压"},
    "声光报警器": {"0": "无报警", "1": "报警"},
    "计量开停控制器": {"0": "停止", "1": "开启"},
    "控制量": {"0": "断开", "1": "合并"},
    "馈电": {"0": "关", "1": "开"},
    "分站": {"0": "故障", "1": "正常"},
    "电源状态": {"0": "无电", "1": "交流供电", "2": "直流供电"},
}

# 表 B.7 测点关联关系字典表
POINT_RELATION_MAP = {
    "B": "闭锁关系",
    "G": "关联风门",
    "K": "控制关系",
    "H": "保护关系",
    "Z": "主备关系",
    "D": "断电关系",
    "T": "调节关系",
}

# 表 B.8 分站运行状态
STATION_RUN_STATUS_MAP = {
    "0": "通信正常",
    "1": "通信中断",
    "2": "故障",
    "9": "未知",
}

# 表 B.9 分站供电状态
STATION_POWER_STATUS_MAP = {
    "0": "直流供电",
    "1": "交流供电",
    "2": "电源故障",
    "9": "未知",
}

POINT_STATUS_BIT_MAP = {
    "0": "正常",
    "1": "报警",
    "2": "断电",
    "4": "标校",
    "8": "超量程",
    "16": "分站故障",
    "32": "不巡检",
    "64": "暂停",
    "128": "传感器故障",
}


def _row_to_zh(field_map, mapping):
    """按 {中文字段: 英文字段} 映射取中文字典"""
    return {zh: field_map.get(en) for zh, en in mapping.items()}


def _map_code(value, code_map, default_prefix=""):
    """
    把编码映射成中文描述；没有匹配时返回原值(可加前缀)
    兼容 None、带空格、大小写不一致等情况
    """
    if value is None:
        return value
    v = str(value).strip()
    if v in code_map:
        return code_map[v]
    v_up = v.upper()
    if v_up in code_map:
        return code_map[v_up]
    return f"{default_prefix}{v}" if default_prefix else value


# ============================================================
# 分站基本信息
# ============================================================
def query_fzdy_info(client, where_clause: Optional[str] = None, chocie='1'):
    """
    查询分站基本信息(字典), 返回 {分站编码: {中文字段: 值}}
    每个分站取 DATA_TIME 最新的一条 (使用 LIMIT 1 BY)
    :param client: clickhouse_connect client
    :param where_clause: 可选 WHERE 子句(不含 WHERE 关键字)
    :param chocie: '1'=天级, '30'=30天, 其他=2年
    """
    if chocie == '1':
        FZDY_TABLE = "ODS_AQJK_FZDY_1D"
    elif chocie == '30':
        FZDY_TABLE = "ODS_AQJK_FZDY_30D"
    else:
        FZDY_TABLE = "ODS_AQJK_FZDY_2Y"

    sql = f"""
        SELECT
            SUBSTATION_CODE,
            SUBSTATION_LOCATION,
            LOC_X,
            LOC_Y,
            LOC_Z,
            DATA_TIME
        FROM {FZDY_TABLE}
    """
    if where_clause:
        sql += f" WHERE {where_clause}\n"
    sql += " ORDER BY DATA_TIME DESC\n"
    sql += " LIMIT 1 BY SUBSTATION_CODE"

    rows = client.query(sql).result_rows
    mapping = {
        "分站编码": "SUBSTATION_CODE",
        "分站安装位置": "SUBSTATION_LOCATION",
        "X坐标": "LOC_X",
        "Y坐标": "LOC_Y",
        "Z坐标": "LOC_Z",
        "数据时间": "DATA_TIME",
    }
    fz_dict = {}
    for row in rows:
        field_map = dict(zip(
            ["SUBSTATION_CODE", "SUBSTATION_LOCATION", "LOC_X", "LOC_Y", "LOC_Z", "DATA_TIME"],
            row
        ))
        info = _row_to_zh(field_map, mapping)
        code = info.get("分站编码")
        if code:
            fz_dict[code] = info
    return fz_dict


# ============================================================
# 测点实时数据 —— 传感器类型名称
# ============================================================
def query_sensor_type_names(client, where_clause: Optional[str] = None, chocie='1'):
    """
    从 ODS_AQJK_CDSS_* 中查询每个测点最新的 SENSOR_TYPE_NAME
    返回 {测点编码: 传感器类型名称}
    :param chocie: '1'=天级, '30'=30天, 其他=2年
    """
    if chocie == '1':
        CDSS_TABLE = "ODS_AQJK_CDSS_1D"
    elif chocie == '30':
        CDSS_TABLE = "ODS_AQJK_CDSS_30D"
    else:
        CDSS_TABLE = "ODS_AQJK_CDSS_2Y"

    sql = f"""
        SELECT
            POINT_CODE,
            SENSOR_TYPE_NAME
        FROM {CDSS_TABLE}
    """
    if where_clause:
        sql += f" WHERE {where_clause}\n"
    sql += " ORDER BY DATA_TIME DESC\n"
    sql += " LIMIT 1 BY POINT_CODE"

    rows = client.query(sql).result_rows
    return {r[0]: r[1] for r in rows if r[0]}


# ============================================================
# 测点基本信息
# ============================================================

def parse_association_string(raw_str: str, fz_dict: dict, name_dict: dict) -> dict:
    """
    解析测点关联关系字符串，支持多行输入、多设备去重、容错处理。
    """
    # 辅助函数：安全获取分站安装位置
    def get_install_location(point_id: str) -> str:
        if not point_id:
            return ""
        station_id = name_dict.get(point_id)
        if not station_id:
            return "未知分站(测点未注册)"
        fz_info = fz_dict.get(station_id)
        if not fz_info:
            return "未知分站(分站未注册)"
        return fz_info.get("分站安装位置", "未知位置")

    result = {}
    seen_lines = set()

    # 1. 按行分割
    lines = raw_str.strip().split("\n")
    
    for line in lines:
        line = line.strip()
        if not line or line in seen_lines:
            continue
        seen_lines.add(line)
        
        # 2. 按 ◇ 拆分多个设备组
        groups = line.split("◇")
        
        for group in groups:
            if not group:
                continue
                
            # 3. 按 : 拆分被控端(K/D等)和控制端
            parts = group.split(":")
            if len(parts) != 2:
                continue
            
            left_part, right_part = parts
            
            # 4. 解析左侧（包含名称）—— 修复点在此处
            left_items = left_part.split("-")
            if len(left_items) >= 3:
                left_rel_code = left_items[0]
                left_point_id = left_items[1]
                # 使用 "-".join() 把剩余部分拼接回来，防止名称中包含 "-"
                device_name = "-".join(left_items[2:]) 
            else:
                continue
                
            # 5. 解析右侧
            right_items = right_part.split("-")
            if len(right_items) >= 2:
                right_rel_code = right_items[0]
                right_point_id = right_items[1]
            else:
                continue
                
            # 6. 组装关系字典
            try:
                relation_data = {
                    "名称": device_name,
                    POINT_RELATION_MAP.get(left_rel_code, left_rel_code): {
                        "测点编码": left_point_id,
                        "分站安装位置": get_install_location(left_point_id)
                    },
                    POINT_RELATION_MAP.get(right_rel_code, right_rel_code): {
                        "测点编码": right_point_id,
                        "分站安装位置": get_install_location(right_point_id) 
                    }
                }
            except:
                import traceback
                traceback.print_exc()
                print('raw_str----------',raw_str,'parts--',parts,'left_items--',left_items,'left_point_id-',left_point_id)

            # 7. 存入结果字典（列表结构防止覆盖）
            if device_name not in result:
                result[device_name] = []
            result[device_name].append(relation_data)

    return result



def query_cddy_info(client, where_clause: Optional[str] = None, chocie='1',
                    fz_dict: Optional[dict] = None,
                    sensor_name_dict: Optional[dict] = None):
    """
    查询测点基本信息(字典), 返回 {测点编码: {中文字段: 值}}
    每个测点取 DATA_TIME 最新的一条 (使用 LIMIT 1 BY)
    :param client: clickhouse_connect client
    :param where_clause: 可选 WHERE 子句(不含 WHERE 关键字)
    :param chocie: '1'=天级, '30'=30天, 其他=2年
    :param fz_dict: 分站信息字典 {分站编码: {中文字段: 值}}，
                    传入后会把测点的"分站编码"替换为对应的分站名字(分站安装位置)
    :param sensor_name_dict: 可选，{测点编码: 传感器类型名称}。
                    不传则内部自动从 ODS_AQJK_CDSS_* 查询(每测点取最新一条)
    """
    if chocie == '1':
        CDDY_TABLE = "ODS_AQJK_CDDY_1D"
    elif chocie == '30':
        CDDY_TABLE = "ODS_AQJK_CDDY_30D"
    else:
        CDDY_TABLE = "ODS_AQJK_CDDY_2Y"

    cols = [
        "POINT_CODE",
        "SYSTEM_CODE",
        "SUBSTATION_CODE",
        "SENSOR_TYPE",
        "POINT_VALUE_TYPE",
        "POINT_VALUE_UNIT",
        "HIGH_RANGE",
        "LOW_RANGE",
        "ALARM_HIGH",
        "UPPER_RELEASE",
        "LOWER_LIMIT",
        "LOWER_RELEASE",
        "UPPER_SHUTDOWN",
        "UPPER_RESTORS",
        "LOWER_SHUTDOWN",
        "LOWER_RESTORE",
        "OPEN_DESC",
        "STOP_DESC",
        "POINT_LOCATION",
        "LOC_X",
        "LOC_Y",
        "LOC_Z",
        "SENSOR_REL",
        "DATA_TIME",
    ]
    sel = ",\n            ".join(cols)
    sql = f"SELECT\n            {sel}\n        FROM {CDDY_TABLE}"
    if where_clause:
        sql += f" WHERE {where_clause}\n"
    sql += " ORDER BY DATA_TIME DESC\n"
    sql += " LIMIT 1 BY POINT_CODE"

    rows = client.query(sql).result_rows

    # ★ 查每个测点最新的 SENSOR_TYPE_NAME（未外部传入时才查）
    if sensor_name_dict is None:
        sensor_name_dict = query_sensor_type_names(client, chocie=chocie)

    mapping = {
        "测点编码": "POINT_CODE",
        "系统编码": "SYSTEM_CODE",
        "分站编码": "SUBSTATION_CODE",
        "传感器类型": "SENSOR_TYPE",
        "数值类型": "POINT_VALUE_TYPE",
        "数值单位": "POINT_VALUE_UNIT",
        "高量程": "HIGH_RANGE",
        "低量程": "LOW_RANGE",
        "上限报警门限": "ALARM_HIGH",
        "上限解报门限": "UPPER_RELEASE",
        "下限报警门限": "LOWER_LIMIT",
        "下限解报门限": "LOWER_RELEASE",
        "上限断电门限": "UPPER_SHUTDOWN",
        "上限复电门限": "UPPER_RESTORS",
        "下限断电门限": "LOWER_SHUTDOWN",
        "下限复电门限": "LOWER_RESTORE",
        "开描述": "OPEN_DESC",
        "停描述": "STOP_DESC",
        "测点安装位置": "POINT_LOCATION",
        "X坐标": "LOC_X",
        "Y坐标": "LOC_Y",
        "Z坐标": "LOC_Z",
        "传感器关联关系": "SENSOR_REL",
        "数据时间": "DATA_TIME",
    }
    cd_dict = {}
    name_dict = {}
    for row in rows:
        name_dict[row[0]] = row[2]
    
    for row in rows:
        field_map = dict(zip(cols, row))
        info = _row_to_zh(field_map, mapping)
        code = info.get("测点编码")
        
        sensor_rel = info.get('传感器关联关系')
        if sensor_rel:
           parsed_dict = parse_association_string(sensor_rel,fz_dict,name_dict)
           info["传感器关联关系"] = parsed_dict
        if code:
            # ★ 编码 -> 中文描述
            info["系统编码"] = _map_code(info.get("系统编码"), SYSTEM_CODE_MAP)
            info["数值类型"] = _map_code(info.get("数值类型"), POINT_VALUE_TYPE_MAP)

            # ★ 追加传感器类型名称(来自 ODS_AQJK_CDSS_*)
            info["传感器类型名称"] = sensor_name_dict.get(code)
            # ★ 用分站信息字典把"分站编码"替换为分站名字(分站安装位置)
            fz_code = info.get("分站编码")
            if fz_dict and fz_code in fz_dict:
                fz_name = fz_dict[fz_code].get("分站安装位置")
                if fz_name:
                    info["分站安装位置"] = fz_name
            cd_dict[code] = info
    
    return cd_dict


# ============================================================
# 测试入口
# ============================================================

if __name__ == "__main__":
    client = clickhouse_connect.get_client(
        host="10.11.3.210",
        port=8123,
        database="PS_NAO",
        username="default",
        password="xt123456",
        autogenerate_session_id=False
    )

    print("=" * 50)
    print("查询分站基本信息...")
    fz = query_fzdy_info(client)
    print(f"分站数量: {len(fz)}")
    sensor_types = [data['分站编码'] for key,data in fz.items()]
    print(f"分站数量: {set(sensor_types)} ")
    if fz:
        k = list(fz.keys())[0]
        print("示例分站:", k, fz[k])

    print("\n" + "=" * 50)
    print("查询测点基本信息...")
    cd = query_cddy_info(client, fz_dict=fz)   # ★ 内部会自动补 SENSOR_TYPE_NAME
    print(f"测点数量: {len(cd)} ")
    
    sensor_types = [data['测点编码'] for key,data in cd.items()]
    print(f"sensor_types: {sorted(set(sensor_types))} ")
    if cd:
        k = list(cd.keys())[0]
        print("示例测点:", k, cd[k])