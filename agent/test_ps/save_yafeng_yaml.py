import re
import yaml
from collections import defaultdict

# ====== 1. 原始数据（把你232条粘进这里） ======
with open('压风.yaml', 'r', encoding='utf-8') as f:
    data = yaml.safe_load(f)



# ====== 2. 分类规则 ======

# 根据 @压风.yaml (1-166) 进行分类

def classify_type(key, value):
    """
    分类：控制 / 状态 / 报警 / 模拟量 / 参数
    优先级（高->低）:
    1. 控制指令（QI_DONG, TING_ZHI, FEN_ZHA, HE_ZHA, XUAN_ZE, SHE_DING, TOU_RU, BEI_YONG, JIAN_XIU, QING_LING）
    2. 报警（BAO_JING, YU_JING, TI_XING, _YAN_WU 系列）
    3. 状态（ZHUANG_TAI, WEI, YI_, CHU_YU, _ZHI_）
    4. 数值/模拟量（WEN_DU, YA_LI, LIU_LIANG, SHI_DU, DIAN_LIU, GONG_LV, YIN_SHU, ZHOU_WEN, CHANG_DU, GONG_SHI, JIA_ZAI, PAI_QI, PIN_LV, GONG_LV）
    5. 其它 => 参数
    """
    # 1. 控制指令
    if any(x in key for x in [
        "QI_DONG", "TING_ZHI", "FEN_ZHA", "HE_ZHA",
        "XUAN_ZE",  "TOU_RU", "BEI_YONG", "JIAN_XIU", "QING_LING"
        ]):
        return "指令"
    elif any(x in key for x in [
         "SHE_DING"
        ]):
        return "设定值"
    # 2. 报警
    elif any(x in key for x in [
        "BAO_JING", "YU_JING", "TI_XING", "YAN_WU_BAO_JING", "YAN_WU_YU_JING"
        ]):
        return "报警"
    # 3. 状态
    elif any(x in key for x in [
        "ZHUANG_TAI", "WEI", "YI_", "CHU_YU"
        ]) or ( "ZHI" in key and ("TING_ZHI" in key or "JIA_ZAI_ZHI" in key or "GONG_LV_ZHI" in key)):
        return "信号"
    # 4. 数值/模拟量
    elif any(x in key for x in [
        "WEN_DU", "YA_LI", "LIU_LIANG", "SHI_DU", "DIAN_LIU", "GONG_LV", "YIN_SHU", "ZHOU_WEN",
        "CHANG_DU", "GONG_SHI", "JIA_ZAI", "PAI_QI", "PIN_LV"
        ]):
        return "监测值"
    # 5. 其它都归入参数
    else:
        return "参数"

def classify_device(key, value):
    """
    分类设备 按照前缀组合和关键字
    - 控制系统分为
      - 断路器（DUAN_LU_QI, MU_LIAN_DUAN_LU_QI）
      - 空压机（KONG_YA_JI, FENG_BAO, GUAN_DAO, PAI_QI)
      - 配电室/机房/操作室（PEI_DIAN_SHI, JI_FANG, CAO_ZUO_SHI, ZHEN_DONG, YAN_WU）
      - 备用/检修/投运等系统级参数（BEI_YONG, JIAN_XIU, TING_ZHI, TOU_RU, ZI_DONG, SHI_YAN, ...）
    """
    if "DUAN_LU_QI" in key:
        if "MU_LIAN" in key:
            return "母联断路器"
        else:
            return "断路器"
    elif any(x in key for x in ["KONG_YA_JI", "_HAO", "_GUAN_DAO","_FENG_BAO"]):  
        if "1" in key:
            return "1 号空压机"
        elif "2" in key:
            return "2 号空压机"
        elif "3" in key:
            return "3 号空压机"
        elif "4" in key:
            return "4 号空压机"
        else:
            return "空压机"
    elif "FENG_BAO" in key or "GUAN_DAO" in key or "PAI_QI" in key:
        return "空压机子系统"
    elif any(x in key for x in ["PEI_DIAN_SHI", "JI_FANG", "CAO_ZUO_SHI"]):
        return "环境系统"
    elif "ZHEN_DONG" in key:
        return "振动系统"
    elif "YAN_WU" in key:
        return "烟雾系统"
    elif any(x in key for x in ["BEI_YONG", "JIAN_XIU", "TOU_RU", "ZI_DONG", "SHI_YAN"]):
        return "系统级"
    else:
        # 剩余全部归为系统级
        return "系统级"

# ====== 3. 构建结构 ======

result = defaultdict(lambda: defaultdict(dict))

for k, v in data.items():
    device = classify_device(k, v)
    dtype = classify_type(k, v)
    result[device][dtype][k] = v

# ====== 4. 输出 YAML ======

yaml_data = {"fan_system": result}

def convert(obj):
    if isinstance(obj, defaultdict):
        obj = dict(obj)
    if isinstance(obj, dict):
        return {k: convert(v) for k, v in obj.items()}
    return obj

yaml_data = convert(yaml_data)

print(yaml.dump(
    yaml_data,
    allow_unicode=True,
    sort_keys=False
))

# ====== 5. 可选：写入文件 ======
with open("yafeng_system.yaml", "w", encoding="utf-8") as f:
    yaml.dump(yaml_data, f, allow_unicode=True, sort_keys=False)