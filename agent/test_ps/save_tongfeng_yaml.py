import re
import yaml
from collections import defaultdict

# ====== 1. 原始数据（把你232条粘进这里） ======
with open('通风.yaml', 'r', encoding='utf-8') as f:
    data = yaml.safe_load(f)



# ====== 2. 分类规则 ======

def classify_type(key, value):
    """分类：控制 / 状态 / 报警 / 模拟量 / 参数"""
    if any(x in key for x in ["QI_DONG", "TING_ZHI", "KAI_FA", "GUAN_FA", "FEN_ZHA", "HE_ZHA", "QIE_HUAN","_FU_WEI","_SHI_HUA"]) \
        and not any(x in key for x in ["DONG_ZHONG", "ZHI_ZHONG","XING_ZHONG","_QIE_","_XIN_HAO"]):
        return "控制指令"
    elif any (x in key for x in ["BAO_JING", "_YAN_WU"]):
        return "报警"
    elif any(x in key for x in ["XI_SHU", "SHE_DING", "BIAN_BI"]):
        return "修正系数"
    elif any(x in key for x in ["SHI_JI_ZHI", "FAN_KUI", "PIN_LV","_XIAO_LV","TF_FENG_TONG_JIE_MIAN_JI"]) \
        and not any(x in key for x in ["_BIAN_BI"]):
        return "数值"
    elif any(x in key for x in ["_XIN_HAO","_YAN_WU","_ZAI_GUAN","_ZAI_KAI","_BAO_JING","JIE_MIAN_JI","_BIAN_BI","TF_CHENG_XU_ZHI_XING_ZHONG"]):
        return "信号"
    elif any(x in key for x in ["_QIE_","_XING_ZHONG"]):
        return "切换过程"
    else:
        return "状态"


def classify_device(key, value):
    """分类设备（兼容如 TF_YH_GAO_YA_...、TF_YH_JIN_XIAN_... 一类归入高压柜/进线柜，而不是一号风机系统）"""
    # 先细分高压柜、进线柜、母联柜优先级，再区分风机本体
    if key.startswith("TF_YH_GAO_YA_"):
        return "高压柜系统"
    elif key.startswith("TF_YH_JIN_XIAN_"):
        return "进线柜系统"
    elif key.startswith("TF_YH_MU_LIAN_"):
        return "母联柜系统"
    elif key.startswith("TF_YH_"):
        return "一号风机系统"
    elif key.startswith("TF_EH_GAO_YA_"):
        return "高压柜系统"
    elif key.startswith("TF_EH_JIN_XIAN_"):
        return "进线柜系统"
    elif key.startswith("TF_EH_MU_LIAN_"):
        return "母联柜系统"
    elif key.startswith("TF_EH_"):
        return "二号风机系统"
    elif "BPQ_" in key:
        return "变频器"
    elif "SPFM_" in key :
        return "风门"
    elif "DF_" in key or "FM_" in key:
        return "阀门系统"
    elif "GAO_YA_" in key:
        return "高压柜系统"
    elif "JIN_XIAN_" in key:
        return "进线柜系统"
    elif "MU_LIAN_" in key:
        return "母联柜系统"
    
    else:
        if key.startswith("TF_YH"):
            return "一号风机系统"
        elif key.startswith("TF_EH"):
            return "二号风机系统"
        elif "BIAN_PIN_QI" in key:
            return "变频器"
        else:
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
with open("fan_system.yaml", "w", encoding="utf-8") as f:
    yaml.dump(yaml_data, f, allow_unicode=True, sort_keys=False)