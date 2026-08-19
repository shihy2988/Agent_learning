import re
import yaml

# === 你的原始数据（粘进来）===
with open('通风.yaml', 'r', encoding='utf-8') as f:
    raw_data = yaml.safe_load(f)

# === 分类规则 ===
def get_device(key):
    if key.startswith("TF_YH"):
        return "一号风机"
    elif key.startswith("TF_EH"):
        return "二号风机"
    elif key.startswith("TF_SH"):
        return "三号变频器"
    elif key.startswith("TF_SI"):
        return "四号变频器"
    elif "GAO_YA" in key:
        return "高压柜"
    elif "JIN_XIAN" in key:
        return "进线柜"
    elif "MU_LIAN" in key:
        return "母联柜"
    elif "DF" in key:
        return "蝶阀"
    else:
        return "系统级"


def get_type(key):
    if any(x in key for x in ["QI_DONG", "TING_ZHI", "KAI_FA", "GUAN_FA", "FEN_ZHA", "HE_ZHA"]):
        return "控制指令", "DO"
    elif "BAO_JING" in key:
        return "报警", "ALARM"
    elif any(x in key for x in ["SHI_JI_ZHI", "FAN_KUI"]):
        return "模拟量", "AI"
    elif "XI_SHU" in key or "SHE_DING" in key:
        return "参数", "PARAM"
    else:
        return "状态", "DI"


# === 构建 YAML ===
result = {"fan_system": {}}

for key, name in raw_data.items():
    device = get_device(key)
    type_name, io_type = get_type(key)

    result["fan_system"].setdefault(device, {})
    result["fan_system"][device].setdefault(type_name, {})

    result["fan_system"][device][type_name][key] = {
        "name": name,
    }

# === 输出 YAML ===
with open("output.yaml", "w", encoding="utf-8") as f:
    yaml.dump(result, f, allow_unicode=True, sort_keys=False)