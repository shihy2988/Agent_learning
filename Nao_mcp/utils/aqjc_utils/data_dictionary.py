# -*- coding: utf-8 -*-
'''
@File    : data_dictionary.py
@Describe: 淖尔壕煤矿 安全监控 数据字典加载器
           读取 NAO_AQJC_tables_desc.yaml, 提供 表->字段->中文描述 查询。
'''
import os
import yaml

current_dir = os.path.dirname(os.path.abspath(__file__))
DEFAULT_YAML = os.path.join(current_dir, "NAO_AQJC_tables_desc.yaml")

# 表名 -> 中文业务含义(按表前缀归类)
TABLE_CATEGORY = {
    "FZDY": "分站基本信息",
    "FZSS": "分站实时数据",
    "CDDY": "测点基本信息",
    "CDSS": "测点实时数据",
    "TJSJ": "测点统计信息",
    "YCBJ": "测点异常信息",
}

_cache = None


def load_tables_desc(yaml_path: str = None) -> dict:
    """
    加载数据字典, 返回:
      {
        表名: {
            "desc": 表描述,
            "category": 业务分类,
            "fields": { 字段名: 中文描述, ... }
        }
      }
    结果缓存, 只读一次。
    """
    global _cache
    if _cache is not None and yaml_path is None:
        return _cache

    path = yaml_path or DEFAULT_YAML
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    out = {}
    for table, entries in (raw or {}).items():
        fields = {}
        table_desc = ""
        for item in entries or []:
            if not isinstance(item, dict):
                continue
            if "name" not in item:
                # 第一行 {"desc": ...} 是表描述
                if "desc" in item:
                    table_desc = item["desc"]
                continue
            fields[item["name"]] = item.get("desc", "")
        # 业务分类: 取表名中 ODS_AQJK_ 后的前缀
        prefix = table.replace("ODS_AQJK_", "")[:4]
        out[table] = {
            "desc": table_desc,
            "category": TABLE_CATEGORY.get(prefix, ""),
            "fields": fields,
        }

    if yaml_path is None:
        _cache = out
    return out


def get_table_desc(table_name: str) -> dict:
    """获取单表描述, 不存在返回 {}"""
    return load_tables_desc().get(table_name, {})


def list_tables() -> dict:
    """返回 表名 -> {desc, category} 概览"""
    return {
        t: {"desc": info["desc"], "category": info["category"]}
        for t, info in load_tables_desc().items()
    }


if __name__ == "__main__":
    for t, info in list_tables().items():
        print(f"{t}  [{info['category']}]  {info['desc']}")
    print("\n示例表字段:")
    import json
    print(json.dumps(get_table_desc("ODS_AQJK_CDDY_30D"), ensure_ascii=False, indent=2))
