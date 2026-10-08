# -*- coding: utf-8 -*-
'''
@File    : __init__.py
@IDE     : PyCharm
@Describe: 淖尔壕煤矿 安全监控(AQJC) ClickHouse 查询层 初始化文件
           表结构见 utils/qyjc_utils/NAO_AQJC_tables_desc.yaml
           数据库: PS_NAO
'''
from .base_info_sqls import (
    query_fzdy_info,      # 分站基本信息
    query_cddy_info,      # 测点基本信息
)
from .substation_sqls import (
    query_fz_history,     # 分站状态历史(状态岛聚合)
    
)
from .point_sqls import (
    query_cd_realtime,    # 测点实时数据(时序, 按天/测点分组)
    query_cd_statistics,  # 测点统计信息
    query_yb_info,        # 测点异常信息
)
