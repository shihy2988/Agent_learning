# -*- coding: utf-8 -*-
'''
@File    : __init__.py
@IDE     : PyCharm
@Author  : shihongyu
@Date    : 2025/09/01
@Describe: 日志配置模块的初始化文件
'''
from .perosn_sqls import query_person_history,query_person_realtime
from .base_info_sqls import query_person_info,query_area_info,query_base_station_info
from .jizhan_sqls import query_jizhan_history,query_jizhan_realtime
from .warning_info_sqls import query_warning_history,query_warning_realtime
