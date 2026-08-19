#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文件名:	person_utils.py
作者:	shihy
创建日期:	2026-04-22
描述:	矿井人员定位相关的数据处理与工具函数。用于时间格式化、筛选、统计分析和结构化人员信息，适配 MCP 服务人员定位业务功能需求。
"""


import re
from datetime import datetime
from typing import Dict, List, Optional
import redis
import time

import requests
import json
from pprint import pp, pprint
import datetime
from fastmcp import FastMCP
from collections import defaultdict

from email import message
import json
import logging
import re
import urllib3
import requests
import clickhouse_connect
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Union
from fastmcp import FastMCP

import sys
import os
from pprint import pprint
from collections import defaultdict
import copy
from fuzzywuzzy import fuzz, process
from tqdm import tqdm

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqls.person_sqls import (
    GET_PERSON_LATEST_SQL,
    GET_PERSON_TRAJECTORY_SQL,
    GET_TODAY_PERSONS_SQL,
    GET_REALTIME_STATION_HEAD_INFO_SQL,
    GET_AREA_LIMITS_SQL, GET_TODAY_CARS_SQL
)

# 配置日志，日志存储在文件中
from logging.handlers import RotatingFileHandler

LOG_DIR = 'logs'
LOG_FILE = os.path.join(LOG_DIR, 'mine_personnel_service.log')

# 确保日志目录存在
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=50 * 1024 * 1024,  # 50MB
    backupCount=5,
    encoding='utf-8'
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logging.getLogger().setLevel(logging.INFO)
logging.getLogger().addHandler(handler)
# 防止日志重复记录（如果已有stream handler则移除）
for h in logging.getLogger().handlers:
    if isinstance(h, logging.StreamHandler) and not isinstance(h, RotatingFileHandler):
        logging.getLogger().removeHandler(h)
logger = logging.getLogger("MinePersonnelService")



class PersonBase:
    """
    人员相关功能的基类，供工具与utils共用。
    主要封装通用的人员信息方法和属性。实际业务类可继承它。
    """

    def __init__(self,
            mcp: FastMCP,
            host: str,
            port: int,
            user: str,
            password: str,
            database: str,
            ):
        # 可以定义一些共用的属性，如站点名称缓存等
        self.station_names = {}
        self.station_names_time = 0

    
    
    # ==================== 3. 封装cache功能 =====================
    def get_person_infos_daytype_with_cache(
            self,
            person_name_filters: Union[List[str], str, None] = None,
            department_filters: Union[List[str], str, None] = None,
            classtype_filters: Union[List[str], str, None] = None,
            worktype_filters: Optional[Dict[str, tuple]] = None,
            duty_filters: Union[List[str], str, None] = None,
            electricity_filters: Optional[Dict[str, tuple]] = None,
            station_filters: Optional[Dict[str, tuple]] = None,
            area_filters: Optional[Dict[str, tuple]] = None,
            in_places_filters: Optional[Dict[str, tuple]] = None,
            out_places_filters: Optional[Dict[str, tuple]] = None,
            start_date: Union[str, datetime, None] = None,
            end_date: Union[str, datetime, None] = None,
            now_or_today: Union[str, None] = None,
    ) -> Dict:
        """
        带细粒度原子级缓存的人员系统数据分析。
        返回值统一为每日数据模式，每天一个键，所有数据点作为子键。
        当天的不管存没存都要重新查。
        """
        
        
    def get_car_infos_daytype_with_cache(
            self,
            car_name_filters: Union[List[str], str, None] = None,
            department_filters: Union[List[str], str, None] = None,
            electricity_filters: Optional[Dict[str, tuple]] = None,
            station_filters: Optional[Dict[str, tuple]] = None,
            area_filters: Optional[Dict[str, tuple]] = None,
            in_places_filters: Optional[Dict[str, tuple]] = None,
            out_places_filters: Optional[Dict[str, tuple]] = None,
            start_date: Union[str, datetime, None] = None,
            end_date: Union[str, datetime, None] = None,
            now_or_today: Union[str, None] = None,
    ) -> Dict:
        """
        带细粒度原子级缓存的车辆系统数据分析。
        返回值统一为每日数据模式，每天一个键，所有数据点作为子键。
        当天的不管存没存都要重新查。
        """
        
```