#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文件名:	tongfeng_tools.py
作者:	shihy
创建日期:	2026-05-06
描述:	通风工具类
"""
import yaml

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文件名:	person_tools.py
作者:	shihy
创建日期:	2026-04-22
描述:	依赖 ClickHouse 实时/历史数据与接口服务，支持多维过滤与分析，适用于 MCP 对接的人员定位服务场景。
"""

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
import traceback
import time
from datetime import datetime
from typing import List, Dict, Optional
import sys
import os
from pprint import pprint
from collections import defaultdict
import copy

from fuzzywuzzy import fuzz

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))



from utils.yafeng_utils_optimized import (
    YafengService
)

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# 配置日志，日志存储在文件中
from logging.handlers import RotatingFileHandler

LOG_DIR = 'logs'
LOG_FILE = os.path.join(LOG_DIR, 'mine_yafeng_service.log')

# 确保日志目录存在
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=50*1024*1024,  # 50MB
    backupCount=5,
    encoding='utf-8'
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(formatter)

# 控制台日志处理器
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)  # 终端日志格式与文件一致

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# 移除已有的 RotatingFileHandler 或 StreamHandler，防止重复
root_logger.handlers = []
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger("MineYafengService")

def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"Type {type(obj)} not serializable")

class YafengMCPService:
    def __init__(
            self,
            mcp: FastMCP,
            host: str,
            port: int,
            user: str,
            password: str,
            database: str,
    ):
        self.mcp = mcp
        self.db_config = {
            "host": host,
            "port": port,
            "username": user,
            "password": password,
            "database": database,
            "secure": True,
            "verify": False,
            "connect_timeout": 10,
            'autogenerate_session_id':False
        }
        
        
        try:
            self.client = clickhouse_connect.get_client(**self.db_config)
            logger.info("Successfully connected to ClickHouse.")
        except Exception as e:
            logger.error(f"Failed to connect to ClickHouse: {e}")
            raise
        self.service = YafengService(self.client,logger)
        self.station_names = {}

        # pprint(self._fetch_car_realtime_api())
        self.fan_monitor_tags = None
        self._register_resources()
        self._register_prompts()
        self._register_tools()

    @staticmethod
    def _match_value_filter(v, op, threshold):
        if v is None:
            return False
        try:
            v = float(v)
            if op == ">":
                return v > threshold
            elif op == ">=":
                return v >= threshold
            elif op == "<":
                return v < threshold
            elif op == "<=":
                return v <= threshold
            elif op == "=":
                return v == threshold
            return False
        except:
            return False
        
  

    # ==================== 1. Resource: 静态文档与数据字典 ====================
    def _register_resources(self):
        @self.mcp.resource("docs://personnel/data-dictionary")
        def get_data_dictionary() -> str:
            """
            获取通风系统的数据字典和字段说明。
            """
            return """
            # 矿井人员定位系统数据字典

        fan_system:

        1号空压机:
            信号:
            YF_KONG_YA_JI_1_ZHUANG_TAI: 1号空压机工作状态反馈信号
            YF_KONG_YA_JI_1_YUN_XING_WEI: 1号空压机处于运行状态信号
            YF_KONG_YA_JI_1_JIA_ZAI_WEI: 1号空压机处于加载运行状态信号
            YF_KONG_YA_JI_1_GU_ZHANG_WEI: 1号空压机检测到故障状态信号
            YF_KONG_YA_JI_1_TING_ZHI_WEI: 1号空压机处于停止状态信号
            YF_YI_HAO_ZHOU_WEN_BAO_JING: 1号空压机轴温过高报警信号
            YF_YI_HAO_DIAN_JI_WEN_DU_YU_JING: 1号空压机电机温度预警信号
            状态:
            YF_YI_HAO_YI_WEI_WEI_BEI_YONG: 1号空压机已处于备用状态
            YF_YI_HAO_YI_TOU_RU: 1号空压机已投入运行状态
            YF_YI_HAO_YI_BEI_YONG: 1号空压机已设置为备用状态
            YF_YI_HAO_YI_JIAN_XIU: 1号空压机已设置为检修状态
            指令:
            YF_KONG_1_BAO_YANG_SHI_JIAN_QING_LING: 1号空压机保养运行时间清零指令
            YF_KONG_YA_JI_1_QI_DONG: 1号空压机执行启动操作指令
            YF_KONG_YA_JI_1_TING_ZHI: 1号空压机执行停止操作指令
            YF_TOU_RU_1_HAO: 1号空压机投入运行控制指令
            YF_BEI_YONG_1_HAO: 设定1号空压机为备用状态指令
            YF_JIAN_XIU_1_HAO: 设定1号空压机为检修状态指令
            YF_XUAN_ZE_YI_HAO_WEI_BEI_YONG: 设定1号空压机为备用机组
            温度监测值:
            YF_GUAN_DAO_1_WEN_DU: 1号空压机管道温度实时监测值
            YF_FENG_BAO_1_WEN_DU: 1号空压机风包温度实时监测值
            压力监测值:
            YF_GUAN_DAO_1_YA_LI: 1号空压机管道压力实时监测值
            YF_FENG_BAO_1_YA_LI: 1号空压机风包压力实时监测值
            YF_PAI_QI_YA_LI_1: 1号空压机排气压力实时监测值
            YF_KONG_YA_JI_1_YOU_YA: 1号空压机润滑油压力实时值
            功率监测值:
            YF_KONG_YA_JI_1_YOU_GONG: 1号空压机有功功率实时监测值
            YF_KONG_YA_JI_1_WU_GONG: 1号空压机无功功率实时监测值
            YF_JI_TOU_1_WEN_DU: 1号空压机机头温度实时监测值
            时间戳:
            YF_TIMESTAMP: 系统时间戳
        2号空压机:
            信号:
            YF_KONG_YA_JI_2_ZHUANG_TAI: 2号空压机工作状态反馈信号
            YF_KONG_YA_JI_2_YUN_XING_WEI: 2号空压机处于运行状态信号
            YF_KONG_YA_JI_2_JIA_ZAI_WEI: 2号空压机处于加载运行状态信号
            YF_KONG_YA_JI_2_GU_ZHANG_WEI: 2号空压机检测到故障状态信号
            YF_KONG_YA_JI_2_TING_ZHI_WEI: 2号空压机处于停止状态信号
            YF_ER_HAO_ZHOU_WEN_BAO_JING: 2号空压机轴温过高报警信号
            YF_ER_HAO_DIAN_JI_WEN_DU_YU_JING: 2号空压机电机温度预警信号
            状态:
            YF_ER_HAO_YI_WEI_WEI_BEI_YONG: 2号空压机已处于备用状态
            YF_ER_HAO_YI_TOU_RU: 2号空压机已投入运行状态
            YF_ER_HAO_YI_BEI_YONG: 2号空压机已设置为备用状态
            YF_ER_HAO_YI_JIAN_XIU: 2号空压机已设置为检修状态
            指令:
            YF_KONG_2_BAO_YANG_SHI_JIAN_QING_LING: 2号空压机保养运行时间清零指令
            YF_KONG_YA_JI_2_QI_DONG: 2号空压机执行启动操作指令
            YF_KONG_YA_JI_2_TING_ZHI: 2号空压机执行停止操作指令
            YF_TOU_RU_2_HAO: 2号空压机投入运行控制指令
            YF_BEI_YONG_2_HAO: 设定2号空压机为备用状态指令
            YF_JIAN_XIU_2_HAO: 设定2号空压机为检修状态指令
            YF_XUAN_ZE_ER_HAO_WEI_BEI_YONG: 设定2号空压机为备用机组
            温度监测值:
            YF_GUAN_DAO_2_WEN_DU: 2号空压机管道温度实时监测值
            YF_FENG_BAO_2_WEN_DU: 2号空压机风包温度实时监测值
            压力监测值:
            YF_GUAN_DAO_2_YA_LI: 2号空压机管道压力实时监测值
            YF_FENG_BAO_2_YA_LI: 2号空压机风包压力实时监测值
            YF_PAI_QI_YA_LI_2: 2号空压机排气压力实时监测值
            YF_KONG_YA_JI_2_YOU_YA: 2号空压机润滑油压力实时值
            功率监测值:
            YF_KONG_YA_JI_2_YOU_GONG: 2号空压机有功功率实时监测值
            YF_KONG_YA_JI_2_WU_GONG: 2号空压机无功功率实时监测值
            YF_ER_HAO_GONG_LV_YIN_SHU: 2号回路功率因数实时监测值
            YF_JI_TOU_2_WEN_DU: 2号空压机机头温度实时监测值
            时间戳:
            YF_TIMESTAMP: 系统时间戳
        3号空压机:
            信号:
            YF_KONG_YA_JI_3_ZHUANG_TAI: 3号空压机工作状态反馈信号
            YF_KONG_YA_JI_3_TING_ZHI_ZHONG_HMI: 3号空压机处于停止过程状态信号
            YF_KONG_YA_JI_3_YUN_XING_ZHONG_HMI: 3号空压机处于运行过程状态信号
            YF_KONG_YA_JI_3_GU_ZHANG_ZHONG_HMI: 3号空压机处于故障报警状态信号
            YF_SAN_HAO_ZHOU_WEN_BAO_JING: 3号空压机轴温过高报警信号
            YF_SAN_HAO_DIAN_JI_WEN_DU_YU_JING: 3号空压机电机温度预警信号
            状态:
            YF_SAN_HAO_YI_WEI_WEI_BEI_YONG: 3号空压机已处于备用状态
            YF_SAN_HAO_YI_TOU_RU: 3号空压机已投入运行状态
            YF_SAN_HAO_YI_BEI_YONG: 3号空压机已设置为备用状态
            YF_SAN_HAO_YI_JIAN_XIU: 3号空压机已设置为检修状态
            指令:
            YF_KONG_YA_JI_3_FU_WEI_HMI: 人机界面下发3号空压机故障复位指令
            YF_KONG_YA_JI_3_QI_DONG: 3号空压机执行启动操作指令
            YF_KONG_YA_JI_3_TING_ZHI: 3号空压机执行停止操作指令
            YF_TOU_RU_3_HAO: 3号空压机投入运行控制指令
            YF_BEI_YONG_3_HAO: 设定3号空压机为备用状态指令
            YF_JIAN_XIU_3_HAO: 设定3号空压机为检修状态指令
            YF_KONG_YA_JI_3_QI_DONG_HMI: 人机界面下发3号空压机启动指令
            YF_KONG_YA_JI_3_TING_ZHI_HMI: 人机界面下发3号空压机停止指令
            YF_XUAN_ZE_SAN_HAO_WEI_BEI_YONG: 设定3号空压机为备用机组
            温度监测值:
            YF_GUAN_DAO_3_WEN_DU: 3号空压机管道温度实时监测值
            YF_FENG_BAO_3_WEN_DU: 3号空压机风包温度实时监测值
            YF_JI_TOU_3_WEN_DU: 3号空压机机头温度实时监测值
            压力监测值:
            YF_GUAN_DAO_3_YA_LI: 3号空压机管道压力实时监测值
            YF_FENG_BAO_3_YA_LI: 3号空压机风包压力实时监测值
            YF_KONG_YA_JI_3_DIAN_LIU: 3号空压机工作电流实时监测值
            YF_KONG_YA_JI_3_PAI_QI_YA_LI: 3号空压机排气压力实时监测值
            YF_KONG_YA_JI_3_YOU_YA: 3号空压机润滑油压力实时值
            
            时间戳:
            YF_TIMESTAMP: 系统时间戳

        断路器系统:
            信号:
            YF_DUAN_LU_QI_1_ZHUANG_TAI: 断路器1工作状态反馈信号
            YF_DUAN_LU_QI_2_ZHUANG_TAI: 断路器2工作状态反馈信号
            YF_DUAN_LU_QI_3_ZHUANG_TAI: 断路器3工作状态反馈信号
            指令:
            YF_DUAN_LU_QI_1_FEN_ZHA: 断路器1执行分闸操作指令
            YF_DUAN_LU_QI_1_HE_ZHA: 断路器1执行合闸操作指令
            YF_DUAN_LU_QI_2_FEN_ZHA: 断路器2执行分闸操作指令
            YF_DUAN_LU_QI_2_HE_ZHA: 断路器2执行合闸操作指令
            YF_MU_LIAN_DUAN_LU_QI_HE_ZHA: 母联断路器执行合闸操作指令
            YF_MU_LIAN_DUAN_LU_QI_FEN_ZHA: 母联断路器执行分闸操作指令
            监测值:
            
            YF_DUAN_LU_QI_DIAN_LIU_1: 断路器1回路电流实时监测值
            YF_DUAN_LU_QI_DIAN_LIU_2: 断路器2回路电流实时监测值
            YF_DUAN_LU_QI_GONG_LV_1: 断路器1回路有功功率实时监测值
            YF_DUAN_LU_QI_GONG_LV_2: 断路器2回路有功功率实时监测值
            YF_DUAN_LU_QI_MU_LIAN_DIAN_LIU: 母联断路器回路电流实时监测值
            YF_DUAN_LU_QI_MU_LIAN_GONG_LV: 母联断路器回路有功功率监测值
            YF_MU_LIAN_WU_GONG: 母联回路无功功率实时监测值
            YF_MU_LIAN_GONG_LV_YIN_SHU: 母联回路功率因数实时监测值
            时间戳:
            YF_TIMESTAMP: 系统时间戳
            

        系统级:
            指令:
            YF_ZI_DONG_QI_DONG: 系统执行自动启动操作指令
            YF_ZI_DONG_TING_ZHI: 系统执行自动停止操作指令
            YF_QUAN_ZU_TOU_RU_MO_SHI: 压风系统所有机组投入运行模式
            YF_QUAN_ZU_YI_TOU_RU: 压风系统全部机组已投入运行
            模式:
            YF_ZI_DONG_MO_SHI: 系统处于自动控制运行模式
            YF_QUAN_ZU_JIE_CHU_MO_SHI: 压风系统所有机组解除运行模式
            信号:
            YF_BAO_YANG_TI_XING: 设备保养周期提醒信号
            YF_BAO_YANG_TI_XING_1: 1号设备保养周期提醒信号
            设定值:
            YF_YA_LI_XIA_XIAN_SHE_DING: 系统供气压力下限阈值设定值
            YF_YA_LI_SHANG_XIAN_SHE_DING: 系统供气压力上限阈值设定值
            YF_DIAN_JI_ZHOU_WEN_BAO_JING_SHE_DING: 电机轴温过高报警阈值设定值
            YF_WEN_DU_YU_JING_SHE_DING: 设备温度预警阈值设定值
            监测值:
            YF_ZONG_GUAN_YA_LI: 压风系统总管压力实时监测值
            YF_ZONG_GUAN_LIU_LIANG: 压风系统总管流量实时监测值
            YF_PIN_LV: 空压机运行频率实时监测值
            YF_GONG_LV_YIN_SHU: 系统回路功率因数实时监测值
            YF_YI_HAO_GONG_LV_YIN_SHU: 1号回路功率因数实时监测值

            时间戳:
            YF_TIMESTAMP: 系统时间戳

        电机系统:
            监测值:
            YF_DIAN_JI_1_QIAN_ZHOU_WEN_DU: 1号电机前轴温度实时监测值
            YF_DIAN_JI_1_HOU_ZHOU_WEN_DU: 1号电机后轴温度实时监测值
            YF_DING_ZI_1_WEN_DU: 1号电机定子温度实时监测值
            YF_DIAN_JI_2_QIAN_ZHOU_WEN_DU: 2号电机前轴温度实时监测值
            YF_DIAN_JI_2_HOU_ZHOU_WEN_DU: 2号电机后轴温度实时监测值
            YF_DING_ZI_2_WEN_DU: 2号电机定子温度实时监测值
            YF_DIAN_JI_3_QIAN_ZHOU_WEN_DU: 3号电机前轴温度实时监测值
            YF_DIAN_JI_3_HOU_ZHOU_WEN_DU: 3号电机后轴温度实时监测值
            YF_DING_ZI_3_WEN_DU: 3号电机定子温度实时监测值


        机房配电室操作室环境烟雾温度系统:
            信号:
            YF_PEI_DIAN_SHI_YAN_WU_BAO_JING: 配电室检测到烟雾报警信号
            YF_CAO_ZUO_SHI_YAN_WU_BAO_JING: 操作室检测到烟雾报警信号
            YF_JI_FANG_YAN_WU_BAO_JING: 机房检测到烟雾报警信号
            YF_JI_FANG_WEN_DU_BAO_JING: 机房环境温度过高报警信号
            YF_JI_FANG_YAN_WU_YU_JING: 机房烟雾浓度达到预警阈值信号
            YF_PEI_DIAN_SHI_YAN_WU_YU_JING: 配电室烟雾浓度达到预警阈值信号
            YF_CAO_ZUO_SHI_YAN_WU_YU_JING: 操作室烟雾浓度达到预警阈值信号
            YF_JI_FANG_WEN_DU_YU_JING: 机房温度达到预警阈值信号
            设定值:
            YF_JI_FANG_WEN_DU_BAO_JING_SHE_DING: 机房环境温度报警阈值设定值
            YF_JI_FANG_WEN_DU_YU_JING_SHE_DING: 机房温度预警阈值设定值
            YF_YAN_WU_BAO_JING_SHE_DING: 环境烟雾报警阈值设定值
            YF_YAN_WU_YU_JING_SHE_DING: 环境烟雾预警阈值设定值
            监测值:
            YF_JI_FANG_YAN_WU: 机房内烟雾浓度实时监测值
            YF_PEI_DIAN_SHI_YAN_WU: 配电室内烟雾浓度实时监测值
            YF_CAO_ZUO_SHI_YAN_WU: 操作室内烟雾浓度实时监测值
            YF_JI_FANG_SHI_DU: 机房环境湿度实时监测值
            YF_JI_FANG_WEN_DU: 机房环境温度实时监测值
            时间戳:
            YF_TIMESTAMP: 系统时间戳

        振动系统:
            信号:
            YF_ZHEN_DONG_1_BAO_JING: 1号振动监测点超限报警信号
            YF_ZHEN_DONG_2_BAO_JING: 2号振动监测点超限报警信号
            YF_ZHEN_DONG_3_BAO_JING: 3号振动监测点超限报警信号
            YF_ZHEN_DONG_1_YU_JING: 1号振动监测点达到预警阈值信号
            YF_ZHEN_DONG_2_YU_JING: 2号振动监测点达到预警阈值信号
            YF_ZHEN_DONG_3_YU_JING: 3号振动监测点达到预警阈值信号
            设定值:
            YF_ZHEN_DONG_BAO_JING_SHE_DING: 设备振动超限报警阈值设定值
            YF_ZHEN_DONG_YU_JING_SHE_DING: 设备振动预警阈值设定值
            监测值:
            YF_ZHEN_DONG_1: 1号振动传感器实时监测值
            YF_ZHEN_DONG_2: 2号振动传感器实时监测值
            YF_ZHEN_DONG_3: 3号振动传感器实时监测值
            时间戳:
            YF_TIMESTAMP: 系统时间戳

        逻辑控制系统:
            停止逻辑:
            YF_SAN_HAO_WEI_BEI_YONG_TING_1: 3号为备用时机组1停止逻辑
            YF_ER_HAO_WEI_BEI_YONG_TING_1: 2号为备用时机组1停止逻辑
            YF_SAN_HAO_WEI_BEI_YONG_TING_2: 3号为备用时机组2停止逻辑
            YF_YI_HAO_WEI_BEI_YONG_TING_2: 1号为备用时机组2停止逻辑
            YF_YI_HAO_WEI_BEI_YONG_TING_3: 1号为备用时机组3停止逻辑
            YF_ER_HAO_WEI_BEI_YONG_TING_3: 2号为备用时机组3停止逻辑
            控制逻辑:
            YF_BEI_YONG_WEI_3_TING_2_YI_XUAN: 已选择备用3停2控制逻辑
            YF_BEI_YONG_WEI_3_TING_1_YI_XUAN: 已选择备用3停1控制逻辑
            YF_BEI_YONG_WEI_1_TING_3_YI_XUAN: 已选择备用1停3控制逻辑
            YF_BEI_YONG_WEI_1_TING_2_YI_XUAN: 已选择备用1停2控制逻辑
            YF_BEI_YONG_WEI_2_TING_1_YI_XUAN: 已选择备用2停1控制逻辑
            YF_BEI_YONG_WEI_2_TING_3_YI_XUAN: 已选择备用2停3控制逻辑  
            时间戳:
            YF_TIMESTAMP: 系统时间戳

            """

    # ==================== 2. Prompt: 全局行为准则 ====================
    def _register_prompts(self):

        @self.mcp.prompt()
        def analysis_guide() -> str:
            """
            获取主要设备分析的专业操作指南。模型在处理用户请求前应默认加载此提示词。
            本指南根据系统内已注册的各类分析工具，说明其最佳使用场景，指导大模型自动选择与合理组合工具，完善多类型查询与数据解释。

            """
            return """
            你是矿井安全生产调度与设备分析专家，熟悉井下工业系统的运行机制。使用本系统时，请依据如下准则和工具描述，科学推理、自动选择最合适的工具并可灵活组合，以获得准确结果：

            - 所有数值回复，需带清晰单位，如：
                - 温度：℃
                - 湿度：%
                - 振动：mm/s
                - 电流、电压：A，V
                - 功率：kW
                - 流量：m³/hh
                - 压力：Pa

            【1. 获取系统基准时间】
            - 使用 `get_system_time()` 工具获取服务器当前时间（格式：{"current_time": "YYYY-MM-DD HH:MM:SS", "weekday": "Wednesday"}）。所有历史、轨迹、对比等查询的时间范围建议以该结果为准。

            【2. 查询空压机主要参数及历史】
            - 使用 `query_kongyaji_records(choose, start_time, end_time, subgroup_filters)` 查询1、2、3号或全部空压机在指定时间段（默认今日0时至当前）的信号、指令、状态、监测值等主要参数。
                - choose: "1"|"2"|"3"|"all"（默认全部）。
                - subgroup_filters 可为 "信号"、"指令"、"状态"、"温度监测值"、"压力监测值"、"功率监测值"（可多选或不指定全部）。
            - 返回内容包含分组字段英文、中文说明、统计等。跨度超一天将只返回统计/分档TOP等摘要。

            【3. 查询其他系统级（高压柜/阀门/环境等）主要参数】
            - 调用 `query_others_system_records(choose, start_time, end_time)`，参数：
                - choose: "system"（系统整体）、"gaoyagui"（高压柜）、"fameng"（阀门）、"jinxiangui"（进线柜）、"fengmen"（风门）、"mulian"（母联柜）、"zhendong"（振动）、"huanjing"（环境）等，也可为字符串数组，"all"则查询全部支持系统。
                - 时间默认今日0时至当前，格式为"YYYY-MM-DD HH:MM:SS"。
            - 返回分组及字段带详细注释说明。

            【4. 查询各类设备级设定/记录】
            - 使用 `query_shebei_system_records(start_time, end_time)` 获取设备设定、报警限值等。支持跨天统计与较大区段等功能。

            【5. 查询功率能耗】
            - 使用 `query_power_energy_records(start_time, end_time)` 查询空压机及风机断路器等设备在指定时间段内的有功功率、能耗信息。
                - 仅支持指定 Tag（如 YF_KONG_YA_JI_1_YOU_GONG、YF_KONG_YA_JI_2_YOU_GONG、YF_DUAN_LU_QI_GONG_LV_1、YF_DUAN_LU_QI_GONG_LV_2、YF_DUAN_LU_QI_MU_LIAN_GONG_LV）对应设备。
                - 跨天仅返回统计/聚合top，详查需缩小时段。

            【6. 获取系统支持字段与分组】
            - 使用 `get_supported_fields()` 工具，直接返回 tongfeng_system.yaml（JSON结构），列出全部支持字段、分组、分组注释与字段中文说明。用于界面字段配置、自定义查询、前端说明等。

            # 重要注意事项：

            - 未指定时间时，所有查询默认当天00:00:00至当前；
            - 查询跨度大于一天，结果将以统计、归档、TOP为主，需要明细须缩短时间区间；
            - 各系统、设备、分组和字段命名、注释严格依据 yaml 配置，务必保持准确；
            - 如遇跨分组、跨系统需求，可灵活组合多工具，多步依次查询；
            - 合理拆解多维请求，尽量返回结构化、易于理解的多层JSON数据，并解释分析维度。

            遇到模糊需求或有疑问时，优先选择最相关工具。如需时间、字段基准先查询基础信息，再进行后续分析。
            """
     

    def _register_tools(self):

        @self.mcp.tool()
        def get_system_time() -> str:
            """
            功能描述: 获取服务器当前的系统时间。由于历史查询和轨迹查询依赖于准确的时间范围，该工具可作为大模型计算“昨天”、“上周”或“三小时前”等相对时间的基准参考。
            输入参数: 无。
            返回描述: 包含当前日期时间（格式：YYYY-MM-DD HH:MM:SS）及星期的 JSON 字符串。例如: {
                "current_time": "2024-06-05 17:23:42",
                "weekday": "Wednesday"
            }
            """
            now = datetime.now()
            return json.dumps(
                {
                    "current_time": now.strftime("%Y-%m-%d %H:%M:%S"),
                    "weekday": now.strftime("%A"),
                },
                ensure_ascii=False,
            )


        @self.mcp.tool()
        def query_kongyaji_records(
                choose: Optional[Union[str, int]] = "all",  # "1"（仅1号空压机）, "2"（仅2号空压机）, "3"（仅3号空压机）, "all"（全部空压机，默认）。可以为字符串或数字
                start_time: Optional[str] = None,  # 起始时间，YYYY-MM-DD HH:MM:SS"，可选
                end_time: Optional[str] = None,    # 结束时间，"YYYY-MM-DD HH:MM:SS"，可选
                subgroup_filters: Union[List[str], str, None] = None,  # 分组筛选（如"信号"、"指令"、"状态"、"监测值"），可选
        ) -> str:
            """
            查询1号、2号、3号或全部空压机在指定时间段内的主要参数（信号、指令、状态、监测值）。
            字段严格遵循 yafeng_system.yaml 配置，分组及字段全为大写并带编号，返回结构包含英文名及中文注释。

            参数说明：
                - choose: 空压机选择。可选值："1"（1号空压机）、"2"（2号空压机）、"3"（3号空压机）、"all"（全部，默认）。
                    - 其中“1号空压机”所有字段均以 YF_KONG_YA_JI_1_ 开头，“2号空压机”以 YF_KONG_YA_JI_2_，"3号空压机"以 YF_KONG_YA_JI_3_开头。
                - start_time: 起始时间（格式："YYYY-MM-DD HH:MM:SS"），可选。默认当天00:00:00。
                - end_time: 结束时间（同上格式），可选。默认服务当前时间。
                - subgroup_filters: 可以不传，不传返回所有数据。仅能从【"信号"、"指令"、"运行状态"、"温度监测值"、"压力监测值"、"功率监测值"】中选择一个或多个，表示只返回指定分组下的字段。

            1号、2号、3号空压机字段分组及说明：
            - 信号：用于反映空压机实时运行/停止/报警等工作状态，主要包括：
                - 工作状态反馈                
                - 加载/停止/运行/故障状态
                - 报警与预警
            - 运行状态：表示空压机当前所处的逻辑运行管理状态，包括：
                - 已处于/已设置为备用、检修或已投入运行等
            - 指令：用于下发操作指令或远程控制，主要包括：
                - 启动/停止/复位等操作指令
                - 切换为备用/检修等设置指令
                - 保养、清零等维护相关指令
            - 监测值：用于实时监控空压机运行参数，主要包括：
                - 管道/风包/排气温度与压力实时值
                - 电流、电压、功率、功率因数等实时运行电气参数
                - 机头温度、轴温、油压等运行安全参数
    
            返回值说明：
                - 查询失败时：
                    {
                        "error": "查询失败",
                        "message": "错误说明"
                    }
                - 未查到记录或无数据：
                    {
                        "message": "未找到指定空压机在给定时间范围内的记录"
                    }
                - 查询成功时，返回多层结构化JSON，各分组字段下有字段英文名、中文说明及分析内容。如果时间跨度超过一天（跨天查询），将提供统计归纳与top3信息，结构包含每日分组与分段统计等。

            特别提示：
                - 当查询时间范围大于一天时，仅返回统计信息与结果归档（如"重要变化数量"/"top3"等）；
                - 若需获更精确/详细的数据分析，请指定更短的时间段（建议不大于1天）。
            """
            # INSERT_YOUR_CODE
            logger.info(
                f"query_kongyaji_records called: choose={choose}, start_time={start_time}, end_time={end_time}, subgroup_filters={subgroup_filters}"
            )
     
            # subgroup_filters 处理，只允许 "信号" "指令" "状态" "监测值"
            allowed_subgroups = ["信号", "指令", "运行状态", "温度监测值", "压力电流监测值", "功率监测值"]
            if subgroup_filters is not None:
                if isinstance(subgroup_filters, str):
                    subgroups = [subgroup_filters]
                elif isinstance(subgroup_filters, list):
                    subgroups = subgroup_filters
                else:
                    subgroups = []
                subgroups = [s for s in subgroups if s in allowed_subgroups]
                if not subgroups:
                    subgroups = None  # 清空无效筛选，返回所有
            else:
                subgroups = None

            system_map = {
                "1": ["1号空压机"],
                "2": ["2号空压机"],
                "3": ["3号空压机"],
                "all": ["1号空压机", "2号空压机", "3号空压机"],
                None: ["1号空压机", "2号空压机", "3号空压机"],
            }
            system_name_filters = system_map.get(str(choose), ["1号空压机", "2号空压机", "3号空压机"])

            try:
                if start_time is None:
                    from datetime import datetime
                    today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
                    start_date = today.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    start_date = start_time
                if end_time is None:
                    from datetime import datetime
                    end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                else:
                    end_date = end_time

                # 调用分析主方法
                records = self.service.print_yafeng_today_with_cache(
                    system_name_filters=system_name_filters,
                    start_date=start_date,
                    end_date=end_date,
                    subgroup_filters=subgroups,
                )

                # 判断是否跨天（跨天结构字段带"每日数据"）
                use_serializer = False
                for system in records.values():
                    for subgroup in system.values():
                        for info in subgroup.values():
                            if isinstance(info, dict) and "每日数据" in info:
                                use_serializer = True
                                break

                if use_serializer:
                    records = self.json_serializer(records)
                    result = {
                        "result": records,
                        "tip": "当前查询为跨天统计结果，如需更精确分析，请提供更详细的时间段和分析角度，建议范围为1天内，仅支持分组[\"信号\",\"指令\",\"状态\",\"监测值\"]。"
                    }
                    result_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
                    logger.info(f"query_kongyaji_records 返回结果长度1: {len(result_json)} use_serializer {use_serializer}")
                    if len(result_json) > 100000:
                        filtered_records = self.json_serializer(records)
                        result = {
                            "result": filtered_records,
                            "tip": "结果过大，仅保留主要监测角度。如需其他监测数据，请提供更详细的查询条件，仅支持分组[\"信号\",\"指令\",\"状态\",\"监测值\"]。"
                        }
                        result_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
                    logger.info(f"query_kongyaji_records 返回结果长度2: {len(result_json)} use_serializer {use_serializer}")
                    return result_json
                else:
                    result_json = json.dumps(records, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
                    if len(result_json) > 100000:
                        records = self.json_serializer(records)
                        result = {
                            "result": records,
                            "tip": "当前查询为当天统计结果，但由于数据过多，如需更精确分析，请提供更详细的时间段和分组，只支持[\"信号\",\"指令\",\"状态\",\"监测值\"]。"
                        }
                        result_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
                    # INSERT_YOUR_CODE
                    logger.info(f"query_kongyaji_records 返回结果长度: {len(result_json)} use_serializer {use_serializer}")
             
                    return result_json

            except Exception as e:
                logger.error(f"query_kongyaji_records 异常: {e}", exc_info=True)
                return json.dumps(
                    {"error": "查询失败", "message": str(e)},
                    ensure_ascii=False
                )

        @self.mcp.tool()
        def query_others_system_records(
                choose: Optional[Union[str, List[str]]] = "振动系统",   # 可为单个字符串或字符串列表，如"断路器"、["断路器", "振动系统"]等，"all"为全部系统，默认
                start_time: Optional[str] = None,   # 起始时间，YYYY-MM-DD HH:MM:SS，可选
                end_time: Optional[str] = None      # 结束时间，YYYY-MM-DD HH:MM:SS，可选
        ) -> str:
            """
            查询断路器系统、振动系统、逻辑控制系统、机房配电室操作室环境烟雾温度系统、电机系统、系统级的记录数据并进行分析。 
            字段严格遵循配置，返回结构包含字段英文名及中文注释。

            参数说明：
                - choose: 系统选择。可选值：
                    * 字符串："断路器系统"、"振动系统"、"逻辑控制系统"、"机房配电室操作室环境烟雾温度系统"、"电机系统"、"系统级"   只能在这六个字段中选择 不能选 all
                    * 字符串数组：如 ["断路器系统", "振动系统"] 表示同时查询多个系统
                - start_time: 起始时间（格式："YYYY-MM-DD HH:MM:SS"），可选，默认当天00:00:00。
                - end_time: 结束时间（格式："YYYY-MM-DD HH:MM:SS"），可选，默认服务当前时间。
                
            字段说明：
                断路器系统记录了：1、2、3号断路器和母联断路器的电流、功率、信号及相关指令。
                振动系统记录了：1、2、3振动监测点超限报警信号、实时监测值以及设备振动超限报警/预警阈值设定值。
                逻辑控制系统记录了：1、2、3号之间的停止逻辑和控制逻辑。
                机房配电室操作室环境烟雾温度系统记录了：配电室、操作室、机房、环境检测的温度、烟雾 报警预警信号/设定值/监测值。
                电机系统记录了：1、2、3号电机的前轴温度、后轴温度、定子温度实时监测值。
                系统级记录了：自动启动、自动停止、运行模式、保养提醒、压力上下限设定值、总管压力流量、频率、电机轴温报警阈值等系统级信号与设定/监测数据。
         
           
            返回值说明：
                - 查询失败时：
                    {
                        "error": "查询失败",
                        "message": "错误说明"
                    }
                - 未查到记录或无数据：
                    {
                        "message": "未找到指定系统在给定时间范围内的记录"
                    }
                - 查询成功时，返回结构化JSON，包括所有查询系统的各分组字段及字段注释。

            特别提示：
                - 当查询时间范围大于一天时，仅返回统计信息与结果归档（如"top3"等）。
                - 若需更详细的数据分析，请指定更短的时间段（建议不大于1天）。
            """
            # INSERT_YOUR_CODE
            logger.info(
                f"query_others_system_records called: choose={choose}, start_time={start_time}, end_time={end_time}"
            )
     
            try:
                # 系统英文名与yaml配置映射
                system_map = {
                    "断路器系统": "断路器系统",
                    "振动系统": "振动系统",
                    "逻辑控制系统": "逻辑控制系统",
                    "机房配电室操作室环境烟雾温度系统": "机房配电室操作室环境烟雾温度系统",
                    "电机系统": "电机系统",
                    "系统级": "系统级",
                }

                # 支持 choose 可以为字符串、列表或元组
                system_names = []
                if choose == "all" or not choose:
                    system_names = list(system_map.values())
                elif isinstance(choose, str):
                    choose_keys = [choose]
                else:  # list or tuple
                    choose_keys = list(choose)

                if not system_names:
                    system_names_tmp = []
                    for key in choose_keys:
                        choose_key = key
                        if choose_key in system_map:
                            system_names_tmp.append(system_map[choose_key])
                        else:
                            return json.dumps({"error": "参数错误", "message": f"不支持的系统类型: {choose_key}"}, ensure_ascii=False)
                    system_names = system_names_tmp

                # 处理时间
                now = datetime.now()
                if not start_time:
                    query_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    query_start = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
                if not end_time:
                    query_end = now
                else:
                    query_end = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

                # 这里假设有接口: self.get_system_records(sys_name, query_start, query_end, summary=multi_day)
                records = self.service.print_yafeng_today_with_cache(
                    system_name_filters=system_names,
                    start_date=query_start, end_date=query_end,
                )
                
                result_json = json.dumps(records, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
                print(f'len_result_json {len(result_json)}')
                if len(result_json) > 100000:
                    records = self.json_serializer(records)
                    
                # 检查是否所有为空
                if all(not v for v in records.values()):
                    return json.dumps({"message": "未找到指定系统在给定时间范围内的记录"}, ensure_ascii=False)

                # 假定 self.json_serializer 适配所有这几类系统结构
                result = {
                    "result": records,
                    "tip": "当前为跨天统计/归档，如需详细数据请仅查一天或指定更短时间段。"
                }
                result_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
                # INSERT_YOUR_CODE
                logger.info(f"query_others_system_records 返回JSON长度: {len(result_json)}，系统名: {system_names}")
           
         

                return result_json

            except Exception as e:
                logger.error(f"query_others_system_records 异常: {e}", exc_info=True)
                return json.dumps(
                    {"error": "查询失败", "message": str(e)},
                    ensure_ascii=False
                )

      
        
        @self.mcp.tool()
        def query_power_energy_records(
                start_time: Optional[str] = None,  # 起始时间，YYYY-MM-DD HH:MM:SS，可选
                end_time: Optional[str] = None  # 结束时间，YYYY-MM-DD HH:MM:SS，可选
        ):
            """
            查功率能耗。只能查询如下设定设备的功率能耗相关数据：
                - 1号空压机有功功率实时监测值 (YF_KONG_YA_JI_1_YOU_GONG)
                - 2号空压机有功功率实时监测值 (YF_KONG_YA_JI_2_YOU_GONG)
                - 断路器1回路有功功率实时监测值 (YF_DUAN_LU_QI_GONG_LV_1)
                - 断路器2回路有功功率实时监测值 (YF_DUAN_LU_QI_GONG_LV_2)
                - 母联断路器回路有功功率监测值 (YF_DUAN_LU_QI_MU_LIAN_GONG_LV)
           
        
            参数说明：
                - start_time: 起始时间（格式："YYYY-MM-DD HH:MM:SS"），可选。默认当天00:00:00。
                - end_time: 结束时间（格式："YYYY-MM-DD HH:MM:SS"），可选。默认服务当前时间。

            返回值说明：
                - 查询失败时：
                    {
                        "error": "查询失败",
                        "message": "错误说明"
                    }
                - 未查到记录或无数据：
                    {
                        "message": "未找到指定功率能耗信息在给定时间范围内的记录"
                    }
                - 查询成功时，返回结构化JSON，包括上面4个设备（key为英文Tag，value为中文说明）功率能耗数据及其相关注释。

            特别提示：
                - 只能查询 tongfeng_utils.py power_keyvalues（即上述四台风机）对应的功率能耗数据。
                - 时间范围大于一天时，返回统计信息与结果归档（如"top3"等）。
                - 如需详细、精细数据请指定查询不大于1天的时间段。
            """
            # INSERT_YOUR_CODE
            logger.info(
                f"query_power_energy_records called: start_time={start_time}, end_time={end_time}"
            )
     
            try:
       
                # 处理时间
                now = datetime.now()
                if not start_time:
                    query_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
                else:
                    query_start = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
                if not end_time:
                    query_end = now
                else:
                    query_end = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

                # 查询数据接口（这里假设 service 层有对应接口，需要你实现）
                records = self.service.calc_gonglv_energy_with_cache(
                    start_date=query_start, end_date=query_end,
                )
                if all(not v for v in records.values()):
                    return json.dumps({"message": "未找到指定功率能耗信息在给定时间范围内的记录"}, ensure_ascii=False)

                result_json = json.dumps(records, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
                if len(result_json) > 100000:
                    records = self.json_serializer(records)
                # 返回序列化数据
                result = {
                    "result": records,
                    "tip": "如需详细数据请仅查一天或指定更短时间段。"
                }
                result_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
                # INSERT_YOUR_CODE
                logger.info(f"query_power_energy_records 返回JSON长度: {len(result_json)}")
         
                return result_json

            except Exception as e:
                logger.error(f"query_power_energy_records 异常: {e}", exc_info=True)
                return json.dumps(
                    {"error": "查询失败", "message": str(e)},
                    ensure_ascii=False
                )
            
        @self.mcp.tool()
        def get_supported_fields():
            """
            功能描述: 获取当前系统全部支持的字段列表及其分组、中文注释说明。可用于自定义查询、字段筛选、前端字段说明展示等场景。
            输入参数: 无
            返回: tongfeng_system.yaml 文件完整结构(JSON格式)，包含1、2、3号空压机、断路器、机房配电室操作室环境烟雾等全部分组、字段及注释等元信息。
            返回示例: {
                "fan_system": {...},
            }
            特别说明: 本接口直接返回对象。
            """
            # INSERT_YOUR_CODE
            logger.info("get_supported_fields called: 返回系统全部支持字段列表及分组说明")
     
            data =  self.service._load_yaml()
            return json.dumps(data, ensure_ascii=False, indent=2)



    #_______________________________辅助函数____________________________________

    def json_serializer(self, records,limit=3):
        # 深度处理 records 结构，使"每日数据"下的各角度（如“温度”等）中的时间字段只保留时分秒
       
        def _remove_date_from_time_string(time_str):
            # 处理如 "2026-05-05 11:47:15" → "11:47:15"
            if isinstance(time_str, str) and len(time_str) >= 8:
                try:
                    # "YYYY-MM-DD HH:MM:SS"
                    if " " in time_str:
                        t = time_str.strip().split(" ")[-1]
                        if len(t) == 8 and t[2] == ":" and t[5] == ":":
                            return t
                    # 仅时间
                    if len(time_str) == 8 and time_str[2] == ":" and time_str[5] == ":":
                        return time_str
                    # "YYYY-MM-DDTHH:MM:SS"
                    if "T" in time_str:
                        t = time_str.strip().split("T")[-1]
                        if len(t) == 8 and t[2] == ":" and t[5] == ":":
                            return t
                except Exception:
                    pass
            return time_str

        def _process_inner_timefields(d):
            # 针对每日详细输入角度，比如最小值、最大值、重要变化等的“时间”字段递归去掉日期
            if not isinstance(d, dict):
                return d
            out = {}
            for k, v in d.items():
                if k in ("最小值", "最大值") and isinstance(v, dict):
                    new_v = v.copy()
                    if isinstance(new_v["时间"], datetime):
                        new_v["时间"] =  new_v["时间"].strftime("%Y-%m-%d %H:%M:%S")
                    if "时间" in new_v and isinstance(new_v["时间"], str):
                        new_v["时间"] = _remove_date_from_time_string(new_v["时间"])
                    out[k] = new_v
                elif k in ("重要变化top3", "稳定阶段top3", "异常值top3") and isinstance(v, list):
                    new_list = []
                    for item in v:
                        if isinstance(item, dict):
                            has_time = any(t in item for t in ("时间", "起始时间", "结束时间"))
                            if has_time:
                                new_item = item.copy()
                                # 针对所有时间相关字段处理
                                for time_key in ("时间", "起始时间", "结束时间"):
                                    if time_key in new_item:
                                        if isinstance(new_item[time_key], datetime):
                                            new_item[time_key] = new_item[time_key].strftime("%Y-%m-%d %H:%M:%S")
                                        if isinstance(new_item[time_key], str):
                                            new_item[time_key] = _remove_date_from_time_string(new_item[time_key])
                                new_list.append(new_item)
                            else:
                                new_list.append(item)
                        else:
                            new_list.append(item)
             
                    out[k] = new_list
                elif k == "时间" and isinstance(v, str):
                    out[k] = _remove_date_from_time_string(v)
                else:
                    out[k] = _process_inner_timefields(v) if isinstance(v, dict) else v
            return out

        def _replace_lists_with_segment_counts(val):
            if isinstance(val, dict):
                new_dict = {}
                for k, v in val.items():
                    # 处理"每日数据"
                    if k == "每日数据" and isinstance(v, dict):
                        daily_dict = {}
                        for day, dayval in v.items():
                            if isinstance(dayval, dict):
                                dval = dayval.copy()
                                # 重要变化
                                if "重要变化" in dval and isinstance(dval["重要变化"], list):
                                    changes = dval["重要变化"]
                                    changes_top3 = sorted(
                                        changes,
                                        key=lambda x: abs(x.get("变化量", 0)),
                                        reverse=True
                                    )[:3]
                                    dval["重要变化数量"] = len(changes)
                                    dval["重要变化top3"] = changes_top3
                                    del dval["重要变化"]
                                # 稳定阶段
                                if "稳定阶段" in dval and isinstance(dval["稳定阶段"], list):
                                    periods = dval["稳定阶段"]
                                    periods_top3 = sorted(
                                        periods,
                                        key=lambda x: abs(x.get("持续秒数", 0)),
                                        reverse=True
                                    )[:3]
                                    dval["稳定阶段数量"] = len(periods)
                                    dval["稳定阶段top3"] = periods_top3
                                    del dval["稳定阶段"]
                                # 异常值
                                if "异常值" in dval and isinstance(dval["异常值"], list):
                                    anomalies = dval["异常值"]
                                    anomalies_top3 = sorted(
                                        anomalies,
                                        key=lambda x: abs(x.get("数值", 0)),
                                        reverse=True
                                    )[:3]
                                    dval["异常值数量"] = len(anomalies)
                                    dval["异常值top3"] = anomalies_top3
                                    del dval["异常值"]
                                # === 新增，递归处理每日角度内部“时间”字段为时分秒 ===
                                dval = _process_inner_timefields(dval)
                                daily_dict[day] = dval
                            else:
                                daily_dict[day] = dayval
                        new_dict[k] = daily_dict
                    # 处理"数据"
                    elif k == "数据" and isinstance(v, dict):
                        dval = v.copy()
                        if "重要变化" in dval and isinstance(dval["重要变化"], list):
                            changes = dval["重要变化"]
                            changes_top3 = sorted(
                                changes,
                                key=lambda x: abs(x.get("变化量", 0)),
                                reverse=True
                            )[:3]
                            dval["重要变化数量"] = len(changes)
                            dval[f"重要变化top{limit}"] = changes_top3
                            del dval["重要变化"]
                        if "稳定阶段" in dval and isinstance(dval["稳定阶段"], list):
                            periods = dval["稳定阶段"]
                            periods_top3 = sorted(
                                periods,
                                key=lambda x: abs(x.get("持续秒数", 0)),
                                reverse=True
                            )[:3]
                            dval["稳定阶段数量"] = len(periods)
                            dval[f"稳定阶段top{limit}"] = periods_top3
                            del dval["稳定阶段"]
                        if "异常值" in dval and isinstance(dval["异常值"], list):
                            anomalies = dval["异常值"]
                            anomalies_top3 = sorted(
                                anomalies,
                                key=lambda x: abs(x.get("数值", 0)),
                                reverse=True
                            )[:3]
                            dval["异常值数量"] = len(anomalies)
                            dval[f"异常值top{limit}"] = anomalies_top3
                            del dval["异常值"]
                        # 递归处理角度内“时间”字段
                        dval = _process_inner_timefields(dval)
                        new_dict[k] = dval
                    else:
                        new_dict[k] = _replace_lists_with_segment_counts(v)
                return new_dict
            elif isinstance(val, list):
                return [_replace_lists_with_segment_counts(x) for x in val]
            else:
                return val

        return _replace_lists_with_segment_counts(records)

import asyncio
import json


async def test_all_tools():
    print("🔥 开始测试 TongfengMCPService 的所有 MCP Tools\n" + "=" * 60)

    try:
        # 1️⃣ get_system_time
        # print("\n1️⃣ 测试 get_system_time")
        # res1 = await mcp_app.call_tool("get_supported_fields")
        # print("get_system_time 返回：")
        # print(res1)

        
        # 测试 query_kongyaji_records
        print("\n🧪 测试 query_kongyaji_records - all 空压机（默认/全部）")
        res_all = await mcp_app.call_tool(
            "query_kongyaji_records", 
            {
                "choose": "all", 
                "start_time": "2026-04-10 00:00:00",
                "end_time": "2026-06-05 00:00:00", 
                "subgroup_filters": None
            }
        )
        print("query_kongyaji_records (全部) 返回：")
        # print(res_all)

        print("\n🧪 测试 query_kongyaji_records - 仅 1号空压机，监测值")
        res_1 = await mcp_app.call_tool(
            "query_kongyaji_records", 
            {
                "choose": "1", 
                "start_time": "2026-04-10 00:00:00",
                "end_time": "2026-06-05 23:59:59", 
                "subgroup_filters": ["监测值"]
            }
        )
        print("query_kongyaji_records (1号空压机，监测值) 返回：")
        # print(res_1)

        print("\n🧪 测试 query_kongyaji_records - 仅 2号空压机，指令")
        res_2 = await mcp_app.call_tool(
            "query_kongyaji_records", 
            {
                "choose": "2", 
                "start_time": "2026-04-01 00:00:00",
                "end_time": "2026-05-16 23:59:59", 
                "subgroup_filters": ["指令"]
            }
        )
        print("query_kongyaji_records (2号空压机，指令) 返回：")
        # print(res_2)

        print("\n🧪 测试 query_kongyaji_records - 仅 3号空压机，状态&信号")
        res_3 = await mcp_app.call_tool(
            "query_kongyaji_records", 
            {
                "choose": "3", 
                "start_time": "2026-04-01 00:00:00",
                "end_time": "2026-05-21 23:59:59", 
                "subgroup_filters": ["状态", "信号"]
            }
        )
        print("query_kongyaji_records (3号空压机，状态+信号) 返回：")
        # print(res_3)
        
        # 这里分别测试6个可选字段，确保都能被 query_others_system_records 正确处理
        test_systems = [
            # "断路器系统",
            # "振动系统",
            # "逻辑控制系统",
            # "机房配电室操作室环境烟雾温度系统",
            # "电机系统",
            "系统级",
        ]
        for system_name in test_systems:
            print(f"\n6️⃣ 测试 query_others_system_records 跨天查询统计 - {system_name}")
            res = await mcp_app.call_tool(
                "query_others_system_records",
                {
                    "choose": [system_name],
                    "start_time": "2026-04-09 00:00:00",
                    "end_time": "2026-05-12 23:59:59",
                }
            )
            print(f"query_others_system_records ({system_name} 跨天 数值+状态统计) 返回：")
            # print(res)
 
        
        

    except Exception as e:
        print(f"\n❌ 测试过程中发生错误: {e}\n{traceback.format_exc()}")
    finally:
        print("\n" + "=" * 60)
        print("🎉 所有 Tool 测试执行完毕！")


# ====================== 执行测试 ======================
if __name__ == "__main__":
    # 注意：请确保 mcp_app 已经在上面被创建
    import json

    mcp_app = FastMCP("MinePersonnelService")

    YafengMCPService(
        mcp=mcp_app,
        host="10.11.22.80",
        port=9120,
        user="nethouse",
        password="CGC%EVXr.ET10Y_N",
        database="PS",
    )

    asyncio.run(test_all_tools())
