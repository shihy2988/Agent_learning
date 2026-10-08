#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文件名:	tongfeng_tools.py
作者:	shihy
创建日期:	2026-05-06
描述:	通风工具类
"""
import yaml


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

from utils.tongfeng_utils_optimized import (
    TongfengService, fan_monitor_tags
)

# 禁用 SSL 警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# 配置日志，日志存储在文件中
from logging.handlers import RotatingFileHandler

LOG_DIR = 'logs'
LOG_FILE = os.path.join(LOG_DIR, 'mine_tongfeng_service.log')

# 确保日志目录存在
if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

# 文件日志处理器
file_handler = RotatingFileHandler(
    LOG_FILE,
    maxBytes=50*1024*1024,  # 50MB
    backupCount=5,
    encoding='utf-8'
)
file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)

# 控制台日志处理器
console_handler = logging.StreamHandler()
console_handler.setFormatter(file_formatter)  # 终端日志格式与文件一致

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)

# 移除已有的 RotatingFileHandler 或 StreamHandler，防止重复
root_logger.handlers = []
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger("MineTongfengService")


def json_serializer(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"Type {type(obj)} not serializable")


def _fmt_num(v):
    """格式化数字用于 markdown 表格"""
    if v is None or v == "":
        return "-"
    try:
        if isinstance(v, float):
            if v != 0 and abs(v) < 0.001:
                return f"{v:.4f}"
            return f"{v:.2f}"
        if isinstance(v, int):
            return str(v)
        return str(v)
    except Exception:
        return str(v)


def _extract_num(v):
    """从 {数值, 时间} 结构中提取数值；非 dict 原样返回"""
    if isinstance(v, dict):
        return v.get("数值")
    return v


def _signal_status_single(sig_data):
    """单日：返回一个信号的状态描述"""
    if not isinstance(sig_data, dict):
        return "正常"
    day_dict = sig_data.get("每日数据", {})
    if not day_dict:
        return "无数据"
    day_data = list(day_dict.values())[0]
    if not isinstance(day_data, dict):
        return "正常"
    if day_data.get("变化", "无变化") == "无变化":
        return "正常"
    cnt = day_data.get("变化次数", 0)
    cur = day_data.get("当前值", "")
    return f"⚠ 有变化（{cnt} 次），当前值 {cur}"


def _signal_status_multi(sig_data):
    """多日：返回一个信号的状态描述"""
    if not isinstance(sig_data, dict):
        return "正常"
    day_dict = sig_data.get("每日数据", {})
    if not day_dict:
        return "无数据"
    total = len(day_dict)
    changed = sum(
        1 for d in day_dict.values()
        if isinstance(d, dict) and d.get("变化", "无变化") != "无变化"
    )
    if changed == 0:
        return f"正常（{total} 天无异常）"
    return f"⚠ {changed}/{total} 天有变化"


def _numeric_cells_single(sig_data):
    """单日：返回 (平均, 中位, 标准差, 最小, 最大)"""
    if not isinstance(sig_data, dict):
        return None
    day_dict = sig_data.get("每日数据", {})
    if not day_dict:
        return None
    day_data = list(day_dict.values())[0]
    if not isinstance(day_data, dict):
        return None
    return (
        _fmt_num(day_data.get("平均值")),
        _fmt_num(day_data.get("中位数")),
        _fmt_num(day_data.get("标准差")),
        _fmt_num(_extract_num(day_data.get("最小值"))),
        _fmt_num(_extract_num(day_data.get("最大值"))),
    )


def _numeric_cells_multi(sig_data):
    """多日：跨天聚合 (平均, 中位, 标准差, 最小, 最大)"""
    if not isinstance(sig_data, dict):
        return None
    day_dict = sig_data.get("每日数据", {})
    if not day_dict:
        return None
    avgs, meds, stds, mins, maxs = [], [], [], [], []
    for d in day_dict.values():
        if not isinstance(d, dict):
            continue
        for lst, key in [(avgs, "平均值"), (meds, "中位数"), (stds, "标准差")]:
            v = d.get(key)
            if isinstance(v, (int, float)):
                lst.append(v)
        mn = _extract_num(d.get("最小值"))
        mx = _extract_num(d.get("最大值"))
        if isinstance(mn, (int, float)):
            mins.append(mn)
        if isinstance(mx, (int, float)):
            maxs.append(mx)
    return (
        _fmt_num(sum(avgs) / len(avgs) if avgs else None),
        _fmt_num(sum(meds) / len(meds) if meds else None),
        _fmt_num(sum(stds) / len(stds) if stds else None),
        _fmt_num(min(mins) if mins else None),
        _fmt_num(max(maxs) if maxs else None),
    )


def _find_report_date(records):
    """从 records 中找出日期或日期范围"""
    days = set()
    fengji = records.get("风机系统", {}) or {}
    for sys_data in fengji.values():
        if not isinstance(sys_data, dict):
            continue
        for sub in ("报警", "状态", "数值"):
            for sig in (sys_data.get(sub, {}) or {}).values():
                if isinstance(sig, dict):
                    days.update((sig.get("每日数据", {}) or {}).keys())
    return sorted(days)


def _build_md_report(records, is_multi_day):
    """把原始记录转为 markdown 文本"""
    md = []

    days = _find_report_date(records)

    # ---------- 标题 ----------
    if is_multi_day:
        md.append("# 通风系统多日报告")
        md.append("")
        if days:
            md.append(f"**统计周期**：{days[0]} ~ {days[-1]}")
            md.append(f"**统计天数**：{len(days)} 天")
        md.append("")
    else:
        md.append("# 通风系统日报")
        md.append("")
        if days:
            md.append(f"**日期**：{days[0]}")
        md.append("")

    md.append("---")
    md.append("")

    # ---------- 一、风机系统 ----------
    md.append("## 一、风机系统")
    md.append("")
    fengji = records.get("风机系统", {}) or {}
    for sys_name in ("一号风机系统", "二号风机系统"):
        sys_data = fengji.get(sys_name, {})
        if not sys_data:
            continue
        md.append(f"### {sys_name}")
        md.append("")

        # 报警
        alerts = sys_data.get("报警", {}) or {}
        if alerts:
            md.append("**报警信号**")
            md.append("")
            for sig_name, sig in alerts.items():
                status = _signal_status_multi(sig) if is_multi_day else _signal_status_single(sig)
                md.append(f"- {sig_name}：{status}")
            md.append("")

        # 状态
        statuses = sys_data.get("状态", {}) or {}
        if statuses:
            md.append("**状态信号**")
            md.append("")
            for sig_name, sig in statuses.items():
                status = _signal_status_multi(sig) if is_multi_day else _signal_status_single(sig)
                md.append(f"- {sig_name}：{status}")
            md.append("")

        # 数值
        numerics = sys_data.get("数值", {}) or {}
        if numerics:
            md.append("**数值监测**")
            md.append("")
            md.append("| 监测项 | 平均值 | 中位数 | 标准差 | 最小值 | 最大值 |")
            md.append("|:------|:------|:------|:------|:------|:------|")
            for sig_name, sig in numerics.items():
                cells = _numeric_cells_multi(sig) if is_multi_day else _numeric_cells_single(sig)
                if cells:
                    md.append(f"| {sig_name} | {cells[0]} | {cells[1]} | {cells[2]} | {cells[3]} | {cells[4]} |")
            md.append("")

    # ---------- 二、功率能耗 ----------
    md.append("## 二、功率能耗")
    md.append("")
    power = records.get("功率能耗", {}) or {}
    if isinstance(power, dict) and power:
        dev_avg = {}
        dev_energy = {}
        for day_data in power.values():
            if not isinstance(day_data, dict):
                continue
            avg_p = day_data.get("总平均功率_kw", {}) or {}
            total_e = day_data.get("总能耗kWh", {}) or {}
            for dev, val in avg_p.items():
                if isinstance(val, (int, float)):
                    dev_avg.setdefault(dev, []).append(val)
            for dev, val in total_e.items():
                if isinstance(val, (int, float)):
                    dev_energy[dev] = dev_energy.get(dev, 0) + val

        md.append("| 设备 | 平均功率 (kW) | 总能耗 (kWh) |")
        md.append("|:-----|:------------|:------------|")
        for dev in sorted(set(list(dev_avg.keys()) + list(dev_energy.keys()))):
            avgs = dev_avg.get(dev, [])
            avg = sum(avgs) / len(avgs) if avgs else 0
            total = dev_energy.get(dev, 0)
            md.append(f"| {dev} | {_fmt_num(avg)} | {_fmt_num(total)} |")
        md.append("")
    else:
        md.append("_无功率能耗数据_")
        md.append("")

    # ---------- 三、其他系统 ----------
    md.append("## 三、其他系统")
    md.append("")
    others = records.get("其他系统", {}) or {}
    for sys_name in ("高压柜系统", "阀门系统", "进线柜系统", "风门系统", "母联柜系统"):
        sys_data = others.get(sys_name, {})
        if not sys_data:
            continue
        md.append(f"### {sys_name}")
        md.append("")
        for sub_name in ("信号", "控制指令"):
            sub = sys_data.get(sub_name, {}) or {}
            if not sub:
                continue
            md.append(f"**{sub_name}**")
            md.append("")
            for sig_name, sig in sub.items():
                status = _signal_status_multi(sig) if is_multi_day else _signal_status_single(sig)
                md.append(f"- {sig_name}：{status}")
            md.append("")

    return "\n".join(md)

class TongfengMCPService:
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
        self.service = TongfengService(self.client,logger=logger)
        self.station_names = {}

        # pprint(self._fetch_car_realtime_api())
        self.fan_monitor_tags = fan_monitor_tags
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
            TF_YH_1_TING_JI_BAO_JING: "一号风机 1 号设备停机报警信号"
            TF_YH_2_TING_JI_BAO_JING: "一号风机 2 号设备停机报警信号"
            TF_YHBPQ_FAN_ZHUAN_QI_DONG: "一号变频器执行反转启动控制指令"
            TF_YHFJ_SHUI_PING_FENG_MEN_GUAN_FA: "一号风机水平风门执行关闭阀门操作"
            TF_YHFJ_SHUI_PING_FENG_MEN_KAI_FA: "一号风机水平风门执行打开阀门操作"
            TF_YHFJ_SHUI_PING_FENG_MEN_TING_ZHI: "一号风机水平风门执行停止动作操作"
            TF_YHFJ_ZHUN_BEI_JIU_XU: "一号风机系统具备启动运行条件，准备就绪"
            TF_YH_GAO_YA_CHU_XIAN_GUI_HE_ZHA_XIN_HAO: "一号高压出线柜断路器处于合闸状态反馈信号"
            TF_YH_GAO_YA_CHU_XIAN_GUI_YUAN_CHENG_XIN_HAO: "一号高压出线柜处于远程控制模式状态信号"
            TF_YH_GAO_YA_GUI_FEN_ZHA: "一号高压柜执行分闸操作控制指令"
            TF_YH_GAO_YA_GUI_HE_ZHA: "一号高压柜执行合闸操作控制指令"
            TF_YHBPQ_FAN_ZHUAN_XIN_HAO: "一号变频器处于反转运行状态反馈信号"
            TF_YHBPQ_GU_ZHANG_XIN_HAO: "一号变频器检测到故障状态反馈信号"
            TF_YHBPQ_QI_DONG: "一号变频器执行启动运行控制指令"
            TF_YHBPQ_TING_ZHI: "一号变频器执行停止运行控制指令"
            TF_YHBPQ_YUAN_CHENG_XIN_HAO: "一号变频器处于远程控制模式状态信号"
            TF_YHBPQ_YUN_XING_XIN_HAO: "一号变频器处于正常运行状态反馈信号"
            TF_YHBPQ_ZHENG_ZHUAN_XIN_HAO: "一号变频器处于正转运行状态反馈信号"
            TF_YHDF_GUAN_DAO_WEI_XIN_HAO: "一号蝶阀完全关闭到位状态反馈信号"
            TF_YHDF_GUAN_FA: "一号蝶阀执行关闭阀门操作"
            TF_YHDF_KAI_DAO_WEI_XIN_HAO: "一号蝶阀完全打开到位状态反馈信号"
            TF_YHDF_KAI_FA: "一号蝶阀执行打开阀门操作"
            TF_YHDF_TING_ZHI: "一号蝶阀执行停止动作操作"
            TF_YHDF_YUAN_CHENG_XIN_HAO: "一号蝶阀处于远程控制模式状态信号"
            TF_YHDF_ZHENG_ZAI_GUAN_XIN_HAO: "一号蝶阀正在执行关闭动作过程反馈信号"
            TF_YHDF_ZHENG_ZAI_KAI_XIN_HAO: "一号蝶阀正在执行打开动作过程反馈信号"
            TF_YHFJ_SUO_YOU_SHE_BEI_CHU_YU_YUAN_CHENG_ZHUANG_TAI: "一号风机所属全部设备均处于远程控制状态"
            TF_YHFJ_YAN_WU: "一号风机区域检测到烟雾报警信号"
            TF_YHFJ_YI_JIAN_QI_DONG: "一号风机执行一键启动操作指令"
            TF_YHFJ_YI_JIAN_QI_DONG_ZHONG: "一号风机处于一键启动过程运行状态"
            TF_YHFJ_YI_JIAN_TING_ZHI: "一号风机执行一键停止操作指令"
            TF_YHFJ_YI_JIAN_TING_ZHI_ZHONG: "一号风机处于一键停止过程运行状态"
            TF_YH_JIN_XIAN_GUI_FEN_ZHA: "一号进线柜执行分闸操作控制指令"
            TF_YH_JIN_XIAN_GUI_HE_ZHA: "一号进线柜执行合闸操作控制指令"
            TF_YH_JIN_XIAN_GUI_HE_ZHA_XIN_HAO: "一号进线柜断路器处于合闸状态反馈信号"
            TF_YH_JIN_XIAN_GUI_YUAN_CHENG_XIN_HAO: "一号进线柜处于远程控制模式状态信号"
            TF_YH_QIE_EH_ZHUN_BEI_JIU_XU: "一号风机切换至二号风机运行准备就绪状态"
            TF_YH_QIE_HUAN_EH_QI_DONG: "一号风机切换至二号风机启动操作指令"
            TF_YH_QIE_HUAN_EH_YUN_XING_ZHONG: "一号风机切换至二号风机处于切换运行过程"
            TF_YHSPFM_GUAN_DAO_WEI_XIN_HAO: "一号水平风门完全关闭到位状态反馈信号"
            TF_YHSPFM_KAI_DAO_WEI_XIN_HAO: "一号水平风门完全打开到位状态反馈信号"
            TF_YHSPFM_YUAN_CHENG_XIN_HAO: "一号水平风门处于远程控制模式状态信号"
            TF_YHSPFM_ZHENG_ZAI_GUAN: "一号水平风门正在执行关闭动作过程反馈信号"
            TF_YHSPFM_ZHENG_ZAI_KAI: "一号水平风门正在执行打开动作过程反馈信号"
            TF_EH_1_TING_JI_BAO_JING: "二号风机 1 号设备停机报警信号"
            TF_EH_2_TING_JI_BAO_JING: "二号风机 2 号设备停机报警信号"
            TF_EHBPQ_FAN_ZHUAN_QI_DONG: "二号变频器执行反转启动控制指令"
            TF_EHFJ_SHUI_PING_FENG_MEN_GUAN_FA: "二号风机水平风门执行关闭阀门操作"
            TF_EHFJ_SHUI_PING_FENG_MEN_KAI_FA: "二号风机水平风门执行打开阀门操作"
            TF_EHFJ_SHUI_PING_FENG_MEN_TING_ZHI: "二号风机水平风门执行停止动作操作"
            TF_EHFJ_ZHUN_BEI_JIU_XU: "二号风机系统具备启动运行条件，准备就绪"
            TF_EH_GAO_YA_CHU_XIAN_GUI_HE_ZHA_XIN_HAO: "二号高压出线柜断路器处于合闸状态反馈信号"
            TF_EH_GAO_YA_CHU_XIAN_GUI_YUAN_CHENG_XIN_HAO: "二号高压出线柜处于远程控制模式状态信号"
            TF_EH_GAO_YA_GUI_FEN_ZHA: "二号高压柜执行分闸操作控制指令"
            TF_EH_GAO_YA_GUI_HE_ZHA: "二号高压柜执行合闸操作控制指令"
            TF_EHBPQ_FAN_ZHUAN_XIN_HAO: "二号变频器处于反转运行状态反馈信号"
            TF_EHBPQ_GU_ZHANG_XIN_HAO: "二号变频器检测到故障状态反馈信号"
            TF_EHBPQ_QI_DONG: "二号变频器执行启动运行控制指令"
            TF_EHBPQ_TING_ZHI: "二号变频器执行停止运行控制指令"
            TF_EHBPQ_YUAN_CHENG_XIN_HAO: "二号变频器处于远程控制模式状态信号"
            TF_EHBPQ_YUN_XING_XIN_HAO: "二号变频器处于正常运行状态反馈信号"
            TF_EHBPQ_ZHENG_ZHUAN_XIN_HAO: "二号变频器处于正转运行状态反馈信号"
            TF_EHDF_GUAN_DAO_WEI_XIN_HAO: "二号蝶阀完全关闭到位状态反馈信号"
            TF_EHDF_GUAN_FA: "二号蝶阀执行关闭阀门操作"
            TF_EHDF_KAI_DAO_WEI_XIN_HAO: "二号蝶阀完全打开到位状态反馈信号"
            TF_EHDF_KAI_FA: "二号蝶阀执行打开阀门操作"
            TF_EHDF_TING_ZHI: "二号蝶阀执行停止动作操作"
            TF_EHDF_YUAN_CHENG_XIN_HAO: "二号蝶阀处于远程控制模式状态信号"
            TF_EHDF_ZHENG_ZAI_GUAN_XIN_HAO: "二号蝶阀正在执行关闭动作过程反馈信号"
            TF_EHDF_ZHENG_ZAI_KAI_XIN_HAO: "二号蝶阀正在执行打开动作过程反馈信号"
            TF_EHFJ_SUO_YOU_SHE_BEI_CHU_YU_YUAN_CHENG_ZHUANG_TAI: "二号风机所属全部设备均处于远程控制状态"
            TF_EHFJ_YAN_WU: "二号风机区域检测到烟雾报警信号"
            TF_EHFJ_YI_JIAN_QI_DONG: "二号风机执行一键启动操作指令"
            TF_EHFJ_YI_JIAN_QI_DONG_ZHONG: "二号风机处于一键启动过程运行状态"
            TF_EHFJ_YI_JIAN_TING_ZHI: "二号风机执行一键停止操作指令"
            TF_EHFJ_YI_JIAN_TING_ZHI_ZHONG: "二号风机处于一键停止过程运行状态"
            TF_EH_JIN_XIAN_GUI_FEN_ZHA: "二号进线柜执行分闸操作控制指令"
            TF_EH_JIN_XIAN_GUI_HE_ZHA: "二号进线柜执行合闸操作控制指令"
            TF_EH_JIN_XIAN_GUI_HE_ZHA_XIN_HAO: "二号进线柜断路器处于合闸状态反馈信号"
            TF_EH_JIN_XIAN_GUI_YUAN_CHENG_XIN_HAO: "二号进线柜处于远程控制模式状态信号"
            TF_EH_QIE_1_HAO_ZHUN_BEI_JIU_XU: "二号风机切换至一号风机运行准备就绪状态"
            TF_EH_QIE_HUAN_1_HAO_QI_DONG: "二号风机切换至一号风机启动操作指令"
            TF_EH_QIE_HUAN_1_HAO_YUN_XING_ZHONG: "二号风机切换至一号风机处于切换运行过程"
            TF_EHSPFM_GUAN_DAO_WEI_XIN_HAO: "二号水平风门完全关闭到位状态反馈信号"
            TF_EHSPFM_KAI_DAO_WEI_XIN_HAO: "二号水平风门完全打开到位状态反馈信号"
            TF_EHSPFM_YUAN_CHENG_XIN_HAO: "二号水平风门处于远程控制模式状态信号"
            TF_EHSPFM_ZHENG_ZAI_GUAN: "二号水平风门正在执行关闭动作过程反馈信号"
            TF_EHSPFM_ZHENG_ZAI_KAI: "二号水平风门正在执行打开动作过程反馈信号"
            TF_SH_BIAN_PIN_QI_FAN_ZHUAN_QI_DONG: "三号变频器执行反转启动控制指令"
            TF_SH_HAO_BIAN_PIN_QI_FAN_ZHUAN_XIN_HAO: "三号变频器处于反转运行状态反馈信号"
            TF_SH_HAO_BIAN_PIN_QI_GU_ZHANG_XIN_HAO: "三号变频器检测到故障状态反馈信号"
            TF_SH_HAO_BIAN_PIN_QI_QI_DONG: "三号变频器执行启动运行控制指令"
            TF_SH_HAO_BIAN_PIN_QI_TING_ZHI: "三号变频器执行停止运行控制指令"
            TF_SH_HAO_BIAN_PIN_QI_YUAN_CHENG_XIN_HAO: "三号变频器处于远程控制模式状态信号"
            TF_SH_HAO_BIAN_PIN_QI_YUN_XING_XIN_HAO: "三号变频器处于正常运行状态反馈信号"
            TF_SH_HAO_BIAN_PIN_QI_ZHENG_ZHUAN_XIN_HAO: "三号变频器处于正转运行状态反馈信号"
            TF_SI_HAO_BIAN_PIN_QI_FAN_ZHUAN_QI_DONG: "四号变频器执行反转启动控制指令"
            TF_SI_HAO_BIAN_PIN_QI_FAN_ZHUAN_XIN_HAO: "四号变频器处于反转运行状态反馈信号"
            TF_SI_HAO_BIAN_PIN_QI_GU_ZHANG_XIN_HAO: "四号变频器检测到故障状态反馈信号"
            TF_SI_HAO_BIAN_PIN_QI_QI_DONG: "四号变频器执行启动运行控制指令"
            TF_SI_HAO_BIAN_PIN_QI_TING_ZHI: "四号变频器执行停止运行控制指令"
            TF_SI_HAO_BIAN_PIN_QI_YUAN_CHENG_XIN_HAO: "四号变频器处于远程控制模式状态信号"
            TF_SI_HAO_BIAN_PIN_QI_YUN_XING_XIN_HAO: "四号变频器处于正常运行状态反馈信号"
            TF_SI_HAO_BIAN_PIN_QI_ZHENG_ZHUAN_XIN_HAO: "四号变频器处于正转运行状态反馈信号"
            TF_MU_LIAN_GUI_FEN_ZHA: "母联柜执行分闸操作控制指令"
            TF_MU_LIAN_GUI_HE_ZHA: "母联柜执行合闸操作控制指令"
            TF_MU_LIAN_GUI_HE_ZHA_XIN_HAO: "母联柜断路器处于合闸状态反馈信号"
            TF_MU_LIAN_GUI_YUAN_CHENG_XIN_HAO: "母联柜处于远程控制模式状态信号"
            TF_BAO_JING_KAI_GUAN: "系统报警总开关控制信号"
            TF_BAO_JING_SHU_CHU_ZHI_HMI: "系统报警信息输出至人机交互界面信号"
            TF_CHENG_XU_ZHI_XING_ZHONG: "系统控制程序正在执行过程状态信号"
            TF_LIANG_TAI_FENG_JI_DU_WEI_YUN_XING: "一号、二号两台风机均处于未运行状态"
            TF_YI_JIAN_FU_WEI: "系统故障及状态执行一键复位操作指令"
            TF_ZI_DONG_SHOU_DONG_QIE_HUAN: "系统控制模式自动与手动方式切换操作"
            TF_ZUO_YOU_JIN_XIAN_GUI_DU_WEI_HE_ZHA_BAO_JING: "左、右两路进线柜均未合闸故障报警信号"
            TF_YH_1_DIAN_LIU_XI_SHU: "一号风机 1 号电机电流修正计算系数"
            TF_YH_2_DIAN_LIU_XI_SHU: "一号风机 2 号电机电流修正计算系数"
            TF_YH_1_JI_CHUI_ZHI_ZHEN_DONG_SHI_JI_ZHI: "一号风机 1 级设备垂直振动实时监测数值"
            TF_YH_1_JI_CHUI_ZHI_ZHEN_DONG_XI_SHU: "一号风机 1 级设备垂直振动修正计算系数"
            TF_YH_1_JI_DIAN_LIU_A_SHI_JI_ZHI: "一号风机 1 级设备 A 相电流实时监测数值"
            TF_YH_1_JI_DIAN_YA_A_SHI_JI_ZHI: "一号风机 1 级设备 A 相电压实时监测数值"
            TF_YH_1_JI_DING_ZI_WEN_DU_A_SHI_JI_ZHI: "一号风机 1 级设备 A 相定子温度实时监测数值"
            TF_YH_1_JI_DING_ZI_WEN_DU_A_XI_SHU: "一号风机 1 级设备 A 相定子温度修正计算系数"
            TF_YH_1_JI_DING_ZI_WEN_DU_B_SHI_JI_ZHI: "一号风机 1 级设备 B 相定子温度实时监测数值"
            TF_YH_1_JI_DING_ZI_WEN_DU_B_XI_SHU: "一号风机 1 级设备 B 相定子温度修正计算系数"
            TF_YH_1_JI_DING_ZI_WEN_DU_C_SHI_JI_ZHI: "一号风机 1 级设备 C 相定子温度实时监测数值"
            TF_YH_1_JI_DING_ZI_WEN_DU_C_XI_SHU: "一号风机 1 级设备 C 相定子温度修正计算系数"
            TF_YH_1_JI_GONG_LV_SHI_JI_ZHI: "一号风机 1 级设备有功功率实时监测数值"
            TF_YH_1_JI_HOU_ZHOU_WEN_DU_SHI_JI_ZHI: "一号风机 1 级设备后轴温度实时监测数值"
            TF_YH_1_JI_HOU_ZHOU_WEN_DU_XI_SHU: "一号风机 1 级设备后轴温度修正计算系数"
            TF_YH_1_JI_QIAN_ZHOU_WEN_DU_SHI_JI_ZHI: "一号风机 1 级设备前轴温度实时监测数值"
            TF_YH_1_JI_QIAN_ZHOU_WEN_DU_XI_SHU: "一号风机 1 级设备前轴温度修正计算系数"
            TF_YH_1_JI_SHUI_PING_ZHEN_DONG_SHI_JI_ZHI: "一号风机 1 级设备水平振动实时监测数值"
            TF_YH_1_JI_SHUI_PING_ZHEN_DONG_XI_SHU: "一号风机 1 级设备水平振动修正计算系数"
            TF_YH_2_JI_CHUI_ZHI_ZHEN_DONG_SHI_JI_ZHI: "一号风机 2 级设备垂直振动实时监测数值"
            TF_YH_2_JI_CHUI_ZHI_ZHEN_DONG_XI_SHU: "一号风机 2 级设备垂直振动修正计算系数"
            TF_YH_2_JI_DIAN_LIU_A_SHI_JI_ZHI: "一号风机 2 级设备 A 相电流实时监测数值"
            TF_YH_2_JI_DIAN_YA_A_SHI_JI_ZHI: "一号风机 2 级设备 A 相电压实时监测数值"
            TF_YH_2_JI_DING_ZI_WEN_DU_A_SHI_JI_ZHI: "一号风机 2 级设备 A 相定子温度实时监测数值"
            TF_YH_2_JI_DING_ZI_WEN_DU_A_XI_SHU: "一号风机 2 级设备 A 相定子温度修正计算系数"
            TF_YH_2_JI_DING_ZI_WEN_DU_B_SHI_JI_ZHI: "一号风机 2 级设备 B 相定子温度实时监测数值"
            TF_YH_2_JI_DING_ZI_WEN_DU_B_XI_SHU: "一号风机 2 级设备 B 相定子温度修正计算系数"
            TF_YH_2_JI_DING_ZI_WEN_DU_C_SHI_JI_ZHI: "一号风机 2 级设备 C 相定子温度实时监测数值"
            TF_YH_2_JI_DING_ZI_WEN_DU_C_XI_SHU: "一号风机 2 级设备 C 相定子温度修正计算系数"
            TF_YH_2_JI_GONG_LV_SHI_JI_ZHI: "一号风机 2 级设备有功功率实时监测数值"
            TF_YH_2_JI_HOU_ZHOU_WEN_DU_SHI_JI_ZHI: "一号风机 2 级设备后轴温度实时监测数值"
            TF_YH_2_JI_HOU_ZHOU_WEN_DU_XI_SHU: "一号风机 2 级设备后轴温度修正计算系数"
            TF_YH_2_JI_QIAN_ZHOU_WEN_DU_SHI_JI_ZHI: "一号风机 2 级设备前轴温度实时监测数值"
            TF_YH_2_JI_QIAN_ZHOU_WEN_DU_XI_SHU: "一号风机 2 级设备前轴温度修正计算系数"
            TF_YH_2_JI_SHUI_PING_ZHEN_DONG_SHI_JI_ZHI: "一号风机 2 级设备水平振动实时监测数值"
            TF_YH_2_JI_SHUI_PING_ZHEN_DONG_XI_SHU: "一号风机 2 级设备水平振动修正计算系数"
            TF_YHBPQ_PIN_LV_FAN_KUI: "一号变频器输出运行频率实时反馈数值"
            TF_YHBPQ_PIN_LV_GEI_DING: "一号变频器运行频率设定给定数值"
            TF_YHFJ_DONG_YA_SHI_JI_ZHI: "一号风机动压实时监测数值"
            TF_YHFJ_DONG_YA_XI_SHU: "一号风机动压修正计算系数"
            TF_YHFJ_FENG_LIANG_SHI_JI_ZHI: "一号风机风量实时监测数值"
            TF_YHFJ_FENG_SU_SHI_JI_ZHI: "一号风机风速实时监测数值"
            TF_YHFJ_JING_YA_SHI_JI_ZHI: "一号风机静压实时监测数值"
            TF_YHFJ_QUAN_YA_SHI_JI_ZHI: "一号风机全压实时监测数值"
            TF_YHFJ_YUN_XING_XIAO_LV: "一号风机实际运行效率计算数值"
            TF_YH_FENG_LIANG_XI_SHU: "一号风机风量修正计算系数"
            TF_YH_FENG_SU_XI_SHU: "一号风机风速修正计算系数"
            TF_YH_JING_YA_XI_SHU: "一号风机静压修正计算系数"
            TF_YH_QUAN_YA_XI_SHU: "一号风机全压修正计算系数"
            TF_YH_XIAO_LV_XI_SHU: "一号风机运行效率修正计算系数"
            TF_YH_ZU_LI_XI_SHU: "一号风机风路阻力修正计算系数"
            TF_EH_1_DIAN_LIU_XI_SHU: "二号风机 1 号电机电流修正计算系数"
            TF_EH_2_DIAN_LIU_XI_SHU: "二号风机 2 号电机电流修正计算系数"
            TF_EH_1_JI_CHUI_ZHI_ZHEN_DONG_SHI_JI_ZHI: "二号风机 1 级设备垂直振动实时监测数值"
            TF_EH_1_JI_CHUI_ZHI_ZHEN_DONG_XI_SHU: "二号风机 1 级设备垂直振动修正计算系数"
            TF_EH_1_JI_DIAN_LIU_A_SHI_JI_ZHI: "二号风机 1 级设备 A 相电流实时监测数值"
            TF_EH_1_JI_DIAN_YA_A_SHI_JI_ZHI: "二号风机 1 级设备 A 相电压实时监测数值"
            TF_EH_1_JI_DING_ZI_WEN_DU_A_SHI_JI_ZHI: "二号风机 1 级设备 A 相定子温度实时监测数值"
            TF_EH_1_JI_DING_ZI_WEN_DU_A_XI_SHU: "二号风机 1 级设备 A 相定子温度修正计算系数"
            TF_EH_1_JI_DING_ZI_WEN_DU_B_SHI_JI_ZHI: "二号风机 1 级设备 B 相定子温度实时监测数值"
            TF_EH_1_JI_DING_ZI_WEN_DU_B_XI_SHU: "二号风机 1 级设备 B 相定子温度修正计算系数"
            TF_EH_1_JI_DING_ZI_WEN_DU_C_SHI_JI_ZHI: "二号风机 1 级设备 C 相定子温度实时监测数值"
            TF_EH_1_JI_DING_ZI_WEN_DU_C_XI_SHU: "二号风机 1 级设备 C 相定子温度修正计算系数"
            TF_EH_1_JI_GONG_LV_SHI_JI_ZHI: "二号风机 1 级设备有功功率实时监测数值"
            TF_EH_1_JI_HOU_ZHOU_WEN_DU_SHI_JI_ZHI: "二号风机 1 级设备后轴温度实时监测数值"
            TF_EH_1_JI_HOU_ZHOU_WEN_DU_XI_SHU: "二号风机 1 级设备后轴温度修正计算系数"
            TF_EH_1_JI_QIAN_ZHOU_WEN_DU_SHI_JI_ZHI: "二号风机 1 级设备前轴温度实时监测数值"
            TF_EH_1_JI_QIAN_ZHOU_WEN_DU_XI_SHU: "二号风机 1 级设备前轴温度修正计算系数"
            TF_EH_1_JI_SHUI_PING_ZHEN_DONG_SHI_JI_ZHI: "二号风机 1 级设备水平振动实时监测数值"
            TF_EH_1_JI_SHUI_PING_ZHEN_DONG_XI_SHU: "二号风机 1 级设备水平振动修正计算系数"
            TF_EH_2_JI_CHUI_ZHI_ZHEN_DONG_SHI_JI_ZHI: "二号风机 2 级设备垂直振动实时监测数值"
            TF_EH_2_JI_CHUI_ZHI_ZHEN_DONG_XI_SHU: "二号风机 2 级设备垂直振动修正计算系数"
            TF_EH_2_JI_DIAN_LIU_A_SHI_JI_ZHI: "二号风机 2 级设备 A 相电流实时监测数值"
            TF_EH_2_JI_DIAN_YA_A_SHI_JI_ZHI: "二号风机 2 级设备 A 相电压实时监测数值"
            TF_EH_2_JI_DING_ZI_WEN_DU_A_SHI_JI_ZHI: "二号风机 2 级设备 A 相定子温度实时监测数值"
            TF_EH_2_JI_DING_ZI_WEN_DU_A_XI_SHU: "二号风机 2 级设备 A 相定子温度修正计算系数"
            TF_EH_2_JI_DING_ZI_WEN_DU_B_SHI_JI_ZHI: "二号风机 2 级设备 B 相定子温度实时监测数值"
            TF_EH_2_JI_DING_ZI_WEN_DU_B_XI_SHU: "二号风机 2 级设备 B 相定子温度修正计算系数"
            TF_EH_2_JI_DING_ZI_WEN_DU_C_SHI_JI_ZHI: "二号风机 2 级设备 C 相定子温度实时监测数值"
            TF_EH_2_JI_DING_ZI_WEN_DU_C_XI_SHU: "二号风机 2 级设备 C 相定子温度修正计算系数"
            TF_EH_2_JI_GONG_LV_SHI_JI_ZHI: "二号风机 2 级设备有功功率实时监测数值"
            TF_EH_2_JI_HOU_ZHOU_WEN_DU_SHI_JI_ZHI: "二号风机 2 级设备后轴温度实时监测数值"
            TF_EH_2_JI_HOU_ZHOU_WEN_DU_XI_SHU: "二号风机 2 级设备后轴温度修正计算系数"
            TF_EH_2_JI_QIAN_ZHOU_WEN_DU_SHI_JI_ZHI: "二号风机 2 级设备前轴温度实时监测数值"
            TF_EH_2_JI_QIAN_ZHOU_WEN_DU_XI_SHU: "二号风机 2 级设备前轴温度修正计算系数"
            TF_EH_2_JI_SHUI_PING_ZHEN_DONG_SHI_JI_ZHI: "二号风机 2 级设备水平振动实时监测数值"
            TF_EH_2_JI_SHUI_PING_ZHEN_DONG_XI_SHU: "二号风机 2 级设备水平振动修正计算系数"
            TF_EHBPQ_PIN_LV_FAN_KUI: "二号变频器输出运行频率实时反馈数值"
            TF_EHBPQ_PIN_LV_GEI_DING: "二号变频器运行频率设定给定数值"
            TF_EHFJ_DONG_YA_SHI_JI_ZHI: "二号风机动压实时监测数值"
            TF_EHFJ_DONG_YA_XI_SHU: "二号风机动压修正计算系数"
            TF_EHFJ_FENG_LIANG_SHI_JI_ZHI: "二号风机风量实时监测数值"
            TF_EHFJ_FENG_SU_SHI_JI_ZHI: "二号风机风速实时监测数值"
            TF_EHFJ_JING_YA_SHI_JI_ZHI: "二号风机静压实时监测数值"
            TF_EHFJ_QUAN_YA_SHI_JI_ZHI: "二号风机全压实时监测数值"
            TF_EHFJ_YUN_XING_XIAO_LV: "二号风机实际运行效率计算数值"
            TF_EH_FENG_LIANG_XI_SHU: "二号风机风量修正计算系数"
            TF_EH_FENG_SU_XI_SHU: "二号风机风速修正计算系数"
            TF_EH_JING_YA_XI_SHU: "二号风机静压修正计算系数"
            TF_EH_QUAN_YA_XI_SHU: "二号风机全压修正计算系数"
            TF_EH_XIAO_LV_XI_SHU: "二号风机运行效率修正计算系数"
            TF_EH_ZU_LI_XI_SHU: "二号风机风路阻力修正计算系数"
            TF_SH_HAO_BIAN_PIN_QI_PIN_LV_FAN_KUI: "三号变频器输出运行频率实时反馈数值"
            TF_SH_HAO_BIAN_PIN_QI_PIN_LV_GEI_DING: "三号变频器运行频率设定给定数值"
            TF_SI_HAO_BIAN_PIN_QI_PIN_LV_FAN_KUI: "四号变频器输出运行频率实时反馈数值"
            TF_SI_HAO_BIAN_PIN_QI_PIN_LV_GEI_DING: "四号变频器运行频率设定给定数值"
            TF_TEST: "系统测试模式标识位"
            TF_CHU_SHI_HUA: "系统参数及状态初始化执行指令"
            TF_DIAN_LIU_BAO_JING_ZHI_SHE_DING: "设备电流超限报警阈值设定数值"
            TF_DIAN_LIU_BIAN_BI: "电流信号互感器变比参数设定数值"
            TF_DIAN_YA_BIAN_BI: "电压信号互感器变比参数设定数值"
            TF_DING_ZI_WEN_DU_BAO_JING_ZHI_SHE_DING: "电机定子温度超限报警阈值设定数值"
            TF_FENG_TONG_JIE_MIAN_JI: "风机风筒横截面积参数设定数值"
            TF_GONG_LV_YIN_SHU: "系统功率因数补偿与计算参数"
            TF_ZHEN_DONG_BAO_JING_ZHI_SHE_DING: "设备振动超限报警阈值设定数值"
            TF_ZHOU_CHENG_WEN_DU_BAO_JING_ZHI_SHE_DING: "轴承温度超限报警阈值设定数值"
            TF_TIMESTAMP: "监测数据采集并录入数据表的时间戳"

            """

    # ==================== 2. Prompt: 全局行为准则 ====================
    def _register_prompts(self):

        @self.mcp.prompt()
        def analysis_guide() -> str:
            """
            获取通风分析的专业操作指南。模型在处理用户请求前应默认加载此提示词。
            本指南包含不同工具的最佳使用场景，可指导大模型合理推断、自动调用合适的查询与分析工具。
            """
            return """
            本系统是通风系统，你现在是一名矿井安全生产调度专家，具备丰富的井下作业与数据分析经验。使用本系统时，请严格遵循以下操作准则，结合各工具的用途，科学调用、组合工具以获得精确答案：
            - 回复时，涉及任何数值时必须带上如下对应单位：
                - 温度：℃  
                - 振动：mm/s  
                - 变频器频率：Hz  
                - 风量：m³/min  
                - 风速：m/s
                - 功率：kW   
                - 压力：Pa  
                - 效率：%
            【1. 查询风机运行数据】
            - 若需获取一号、二号或全部风机在指定时间段内的报警、控制指令、状态、切换过程、修正系数、数值等主要参数，可调用 `query_fengji_records(choose, start_time, end_time, subgroup_filters, value_filters)`。
            - 其中 choose 可选 "1"（仅一号风机）、"2"（仅二号风机）、"all"（全部，默认），start_time/end_time 支持 "YYYY-MM-DD HH:MM:SS" 格式，subgroup_filters 可选分组如"报警"、"控制指令"、"状态"、"切换过程"、"数值"。
            - value_filters 仅当 subgroup_filters 包含 "数值" 时可用，例如 {"风速": (">", -100)}，用于定子温度、轴温度、电流、电压、振动、功率、风速、静压、全压、效率、风量、动压等字段的条件过滤。

            【2. 获取系统基准时间】
            - 需要用于确定查询时段或对外展示服务当前时刻，可调用 `get_system_time()` 获取服务器时间，格式为 {"current_time": "YYYY-MM-DD HH:MM:SS", "weekday": "Monday"}。

            【3. 查询其他系统级数据】
            - 若需查询高压柜、阀门、进线柜、风门、母联柜等非风机类系统的主要参数，可使用 `query_others_system_records(choose, start_time, end_time)` 工具。
            - 其中 choose 可选 "system"（系统级别）、"gaoyagui"（高压柜）、"fameng"（阀门）、"jinxiangui"（进线柜）、"fengmen"（风门）、"mulian"（母联柜），也可为这些系统的字符串数组，或用 "all" 查询全部支持的系统。
            - start_time/end_time 支持 "YYYY-MM-DD HH:MM:SS" 格式，未指定时默认今日0时至现在。

            【4. 查询设备级参数/历史数据】
            - 获取设备级别的设定参数、报警限定、数值设定等，请使用 `query_shebei_system_records(start_time, end_time)` 工具。支持跨天数据统计及设备参数变动查询。

            【5. 查询变频器系统数据】
            - 若需查询变频器系统在指定时间段内的主要参数，可调用 `query_bianpinqi_system_records(start_time, end_time)` 工具。
            - start_time/end_time 支持 "YYYY-MM-DD HH:MM:SS" 格式，未指定时默认今日0时至现在。查询时间范围大于一天时，仅返回统计信息与归档结果，需要更详细数据请缩短时间段。

            【6. 获取所有支持的字段及分组说明】
            - 如需获取当前系统全部支持的字段列表与分组、注释说明，可调用 `get_supported_fields()` 工具，便于自定义查询或做更细粒度的筛选。

            【7. 查询风机功率能耗】
            - 如需查询一号/二号风机（含1级、2级设备）在指定时间段内的功率能耗数据或历史趋势，可调用 `query_power_energy_records(start_time, end_time)` 工具,自动返回结构化的能耗统计与结果说明。
             - start_time/end_time 支持 "YYYY-MM-DD HH:MM:SS" 格式，未指定时默认今日0时至现在。查询时间范围大于一天时，仅返回统计信息与归档结果，需要更详细数据请缩短时间段。

            【8. 生成日报 / 多日报告】
            - 若用户明确说通风系统的"出一份报告"、"生成日报"、"汇总汇报"、"周报"、"近N天情况"等，请调用 `generate_tongfeng_report(start_date, end_date)`。
            - 入参只有两个：`start_date` 和 `end_date`（格式 "YYYY-MM-DD HH:MM:SS" 或 "YYYY-MM-DD"）。
              - 同一天 → **单日报告**；跨天 → **多日报告**（多日已做跨天聚合，不按天罗列）。
            - 报告固定包含三块：
              1. **风机系统**：报警 + 状态 + 数值（含风量/风速/静压/全压/效率等关键监测角度）
              2. **功率能耗**：一号/二号风机 1级、2级设备的功率能耗
              3. **其他系统**：高压柜 / 阀门 / 进线柜 / 风门 / 母联柜
            - **不要**用 "`query_fengji_records` + `query_power_energy_records` + `query_others_system_records`" 手工拼装日报——三块内容已内聚到本工具，一次调用即可。

            【9. 通风系统的报告 vs 单项查询的选型】
            - 需要**报告结构**（三块一次返回）→ 用 `generate_tongfeng_report`
            - 只需要**风机单项数据** → 用 `query_fengji_records`
            - 只需要**功率能耗** → 用 `query_power_energy_records`
            - 只需要**其他系统** → 用 `query_others_system_records`
            - 只需要**变频器/设备级** → 用 `query_bianpinqi_system_records` / `query_shebei_system_records`

            # 注意事项：
            - 未指定时间时，风机查询默认今日0时至现在；
            - 字段、分组规范详见 tongfeng_system.yaml 配置及各字段注释；
            - 查询范围大于一天时，主要返回统计或TOP信息，可通过缩短时段获得明细数据。


            请根据用户需求描述，优先选择最贴切的工具，并合理组合使用（如先查询时间基准，再用分段、批量筛选工具）。分析时尽量给出简洁明了、结构化的答案，并解释数据中的主要维度。
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
        def query_fengji_records(
                choose: Optional[Union[str, int]] = "all",  # "1"（仅一号风机）, "2"（仅二号风机）, "all"（全部风机，默认）。可以为字符串或数字
                start_time: Optional[str] = None,  # 起始时间，YYYY-MM-DD HH:MM:SS"，可选
                end_time: Optional[str] = None,  # 结束时间，"YYYY-MM-DD HH:MM:SS"，可选
                subgroup_filters: Union[List[str], str, None] = None,  # 分组筛选（如"切换过程"），可选
                value_filters: Optional[Dict[str, tuple]] = None,  # 字段值条件过滤，如{"风量": (">", -100)}，可选
        ) -> str:
            """
            查询通风系统一号、二号或全部风机在指定时间段内的主要参数（报警、控制指令、状态、切换过程、修正系数、数值）。
            字段严格遵循 tongfeng_system.yaml 配置，分组及字段全为大写并带编号，返回结构包含英文名及中文注释。
            
            参数说明：
                - choose: 风机选择。可选值："1"（仅一号风机）、"2"（仅二号风机）、"all"（全部，默认）。
                    - 其中“一号风机系统”所有字段均以 TF_YH 开头，“二号风机系统”所有字段均以 TF_EH 开头。
                - start_time: 起始时间（格式："YYYY-MM-DD HH:MM:SS"），可选。默认当天00:00:00。
                - end_time: 结束时间（同上格式），可选。默认服务当前时间。
                - subgroup_filters: 可以不传，不传返回所有数据，仅能从【"报警"、"控制指令"、"状态"、"切换过程"、"数值"】中选择一个或多个，表示只返回指定分组下的字段。查询运行状态时请调用"数值"。
                - value_filters: 仅当 subgroup_filters 包含 "数值" 时可用，并且只能在【定子温度、轴温度、电流、电压、振动、功率、风速、静压、全压、效率、风量、动压】这些字段进行筛选。格式为 {"字段名": (">", 阈值)}，如 {"风速": (">", -100)},其中阈值默认为-100，仅支持筛选"数值"分组对应字段。
            
            字段分组及简要说明：
            - 报警：表示风机的运行/停机等报警信号，包括：
                · 一号风机 1 号设备停机报警信号
                · 一号风机 2 号设备停机报警信号
                · 一号风机区域检测到烟雾报警信号
            - 指令：用于风机远程或自动操作控制，包括：
                · 水平风门执行关闭/打开/停止动作指令
                · 执行一键启动/一键停止操作指令
            - 状态：反映风机及系统当前运行或切换等逻辑状态，包括：
                · 一号、二号两台风机均处于未运行状态
                · 一号风机具备启动运行条件、准备就绪
                · 所有设备已处于远程控制状态
                · 处于一键启动/一键停止过程
            - 切换过程：记录风机切换到二号风机时的准备、启动及运行过程
            - 数值：实时监测风机及其子设备各类运行参数，包括：
                · 振动、电流、电压、定子温度、轴温、功率、风量、风速、静压、全压、运行效率等多项实时数值参数

            返回值说明：
                - 查询失败时：
                    {
                        "error": "查询失败",
                        "message": "错误说明"
                    }
                - 未查到记录或无数据：
                    {
                        "message": "未找到指定风机在给定时间范围内的记录"
                    }
                - 查询成功时，返回多层结构化JSON，各分组字段下有字段英文名、中文说明及分析内容。如果时间跨度超过一天（跨天查询），将提供统计归纳与top3信息，结构包含每日分组与分段统计等。

            特别提示：
                - 当查询时间范围大于一天时，仅返回统计信息与结果归档（如"重要变化数量"/"top3"等）；
                - 若需获更精确/详细的数据分析，请指定更短的时间段（建议不大于1天）。
            """
            # 参数转换和校验
            # INSERT_YOUR_CODE
            logger.info(
                f"query_fan_system_records called: choose={choose}, start_time={start_time}, end_time={end_time}, "
                f"subgroup_filters={subgroup_filters}, value_filters={value_filters}"
            )
     
            # 对 value_filters 做 fan_monitor_tags 合法性校验
            # 仅当 subgroup_filters 包含 "数值" 时，value_filters 生效
            allowed_types = ["定子温度", "轴温度", "电流", "电压", "振动", "功率", "风速", "静压", "全压", "效率",
                             "风量", "动压"]
            valid_value_fields = self.fan_monitor_tags
            if subgroup_filters is not None:
                if isinstance(subgroup_filters, str):
                    subgroups = [subgroup_filters]
                elif isinstance(subgroup_filters, list):
                    subgroups = subgroup_filters
                else:
                    subgroups = []
            else:
                subgroups = []

            # 若未包含"数值"，不允许 value_filters
            if value_filters is not None:
                if "数值" not in subgroups:
                    value_filters = None
                else:
                    # 保留 value_filters 中字段名为合法 "数值" 字段的部分（交集筛选）
                    # 将 valid_value_fields 的 values 合成一组合法字段列表，再据此做筛选
                    value_filtersnew = {}
                    for k, v in value_filters.items():
                        if k in valid_value_fields:
                            valid_values = {value: v for value in valid_value_fields[k]}
                            value_filtersnew.update(valid_values)

                    value_filters = value_filtersnew

            system_map = {
                "1": ["一号风机系统"],
                "2": ["二号风机系统"],
                1: ["一号风机系统"],
                2: ["二号风机系统"],
                "all": ["一号风机系统", "二号风机系统"],
                None: ["一号风机系统", "二号风机系统"],
            }
            system_name_filters = system_map.get(choose, ["一号风机系统", "二号风机系统"])
            # 处理时间参数
            try:
                if start_time is None:
                    from datetime import datetime, timedelta
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
                records = self.service.print_tongfeng_today_with_cache(
                    system_name_filters=system_name_filters,
                    start_date=start_date, end_date=end_date,
                    subgroup_filters=subgroup_filters,
                    value_filters=value_filters
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
                        "tip": "当前查询为跨天统计结果，如需更精确分析，请提供更详细的时间段和分析角度，如[\"定子温度\",\"轴温度\", \"电流\", \"电压\", \"振动\", \"功率\", \"风速\", \"静压\", \"全压\", \"效率\", \"风量\", \"动压\"]，建议范围为1天内。"
                    }
                    result_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
                   
                    if len(result_json) > 100000 and value_filters is None:
                        filtered_records = self.filter_records(records)
                        result = {
                            "result": filtered_records,
                            "tip": "结果过大，仅保留主要监测角度。如需其他监测数据，请提供更详细的查询条件,如[\"定子温度\",\"轴温度\", \"电流\", \"电压\", \"振动\", \"功率\", \"风速\", \"静压\", \"全压\", \"效率\", \"风量\", \"动压\"]。"
                        }
                        result_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"),
                                                 default=json_serializer)
                    logger.info(f"query_fengji_records 正常返回，use_serializer={use_serializer}, 返回json长度={len(result_json)}")
                    return result_json
                else:
                    # 返回标准分析
                    result_json = json.dumps(records, ensure_ascii=False, separators=(",", ":"),
                                             default=json_serializer)
                    if len(result_json) > 100000:
                        records = self.json_serializer(records)
                        result = {
                            "result": records,
                            "tip": "当前查询为当天统计结果，但由于数据过多，如需更精确分析，请提供更详细的时间段和分析角度，如[\"定子温度\",\"轴温度\", \"电流\", \"电压\", \"振动\", \"功率\", \"风速\", \"静压\", \"全压\", \"效率\", \"风量\", \"动压\"]"

                        }
                        result_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"),
                                                 default=json_serializer)
                    # INSERT_YOUR_CODE
                    logger.info(f"query_fengji_records 正常返回，use_serializer={use_serializer}, 返回json长度={len(result_json)}")
             
                    return result_json

            except Exception as e:
                logger.error(f"query_fengji_records 异常: {e}", exc_info=True)
                return json.dumps(
                    {"error": "查询失败", "message": str(e)},
                    ensure_ascii=False
                )

        @self.mcp.tool()
        def query_others_system_records(
                choose: Optional[Union[str, List[str]]] = "all",
                # 可为单个字符串或字符串列表: "gaoyagui"、["gaoyagui", "fameng"]等，"all"为全部系统，默认
                start_time: Optional[str] = None,  # 起始时间，YYYY-MM-DD HH:MM:SS，可选
                end_time: Optional[str] = None  # 结束时间，YYYY-MM-DD HH:MM:SS，可选
        ) -> str:
            """
            查询通风系统高压柜系统、阀门系统、进线柜系统、风门系统、母联柜系统的主要参数。
            字段严格遵循配置，返回结构包含字段英文名及中文注释。

            参数说明：
                - choose: 系统选择。可选值：
                    * 字符串："system"(系统级别的), "gaoyagui"（高压柜系统）, "fameng"（阀门系统）, "jinxiangui"（进线柜系统）, "fengmen"（风门系统）, "mulian"（母联柜系统）, "all"（全部，默认）
                    * 字符串数组：如 ["gaoyagui", "fameng"] 表示同时查询多个系统
                - start_time: 起始时间（格式："YYYY-MM-DD HH:MM:SS"），可选，默认当天00:00:00。
                - end_time: 结束时间（格式："YYYY-MM-DD HH:MM:SS"），可选，默认服务当前时间。

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
            logger.debug(f"[query_others_system_records] 入参 choose={choose}, start_time={start_time}, end_time={end_time}")
     
            try:
                # 系统英文名与yaml配置映射
                system_map = {
                    "system": "系统级",
                    "gaoyagui": "高压柜系统",
                    "fameng": "阀门系统",
                    "jinxiangui": "进线柜系统",
                    "fengmen": "风门系统",
                    "mulian": "母联柜系统"
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
                        choose_key = key.lower()
                        if choose_key in system_map:
                            system_names_tmp.append(system_map[choose_key])
                        else:
                            return json.dumps({"error": "参数错误", "message": f"不支持的系统类型: {choose_key}"},
                                              ensure_ascii=False)
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
                records = self.service.print_tongfeng_today_with_cache(
                    system_name_filters=system_names,
                    start_date=query_start, end_date=query_end,

                )
                # 检查是否所有为空
                if all(not v for v in records.values()):
                    return json.dumps({"message": "未找到指定系统在给定时间范围内的记录"}, ensure_ascii=False)

                # 假定 self.json_serializer 适配所有这几类系统结构
                result = {
                    "result": records,
                    "tip": "当前为跨天统计/归档，如需详细数据请仅查一天或指定更短时间段。"
                }
                result_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"),
                                         default=json_serializer)
                logger.info(f'queryotherssystems请求了以下系统: {system_names}，结果json长度: {len(result_json)}')
           
                return result_json

            except Exception as e:
                logger.error(f"query_others_system_records 异常: {e}", exc_info=True)
                return json.dumps(
                    {"error": "查询失败", "message": str(e)},
                    ensure_ascii=False
                )

        @self.mcp.tool()
        def query_bianpinqi_system_records(
                start_time: Optional[str] = None,  # 起始时间，YYYY-MM-DD HH:MM:SS，可选
                end_time: Optional[str] = None  # 结束时间，YYYY-MM-DD HH:MM:SS，可选):
        ):
            """
            查询通风系统变频器系统在指定时间段内的主要参数。字段和数据结构严格依据配置，返回内容包含字段英文名及中文注释。
            对应关系:
                一号变频器 <=> 一号一级电机变频器
                二号变频器 <=> 一号二级电机变频器
                三号变频器 <=> 二号一级电机变频器
                四号变频器 <=> 二号二级电机变频器
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
                        "message": "未找到指定变频器在给定时间范围内的记录"
                    }
                - 查询成功时，返回结构化JSON，包括所有查询变频器系统的各分组字段及字段注释。

            特别提示：
                - 当查询时间范围大于一天时，仅返回统计信息与结果归档（如"top3"等）。
                - 若需更详细的数据分析，请指定更短的时间段（建议不大于1天）。

            """
            # INSERT_YOUR_CODE
            logger.info(
                f"query_bianpinqi_system_records called: start_time={start_time}, end_time={end_time}"
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
                records = self.service.print_tongfeng_today_with_cache(
                    system_name_filters=['变频器系统'],
                    start_date=query_start, end_date=query_end,
                )
                if all(not v for v in records.values()):
                    return json.dumps({"message": "未找到指定变频器在给定时间范围内的记录"}, ensure_ascii=False)
                result_json = json.dumps(records, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
                if len(result_json) > 100000:
                    records = self.json_serializer(records)
                # 返回序列化数据
                result = {
                    "result": records,
                    "tip": "如需详细数据请仅查一天或指定更短时间段。"
                }
                result_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
                logger.info(f"[query_bianpinqi_system_records] 返回的result_json长度: {len(result_json)}")
           
                return result_json

            except Exception as e:
                logger.error(f"query_bianpinqi_system_records 异常: {e}", exc_info=True)
                return json.dumps(
                    {"error": "查询失败", "message": str(e)},
                    ensure_ascii=False
                )

        @self.mcp.tool()
        def query_shebei_system_records(
                start_time: Optional[str] = None,  # 起始时间，YYYY-MM-DD HH:MM:SS，可选
                end_time: Optional[str] = None  # 结束时间，YYYY-MM-DD HH:MM:SS，可选
        ):
            """
            只能查询通风系统设定的数值：如设备电流超限报警阈值设定数值、电机定子温度超限报警阈值设定数值、 设备振动超限报警阈值设定数值、轴承温度超限报警阈值设定数值、电流信号互感器变比参数设定数值、电压信号互感器变比参数设定数值、风机风筒横截面积参数设定数值。

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
                        "message": "未找到指定设备级信息在给定时间范围内的记录"
                    }
                - 查询成功时，返回结构化JSON，包括所有查询设备级系统的各分组字段及字段注释。

            特别提示：
                - 只能查询 tongfeng_system.yaml 【设备级】部分下的数值、报警等数据。
                - 当查询时间范围大于一天时，仅返回统计信息与结果归档（如"top3"等）。
                - 若需更详细的数据分析，请指定更短的时间段（建议不大于1天）。
            """
            # INSERT_YOUR_CODE
            logger.info(
                f"query_shebei_system_records called: start_time={start_time}, end_time={end_time}"
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
                records = self.service.print_tongfeng_today_with_cache(
                    system_name_filters=['设备级'],
                    start_date=query_start, end_date=query_end,
                )
                if all(not v for v in records.values()):
                    return json.dumps({"message": "未找到指定设备级信息在给定时间范围内的记录"}, ensure_ascii=False)

                result_json = json.dumps(records, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
                if len(result_json) > 100000:
                    records = self.json_serializer(records)
                # 返回序列化数据
                result = {
                    "result": records,
                    "tip": "如需详细数据请仅查一天或指定更短时间段。"
                }
                result_json = json.dumps(result, ensure_ascii=False, separators=(",", ":"), default=json_serializer)
                logger.info(f"设备级系统记录结果 JSON 长度: {len(result_json)}")
           
                return result_json

            except Exception as e:
                logger.error(f"query_shebei_system_records 异常: {e}", exc_info=True)
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
            查通风系统功率能耗。只能查询如下设定设备的功率能耗相关数据：
                - 一号风机1级设备 (TF_YH_1_JI_GONG_LV_SHI_JI_ZHI)
                - 一号风机2级设备 (TF_YH_2_JI_GONG_LV_SHI_JI_ZHI)
                - 二号风机1级设备 (TF_EH_1_JI_GONG_LV_SHI_JI_ZHI)
                - 二号风机2级设备 (TF_EH_2_JI_GONG_LV_SHI_JI_ZHI)
            
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
                logger.info(f"query_power_energy_records 返回 JSON 长度为: {len(result_json)}")
           
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
            功能描述: 获取当前通风系统全部支持的字段列表及其分组、中文注释说明。可用于自定义查询、字段筛选、前端字段说明展示等场景。
            输入参数: 无
            返回: tongfeng_system.yaml 文件完整结构(JSON格式)，包含风机系统、设备级、变频器系统等全部分组、字段及注释等元信息。
            返回示例: {
                "fan_system": {...},
                "device_level": {...},
                "bianpinqi_system": {...},
                ...
            }
            特别说明: 本接口直接返回对象。
            """
            # INSERT_YOUR_CODE
            logger.info("get_supported_fields called, loading all supported fields from YAML configuration.")
     
            data = self.service._load_yaml()
            return json.dumps(data, ensure_ascii=False, indent=2)

        
        @self.mcp.tool()
        def generate_tongfeng_report(
                start_date: Union[str, datetime, None] = None,
                end_date: Union[str, datetime, None] = None,
        ) -> str:
            """
            【报告生成工具】根据起止日期生成通风系统综合日报 / 多日报告（Markdown 格式）。

            【使用场景】
            用户说"出一份今天的通风报告"、"汇总近3天的风机运行情况"、"生成通风系统的周报/月报"等。

            【参数说明】
            - start_date: 起始日期 (格式: "YYYY-MM-DD HH:MM:SS" 或 "YYYY-MM-DD")，缺省为当天 00:00:00。
            - end_date:   截止日期 (格式: "YYYY-MM-DD HH:MM:SS" 或 "YYYY-MM-DD")，缺省为当天 23:59:59。

            【报告内容】
            一、风机系统（一号 + 二号）：报警信号 / 状态信号 / 数值监测
                - 报警、状态：如无变化，只输出"正常"；有变化才输出变化次数与当前值
                - 数值：表格展示 平均值、中位数、标准差、最小值、最大值
            二、功率能耗：一号1级/2级、二号1级/2级设备的平均功率与总能耗
            三、其他系统：高压柜 / 阀门 / 进线柜 / 风门 / 母联柜（信号 + 控制指令）

            【返回值】
            Markdown 文本。
            - 单日报告：{报告类型=单日报告，含"日期"}
            - 多日报告：{报告类型=多日报告，含"统计周期"与"统计天数"；数值做跨天聚合}
            """
            logger.info(
                f"generate_tongfeng_report called: start_date={start_date}, end_date={end_date}"
            )
            try:
                # ========== 时间处理 ==========
                def _normalize_time(t, is_end: bool):
                    if t is None:
                        return None
                    if isinstance(t, datetime):
                        return t.strftime("%Y-%m-%d %H:%M:%S")
                    t = str(t).strip()
                    if not t:
                        return None
                    if len(t) == 10:
                        return f"{t} 23:59:59" if is_end else f"{t} 00:00:00"
                    return t

                today = datetime.now().date()
                start_time = _normalize_time(start_date, is_end=False) or f"{today} 00:00:00"
                end_time = _normalize_time(end_date, is_end=True) or f"{today} 23:59:59"

                start_dt = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
                end_dt = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")
                is_multi_day = start_dt.date() != end_dt.date()

                # ========== 拉取原始数据 ==========
                body = {}

                # 1) 风机系统
                try:
                    body["风机系统"] = self.service.print_tongfeng_today_with_cache(
                        system_name_filters=["一号风机系统", "二号风机系统"],
                        start_date=start_time,
                        end_date=end_time,
                        subgroup_filters=["报警", "状态", "数值"],
                    ) or {}
                except Exception as e:
                    logger.error(f"generate_tongfeng_report 风机查询失败: {traceback.format_exc()}")
                    body["风机系统"] = {}

                # 2) 功率能耗
                try:
                    body["功率能耗"] = self.service.calc_gonglv_energy_with_cache(
                        start_date=start_time, end_date=end_time,
                    ) or {}
                except Exception as e:
                    logger.error(f"generate_tongfeng_report 功率能耗失败: {traceback.format_exc()}")
                    body["功率能耗"] = {}

                # 3) 其他系统
                try:
                    body["其他系统"] = self.service.print_tongfeng_today_with_cache(
                        system_name_filters=[
                            "高压柜系统", "阀门系统", "进线柜系统",
                            "风门系统", "母联柜系统",
                        ],
                        start_date=start_time,
                        end_date=end_time,
                    ) or {}
                except Exception as e:
                    logger.error(f"generate_tongfeng_report 其他系统失败: {traceback.format_exc()}")
                    body["其他系统"] = {}

                # ========== 生成 Markdown ==========
                md = _build_md_report(body, is_multi_day)
                logger.info(f"generate_tongfeng_report 完成, markdown 长度: {len(md)}")
                return md

            except Exception as e:
                logger.error(f"generate_tongfeng_report 异常: {traceback.format_exc()}")
                return f"# 通风系统报告生成失败\n\n错误信息：`{e}`\n"
    # _______________________________辅助函数____________________________________
    def filter_records(self, records):
        filtered = {}
        keep = {
            '一号风机系统': {
                '数值': [
                    '一号风机动压实时监测数值',
                    '一号风机风量实时监测数值',
                    '一号风机风速实时监测数值',
                    '一号风机风量静压监测数值',
                    '一号风机风量全压监测数值',
                    '一号风机实际运行效率计算数值'
                ]
            },
            '二号风机系统': {
                '数值': [
                    '二号风机动压实时监测数值',
                    '二号风机风量实时监测数值',
                    '二号风机风速实时监测数值',
                    '二号风机风量静压监测数值',
                    '二号风机风量全压监测数值',
                    '二号风机实际运行效率计算数值'
                ]
            }
        }
        for sys_name, sys_data in records.items():
            if sys_name not in keep:
                continue
            filtered[sys_name] = {}
            for subgroup, metas in sys_data.items():
                if subgroup not in keep[sys_name]:
                    continue
                filtered[sys_name][subgroup] = {}
                for key in keep[sys_name][subgroup]:
                    if key in metas:
                        filtered[sys_name][subgroup][key] = metas[key]
        return filtered

    def json_serializer(self, records):
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
                        new_v["时间"] = new_v["时间"].strftime("%Y-%m-%d %H:%M:%S")
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
                            dval["重要变化top3"] = changes_top3
                            del dval["重要变化"]
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

        # # 2️⃣ query_fengji_records - 默认参数
        # print("\n2️⃣ 测试 query_fengji_records 默认参数")
        # res2 = await mcp_app.call_tool("query_fengji_records", {})
        # print("query_fengji_records (默认) 返回：")
        # # print(res2)
        # #
        # 3️⃣ query_fengji_records - 指定风机与分组
        # print("\n3️⃣ 测试 query_fengji_records 仅查询一号风机, 分组: '报警', 指定时间")
        # res3 = await mcp_app.call_tool(
        #     "query_fengji_records",
        #     {
        #         "choose": "all",
        #         "start_time": "2026-05-20 00:00:00",
        #         "end_time": "2026-06-12 23:59:59",
        #         # "subgroup_filters": "报警"
        #     }
        # )
        # print("query_fengji_records (一号风机-报警) 返回：")
        # # print(res3)
        # #
        # # 4️⃣ query_fengji_records - 多分组多风机、分段、批量
        # print("\n4️⃣ 测试 query_fengji_records 指定分组['状态','切换过程']，全部风机")
        # res4 = await mcp_app.call_tool(
        #     "query_fengji_records",
        #     {
        #         "choose": "all",
        #         "start_time": "2026-05-06 02:00:00",
        #         "end_time": "2026-05-06 08:00:00",
        #         "subgroup_filters": ["状态", "切换过程"]
        #     }
        # )
        # print("query_fengji_records (all/状态+切换过程) 返回：")
        # # print(res4)
        #
        # # 5️⃣ query_fengji_records - 数值分组+数值筛选
        # print("\n5️⃣ 测试 query_fengji_records 分组'数值',风量>1000")
        # res5 = await mcp_app.call_tool(
        #     "query_fengji_records",
        #     {
        #         "choose": "1",
        #         "start_time": "2026-05-06 00:00:00",
        #         "end_time": "2026-05-06 23:59:59",
        #         "subgroup_filters": "数值",
        #         "value_filters": {"风量": (">", 600),"风速": (">", 600)}
        #     }
        # )
        # print("query_fengji_records (数值/风量>600) 返回：")
        # print(res5)

        # 6️⃣ query_fengji_records - 跨天大统计
        # print("\n6️⃣ 测试 query_fengji_records 跨天查询统计")
        # res6 = await mcp_app.call_tool(
        #     "query_fengji_records",
        #     {
        #         "choose": "all",
        #         "start_time": "2026-04-04 00:00:00",
        #         "end_time": "2026-05-09 23:59:59",
        #         "subgroup_filters": ["数值", "状态"],
        #         "value_filters": {"定子温度": (">", -100)}
        #     }
        # )
        # print("query_fengji_records (二号风机 跨天 数值+状态统计) 返回：")
        # print(res6)

        # # 7️⃣ query_fengji_records - 非法字段、无命中等异常测试
        # print("\n7️⃣ 测试 query_fengji_records value_filters字段非法/数据为空")
        # res7 = await mcp_app.call_tool(
        #     "query_fengji_records",
        #     {
        #         "choose": "1",
        #         "start_time": "2022-01-01 00:00:00",
        #         "end_time": "2022-01-01 23:59:59",
        #         "subgroup_filters": "数值",
        #         "value_filters": {"TF_FAKE_FIELD": (">", 999999)}
        #     }
        # )
        # # print("query_fengji_records (非法字段/空数据) 返回：")
        # # print(res7)

        # print("\n6️⃣ 测试 query_others_system_records 跨天查询统计")
        # res6 = await mcp_app.call_tool(
        #     "query_others_system_records",
        #     {
        #         "choose": "all",
        #         "start_time": "2026-04-04 00:00:00",
        #         "end_time": "2026-05-09 23:59:59",
        #     }
        # )
        # print("query_others_system_records (二号风机 跨天 数值+状态统计) 返回：")

        # # 单日报告（今天）
        # result = await mcp_app.call_tool("generate_tongfeng_report", {
        #     "start_date": "2026-08-20",
        #     "end_date":   "2026-08-21",
        # })

        # 多日报告
        result = await mcp_app.call_tool("generate_tongfeng_report", {
            "start_date": "2026-08-30 00:00:00",
            "end_date":   "2026-09-12 23:59:59",
        })
        print(result)
        # 不传参数 → 默认当天
        # result = await mcp_app.call_tool("generate_tongfeng_report", {})
        # print(result)
        # print("\n6️⃣ 测试 query_power_energy_records 跨天查询统计")
        # res6 = await mcp_app.call_tool(
        #     "query_power_energy_records",
        #     {
        #         "start_time": "2026-04-04 00:00:00",
        #         "end_time": "2026-05-09 23:59:59",
        #     }
        # )
        # print("query_power_energy_records (二号风机 跨天 数值+状态统计) 返回：")



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

    TongfengMCPService(
        mcp=mcp_app,
        host="10.11.22.80",
        port=9120,
        user="nethouse",
        password="CGC%EVXr.ET10Y_N",
        database="PS",
    )

    asyncio.run(test_all_tools())
