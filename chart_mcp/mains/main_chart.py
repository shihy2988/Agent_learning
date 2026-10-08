# -*- coding: utf-8 -*-
"""
============================================================
文件名: main_chart.py
功能描述: MCP 图表服务 HTTP 入口文件
============================================================

本文件是整个图表 MCP 服务的启动入口。它完成以下工作：
1. 初始化 FastMCP 服务器实例
2. 实例化 ChartMCPService（自动注册所有 MCP Tools/Resources/Prompts）
3. 启动 HTTP 传输模式的 MCP 服务

启动后，MCP 服务会监听在端口 9900，等待大模型 Agent
通过 HTTP JSON-RPC 协议调用图表生成工具。

与其他子系统 MCP 的集成方式：
  在 Agent 端的 MultiServerMCPClient 配置中添加:
  "chart": {"transport": "http", "url": "http://127.0.0.1:9900/mcp"}
"""

from fastmcp import FastMCP
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.chart_tools import ChartMCPService

mcp = FastMCP(
    "mcp_chart_service",
    "通用图表生成 MCP 服务 - 提供 11 种可视化图表类型（trend/bar/pie/scatter/heatmap/"
    "boxplot/area/gauge/dual_axis/radar/energy），支持 matplotlib(PNG) + plotly(HTML) 双引擎。"
)

ChartMCPService(mcp=mcp)

if __name__ == "__main__":
    mcp.run(transport="http", port=9900, host="0.0.0.0")
