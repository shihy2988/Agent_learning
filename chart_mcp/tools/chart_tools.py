# -*- coding: utf-8 -*-
"""
============================================================
文件名: chart_tools.py
功能描述: MCP 图表工具注册层
============================================================

本模块负责将图表生成能力注册为 MCP Tools，
供所有子系统的大模型 Agent 通过 MCP 协议调用。

关于 Prompt 的设计思路：
  图表的 Prompt（图表选择策略、什么时候画什么图）不能写在各子系统的 agent 代码里，
  因为那样每个子系统都得写一份。也不能写在 agent 通用的 system prompt 里，
  因为 agent 是通用的。最好的位置就是写在 MCP 工具的 docstring 和 Prompt 模板中。
  大模型在调用 MCP 工具前会读取工具的 description（docstring），
  这些描述本身就构成了"图表选择指南"。

  同时我们还提供了一个 `chart_decision_guide` prompt，
  Agent 可以在需要时主动调用来获取更详细的图表选择策略。

注册的工具:
  1. generate_chart    - 生成图表（PNG base64）
  2. get_chart_types   - 获取支持的图表类型

注册的资源:
  - chart://guide      - 图表工具使用指南

注册的 prompts:
  - chart_decision_guide - 图表选择决策指南（核心！）
"""

import json
import logging
import os
import sys
import traceback
import urllib3
from datetime import datetime
from typing import Optional

from fastmcp import FastMCP

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.chart_engine import (
    generate_chart_with_file,
    generate_plotly_chart,
    get_supported_chart_types,
    SUPPORTED_CHART_TYPES,
    CHART_TYPE_MAP,
    _PLOTLY_AVAILABLE,
)

# ====================================================================
# 日志配置
# ====================================================================
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

from logging.handlers import RotatingFileHandler

LOG_DIR = "logs"
LOG_FILE = os.path.join(LOG_DIR, "chart_mcp_service.log")

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

file_handler = RotatingFileHandler(
    LOG_FILE, maxBytes=50 * 1024 * 1024, backupCount=5, encoding="utf-8")
file_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
file_handler.setFormatter(file_formatter)

console_handler = logging.StreamHandler()
console_handler.setFormatter(file_formatter)

root_logger = logging.getLogger()
root_logger.setLevel(logging.INFO)
root_logger.handlers = []
root_logger.addHandler(file_handler)
root_logger.addHandler(console_handler)

logger = logging.getLogger("ChartMCPService")


# ====================================================================
# ChartMCPService 类
# ====================================================================
class ChartMCPService:
    """
    图表 MCP 服务主类。

    职责:
      1. 初始化图表引擎
      2. 注册 MCP Resources / Prompts / Tools
    """

    def __init__(self, mcp: FastMCP):
        self.mcp = mcp
        self.start_time = datetime.now()
        self.total_charts = 0
        self.total_errors = 0

        # 注册 MCP 组件
        self._register_resources()
        self._register_prompts()
        self._register_tools()

        logger.info("ChartMCPService 初始化完成，所有图表工具已注册。")
        logger.info(f"Plotly 可用: {_PLOTLY_AVAILABLE}")
        logger.info(f"支持的图表类型: {', '.join(SUPPORTED_CHART_TYPES)}")

    # ==================== Resources ====================

    def _register_resources(self):
        @self.mcp.resource("chart://guide")
        def get_chart_guide() -> str:
            """获取图表工具使用指南"""
            chart_list = "\n".join([f"  · {k:12s} → {v}" for k, v in CHART_TYPE_MAP.items()])
            return f"""
# 图表生成工具使用指南

本 MCP 服务提供 11 种可视化图表类型，供大模型 Agent 调用。

## 支持的图表类型
{chart_list}

## 使用方法
1. 先通过各子系统的数据查询工具获取数据
2. 从查询结果中提取需要可视化的字段
3. 根据数据特点选择最合适的 chart_type
4. 将数据组织成对应格式的 JSON，调用 generate_chart 工具

## 引擎
- 默认: matplotlib（PNG 静态图片，返回 base64 编码）
- 可选: plotly（HTML 交互式图表，需安装 plotly 包）
"""

    # ==================== Prompts ====================

    def _register_prompts(self):
        @self.mcp.prompt()
        def chart_decision_guide() -> str:
            """
            图表选择决策指南 Prompt。

            **这是核心 Prompt**。当 Agent 拿到数据后，
            不确定是否应该画图、画什么类型的图时，
            可以调用此 prompt 获取决策指导。

            这个 prompt 写在 chart_mcp 中而非各子系统的 agent 里，
            是因为图表决策逻辑是通用的，所有子系统共享。
            """
            return """
# 图表选择决策指南

你是煤矿安全监测系统的数据分析助手。当你从数据查询工具拿到数据后，
请按以下策略决定是否画图、画什么图：

## 第一步：判断是否需要画图

以下情况**需要画图**：
- 用户明确要求看图表、趋势、对比、分布
- 数据有时间维度（多天/多时段），且用户关心变化趋势
- 需要对比多个设备/指标/时间段
- 需要展示占比、构成、分布
- 数据量较大，纯文字难以清晰表达

以下情况**不需要画图**：
- 简单的是/否问题、状态查询
- 只有一个时间点的单值查询
- 用户只要求文字解释/分析，未要求可视化
- 数据为空或数据量太少（<3个数据点）

## 第二步：选择合适的图表类型

根据数据特征选择 chart_type：

| 数据特征 | 推荐 chart_type | 示例场景 |
|---------|:---:|---------|
| 时序变化（温度/风量/振动/电流等随时间变化） | trend | 风机7天风量趋势 |
| 不同类别/设备指标对比 | bar | 一二号风机风量对比 |
| 分布占比（报警类型、状态比例） | pie | 报警类型分布饼图 |
| 两变量相关性分析 | scatter | 振动-温度关系 |
| 多天多时段矩阵数据 | heatmap | 时段-日期风量热力图 |
| 数据分布统计（离散度、异常值） | boxplot | 多设备参数分布对比 |
| 多指标堆叠趋势（累积关系） | area | 能耗堆叠构成 |
| 单个实时数值展示 | gauge | 当前风量仪表盘 |
| 不同量纲指标同时间轴对比 | dual_axis | 风量+温度同图 |
| 多维综合评估 | radar | 设备性能雷达图 |
| 能耗日趋势 | energy | 每日功率能耗 |

## 第三步：组织数据格式

确定 chart_type 后，将查询结果数据组织成对应的 JSON 格式。
具体格式请参考 generate_chart 工具的 data_json 参数说明。

## 第四步：调用 generate_chart 工具

调用 `generate_chart(chart_type, title, data_json)` 生成图表。
工具会返回 {"success": true, "file_path": ![图片描述](图片URL)}。
在回复中使用 file_path 图片。

## 注意事项
- 图表数据必须来自实际查询结果，禁止编造数据
- data_json 必须使用双引号，确保是合法 JSON
- 先查询数据再画图，不要凭空想象数据
- 图表标题应清晰描述内容，让用户一眼看懂
## 当生成图表时：
1. 使用Markdown图片格式
2. 图片地址直接输出
3. 不要转义URL
4. 格式：
![图片描述](图片URL)
"""

    # ==================== Tools ====================

    def _register_tools(self):
        self._register_generate_chart()
        self._register_get_chart_types()

    def _register_generate_chart(self):
        @self.mcp.tool()
        def generate_chart(
            chart_type: str,
            title: str,
            data_json: str,
            figsize_width: Optional[int] = None,
            figsize_height: Optional[int] = None,
            dpi: Optional[int] = 80,
        ) -> str:
            """
            功能描述: 根据数据生成可视化图表（PNG 图片的 base64 编码）。
            支持 11 种图表类型，适用于趋势分析、对比统计、分布展示、相关性分析、综合评估等场景。

            参数说明：
                - chart_type: 图表类型，必填。可选值及适用场景:
                    · "trend"     : 时序趋势折线图 —— 单/多指标随时间变化趋势（如风量、温度、压力趋势）
                    · "bar"       : 多指标分组柱状图 —— 不同设备/维度对比（如风机风量对比、统计值对比）
                    · "pie"       : 饼图/环形图 —— 占比分布（如报警类型分布、状态占比、能耗构成）
                    · "scatter"   : 散点图 —— 两变量相关性分析（如振动-温度关系、风量-功率关系）
                    · "heatmap"   : 热力图 —— 二维数据密度分布（如多天多时段矩阵、设备-指标交叉分析）
                    · "boxplot"   : 箱线图 —— 数据分布统计含中位数/四分位数/异常值（如多设备参数分布对比）
                    · "area"      : 面积图 —— 堆叠趋势展示累积关系（如能耗堆叠构成、产量分层）
                    · "gauge"     : 仪表盘 —— 单值实时展示含阈值区间（如当前风量、温度监控）
                    · "dual_axis" : 双Y轴图 —— 不同量纲指标同时间轴对比（如风量+温度同图）
                    · "radar"     : 雷达图 —— 多维指标综合评估（如设备性能多维度评分）
                    · "energy"    : 能耗统计图 —— 日能耗趋势柱状图+趋势线

                - title: 图表标题，必填。如 "一号风机风量趋势图"、"振动-温度相关性分析"。

                - data_json: 图表数据 JSON 字符串，必填。不同图表类型的格式如下：

                    【trend 趋势折线图】:
                    {
                        "x_labels": ["2026-05-01", "2026-05-02", "2026-05-03"],
                        "series_list": [
                            {"name": "一号风机风量", "data": [1200, 1350, 1180], "unit": "m³/min"},
                            {"name": "二号风机风量", "data": [1150, 1280, 1220], "unit": "m³/min"}
                        ],
                        "y_label": "风量 (m³/min)"
                    }

                    【bar 柱状图】:
                    {
                        "categories": ["一号风机", "二号风机"],
                        "series_list": [
                            {"name": "平均风量", "data": [1350, 1280], "unit": "m³/min"},
                            {"name": "最大风量", "data": [1520, 1410], "unit": "m³/min"}
                        ],
                        "y_label": "风量 (m³/min)",
                        "horizontal": false
                    }

                    【pie 饼图】:
                    {
                        "labels": ["停机报警", "温度报警", "振动报警"],
                        "values": [3, 8, 5],
                        "donut": false
                    }

                    【scatter 散点图】:
                    {
                        "series_list": [
                            {"name": "一号风机", "x": [25,28,32,35,40,42], "y": [1.2,1.5,2.1,2.8,3.5,4.0]}
                        ],
                        "x_label": "温度 (°C)",
                        "y_label": "振动 (mm/s)",
                        "show_trendline": true
                    }

                    【heatmap 热力图】:
                    {
                        "row_labels": ["00:00","04:00","08:00","12:00","16:00","20:00"],
                        "col_labels": ["05-01","05-02","05-03","05-04","05-05"],
                        "data_matrix": [[1200,1180,1250,1220,1190],[1150,1130,1200,1180,1140],
                                        [1350,1320,1400,1380,1330],[1420,1400,1480,1450,1410],
                                        [1380,1360,1420,1400,1370],[1250,1220,1300,1280,1240]],
                        "x_label": "日期", "y_label": "时段", "color_scheme": "coolwarm"
                    }

                    【boxplot 箱线图】:
                    {
                        "series_list": [
                            {"name": "一号风机", "data": [1200,1350,1180,1420,1380,1250,1300]},
                            {"name": "二号风机", "data": [1150,1280,1220,1350,1300,1200,1250]}
                        ],
                        "y_label": "风量 (m³/min)", "horizontal": false
                    }

                    【area 面积图】:
                    {
                        "x_labels": ["05-01","05-02","05-03","05-04","05-05"],
                        "series_list": [
                            {"name": "风机能耗", "data": [800,820,790,850,830], "unit": "kWh"},
                            {"name": "水泵能耗", "data": [400,380,420,410,390], "unit": "kWh"}
                        ],
                        "y_label": "能耗 (kWh)", "stacked": true, "alpha": 0.6
                    }

                    【gauge 仪表盘】:
                    {
                        "value": 78.5, "min_val": 0, "max_val": 100, "unit": "m³/min",
                        "thresholds": [
                            {"label":"正常","range":[0,60],"color":"#4CAF50"},
                            {"label":"注意","range":[60,85],"color":"#FF9800"},
                            {"label":"报警","range":[85,100],"color":"#FF5722"}
                        ]
                    }

                    【dual_axis 双Y轴图】:
                    {
                        "x_labels": ["05-01","05-02","05-03","05-04","05-05"],
                        "left_series_list": [
                            {"name":"风量","data":[1200,1350,1180,1420,1380],"unit":"m³/min"}
                        ],
                        "right_series_list": [
                            {"name":"温度","data":[25,28,26,32,30],"unit":"°C"}
                        ],
                        "left_y_label": "风量 (m³/min)", "right_y_label": "温度 (°C)"
                    }

                    【radar 雷达图】:
                    {
                        "categories": ["效率","风量","压力","温度","振动"],
                        "series_list": [
                            {"name":"一号风机","data":[85,90,78,72,88]},
                            {"name":"二号风机","data":[80,85,82,75,82]}
                        ],
                        "fill": true
                    }

                    【energy 能耗图】:
                    {
                        "daily_labels": ["2026-05-01","2026-05-02","2026-05-03"],
                        "daily_values": [1250.5, 1180.3, 1320.8],
                        "unit": "kWh"
                    }

                - figsize_width: 图片宽度（英寸），可选。不填则使用默认尺寸。
                - figsize_height: 图片高度（英寸），可选。不填则使用默认尺寸。
                - dpi: 分辨率，可选。默认 80。

            返回格式:
                {
                    "success": true,

                    "file_path": "/path/to/chart_xxx.png",

                }

            图表类型选择建议:
                - 看趋势变化 → trend（折线图）或 area（面积图）
                - 看对比排名 → bar（柱状图）
                - 看占比构成 → pie（饼图）
                - 看两个变量关系 → scatter（散点图）
                - 看矩阵密度分布 → heatmap（热力图）
                - 看数据离散程度 → boxplot（箱线图）
                - 看单个实时数值 → gauge（仪表盘）
                - 不同量纲指标同图 → dual_axis（双Y轴图）
                - 多维综合评估 → radar（雷达图）
                - 能耗日趋势 → energy（能耗图）

            使用流程:
                ① 先调用数据查询工具获取数据
                ② 从返回的 JSON 中提取需要可视化的字段数据
                ③ 根据数据特点和分析意图，选择最合适的 chart_type
                ④ 组织成对应 chart_type 的 data_json 格式调用本工具
                ⑤ 在回复中使用返回的 chart_base64 嵌入图片
                **重要**: 图表数据必须来自实际查询结果，禁止编造数据
            """
            logger.info(f"[generate_chart] chart_type={chart_type}, title={title}")

            try:
                data = json.loads(data_json)

                figsize = None
                if figsize_width is not None and figsize_height is not None:
                    figsize = (figsize_width, figsize_height)

                chart_result = generate_chart_with_file(
                    chart_type=chart_type, title=title, data=data,
                    figsize=figsize, dpi=dpi or 80,
                )
                path = chart_result["file_path"]
                result = {
                    "success": True,
                    # "chart_base64": chart_result["chart_base64"],
                    # "chart_type": chart_type,
                    # "title": title,
                    # "format": "png",
                    "file_path": f"![{title}]({path})"
                    # "usage": f"在回复中引用: <img src='data:image/png;base64,{chart_result['chart_base64'][:50]}...' /> 或引用文件路径 {chart_result['file_path']}",
                }

                self.total_charts += 1
                logger.info(f"[generate_chart] 成功, file_path={chart_result['file_path']}, base64长度={len(chart_result['chart_base64'])}")
                return json.dumps(result, ensure_ascii=False)

            except json.JSONDecodeError as e:
                logger.error(f"[generate_chart] JSON 解析失败: {e}")
                self.total_errors += 1
                return json.dumps({"success": False, "error": f"data_json 不是合法的 JSON 格式: {e}",
                                   "message": "请检查 JSON 字符串是否使用双引号，确保格式正确"}, ensure_ascii=False)

            except ValueError as e:
                logger.error(f"[generate_chart] 参数错误: {e}")
                self.total_errors += 1
                return json.dumps({"success": False, "error": str(e),
                                   "supported_types": SUPPORTED_CHART_TYPES}, ensure_ascii=False)

            except Exception as e:
                logger.error(f"[generate_chart] 异常: {e}\n{traceback.format_exc()}")
                self.total_errors += 1
                return json.dumps({"success": False, "error": f"图表生成失败: {e}"}, ensure_ascii=False)

    def _register_get_chart_types(self):
        @self.mcp.tool()
        def get_chart_types() -> str:
            """
            获取所有支持的图表类型及其说明。

            调用此工具可以查看当前可用的图表类型列表、每种类型的适用场景和数据格式要求。
            当你不确定该选什么图表类型时，可以先调用此工具了解可选方案。

            Returns:
                JSON 格式的图表类型列表和说明
            """
            info = get_supported_chart_types()
            return json.dumps(info, ensure_ascii=False, indent=2)
