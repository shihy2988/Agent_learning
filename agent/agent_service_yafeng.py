#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
矿井人员定位智能调度系统 - Gradio + FastAPI (支持流式和一次性请求)
"""

import gradio as gr
import asyncio
import re
import logging
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

from yafeng_stream import build_agent, mcp_client
import os

# ====================== 日志配置 ======================
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "agent_service.log")

logger = logging.getLogger("loggru.interact")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(levelname)s %(asctime)s %(name)s: %(message)s')

if not logger.handlers:
    # 滚动日志：单文件最大50MB，备份一个旧文件，模式为"a"（追加）
    fh = logging.handlers.RotatingFileHandler(
        log_path, mode='a', maxBytes=50 * 1024 * 1024, backupCount=1, encoding="utf-8", delay=False
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

if not logger.handlers:
    # 滚动日志：单文件最大50MB，备份一个旧文件，模式为"a"（追加）
    fh = logging.handlers.RotatingFileHandler(
        log_path, mode='a', maxBytes=50 * 1024 * 1024, backupCount=1, encoding="utf-8", delay=False
    )
    fh.setFormatter(formatter)
    logger.addHandler(fh)
    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)
# ====================== Agent 单例 ======================
agent_instance = None


async def get_agent_instance():
    global agent_instance
    if agent_instance is None:
        print("正在初始化 Agent...")
        tools = await mcp_client.get_tools()
        agent_instance = build_agent(tools)
        print(f"Agent 初始化完成，加载 {len(tools)} 个工具")
    return agent_instance


def process_thinking_process(text: str) -> str:
    """
    只处理最外层的 <think>...</think>，支持流式和完整模式
    """
    start = text.find("<think>")
    if start == -1:
        return text

    end = text.rfind("</think>")

    # 未闭合（流式进行中）
    if end == -1 or end < start:
        prefix = text[:start]
        content = text[start + len("<think>"):]
        return (
            f"{prefix}"
            f"<details open style='color: #666; border-left: 3px solid #888; padding-left: 12px; margin: 8px 0;'>"
            f"<summary>🤔 正在思考...</summary>{content}</details>"
        )

    # 已闭合（完整回复）
    prefix = text[:start]
    content = text[start + len("<think>"):end]
    suffix = text[end + len("</think>"):]

    return (
        f"{prefix}"
        f"<details style='color: #555; border-left: 3px solid #ccc; padding-left: 12px; margin: 8px 0; background: #f9f9f9;'>"
        f"<summary>📝 查看思考过程</summary>{content}</details>"
        f"{suffix}"
    )


async def generate_response(langchain_history, stream: bool = True):
    """核心生成逻辑"""
    agent = await get_agent_instance()
    full_response = "<think> "

    async for chunk, _ in agent.astream({"messages": langchain_history}, stream_mode="messages"):
        if isinstance(chunk, AIMessage) and chunk.content:
            piece = chunk.content
            if isinstance(piece, list):
                piece = "".join(p.get("text", "") if isinstance(p, dict) else str(p) for p in piece)
            full_response += piece

            if stream:
                yield process_thinking_process(full_response)

    if not stream:
        yield process_thinking_process(full_response)


# ====================== 原始 predict（供 Gradio 使用） ======================
async def predict(message: str, history, request: gr.Request = None, stream: bool = True):
    # IP 日志
    client_ip = "unknown"
    try:
        if request and hasattr(request, "client"):
            client_ip = request.client.host
    except:
        pass
    logger.info(f"[IP:{client_ip}] 用户提问: {message}")

    # ------------------ 构建 LangChain 消息历史 ------------------
    system_instruction = """
你是矿井安全生产调度与设备分析专家，熟悉井下工业系统的运行机制。使用本系统时，请依据如下准则和工具描述，科学推理、自动选择最合适的工具并可灵活组合，以获得准确结果：

【最高优先级规则 - 时间处理】
- 只要用户问题涉及**相对时间**（如今天、昨天、过去几天、上周、刚才、最近、本周、本月、“现在起三小时内”等），你必须始终**第一步调用** `get_system_time` 工具来确定服务器准确时间作为一切推算的基准。
- 严禁直接推测或手动计算时间起止，务必先获取基准时间后再推导 start_time 和 end_time。
- 工具调用时，所有包含数字类参数的字段都须转换成字符串（str）类型后再传递
- 回复时，涉及任何数值时必须带上如下对应单位：
    - 温度：℃  
    - 振动：mm/s  
    - 变频器频率：Hz  
    - 风量：m³/min  
    - 风速：m/s  
    - 压力：Pa  
    - 效率：%
- 回复时请勿直接暴露任何工具名。
- 每次回复，思考过程须以一对 <think> ... </think> 标签包裹，并置于最前。

【1. 获取系统基准时间】
- 使用 `get_system_time()` 工具获取服务器当前时间（格式：{"current_time": "YYYY-MM-DD HH:MM:SS", "weekday": "Wednesday"}）。
  所有历史、轨迹、对比等查询的时间范围建议以该结果为准。

【2. 查询空压机主要参数及历史】
- 使用 `query_kongyaji_records(choose, start_time, end_time, subgroup_filters)` 查询1、2、3号或全部空压机在指定时间段（默认今日0时至当前）的信号、指令、状态、监测值等主要参数。
    - choose: "1"|"2"|"3"|"all"（默认全部）。
    - subgroup_filters 可为 "信号"、"指令"、"状态"、"监测值"（可多选或不指定全部）。
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

【强制思考结构模版】  
请在每次回复前，先用如下结构在 <think> 标签内进行结构化思考：

<think>
1. 用户核心需求及意图：...
2. 是否涉及相对时间？（是/否）→ 若是，必须第一步调用 get_system_time
3. 应推荐的工具及输入参数：...
4. 工具调用顺序：第1步 → 第2步 → ...
5. 关键注意事项与边界说明：...
</think>

只有在结构化思考充分完毕后，才可进入工具调用和对用户的专业、条理化、简明答复。请统一使用便于阅读的 markdown 格式输出。

"""

    langchain_history = [SystemMessage(content=system_instruction)]
    for h in history or []:
        content = h.get('content', "")
        if isinstance(content, list):
            content = "".join(p.get("text", "") if isinstance(p, dict) else str(p) for p in content)
        role = h.get('role')
        if role == 'user':
            langchain_history.append(HumanMessage(content=content))
        else:
            clean = re.sub(r"<details.*?</details>", "", str(content), flags=re.DOTALL).strip()
            langchain_history.append(AIMessage(content=clean))

    langchain_history.append(HumanMessage(content=message))

    async for resp in generate_response(langchain_history, stream=stream):
        yield resp


# ====================== FastAPI 部分 ======================
fastapi_app = FastAPI(title="矿井人员定位系统 API")

fastapi_app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# 合并为单端口，通过 stream 参数控制流式/非流式响应
@fastapi_app.post("/chat")
async def chat(request: Request):
    data = await request.json()
    message = data.get("message", "")
    history = data.get("history", [])
    stream = data.get("stream", False)

    if not stream:
        # 非流式，按普通 JSON 返回
        final_result = None
        async for response in predict(message, history, stream=False):
            final_result = response
        return {"response": final_result}
    else:
        # 流式 SSE 返回
        async def event_generator():
            async for response in predict(message, history, stream=True):
                yield f"data: {response}\n\n"
            yield "data: [DONE]\n\n"

        return StreamingResponse(
            event_generator(),
            media_type="text/event-stream"
        )


# ====================== Gradio Blocks ======================
with gr.Blocks(title="矿井安全生产智能调度系统") as demo:
    gr.Markdown("""
    # 数字人 👷‍♂️🤖🦺⛑️⛏️🚜🌪️⚡  
    ## 压风系统智能专家助手

    **🚀 集成空压机/高压柜/断路器/阀门/环境/振动/运行逻辑/安全等系统全局监测与智能分析 🤓**  
    <br>

    | 🏭 空压机监测 | ⚡ 回路功率能耗 | ⚙️ 设定&报警限值 | 🛰️ 设备与系统状态 | 🚨 告警&异常事件 | 📈 历史分组统计 |
    |:-------------:|:--------------:|:---------------:|:---------------:|:----------------:|:-----------------:|
    | 1/2/3号空压机、管道、风包、压力、温度等 | 断路器/母联回路功率与因数 | 设定/报警限值、周期提醒 | 高压柜/风门/阀门/母联/电机等设备实时状态 | 报警/异常/运行/切换事件 | 灵活时间段分组、统计与趋势分析 |

    <br>

    ### 🧠 系统专家能力
    - 🏭 查询1/2/3号空压机、所有分子系统（管道/风包/断路器/电机等）的运行参数、指令、监测信号和报警历史
    - ⚡ 能耗与回路功率聚合，支持断路器/母联实时功率因数、能耗对比与趋势
    - ⚙️ 查询系统设定参数（压力区间、温度/振动限值、各类阈值）、周期维护与告警提醒全追溯
    - 🛰️ 实时获取高压/低压柜、风门、阀门、母联柜、进线柜、电机等关键设备与系统运行状态
    - 🚨 组合筛选不同报警、告警、维修/切换/控制等异常或指定逻辑事件
    - 📈 针对各维度、多分组关键指标进行时段统计、归档、趋势和波动追溯

    <br>

    #### 🗂️ 主要分系统能力对照
    - **空压机系统**：运行/报警/温压/振动/能耗/控制信号及历史追溯  
    - **高压柜/断路器系统**：开关状态、回路功率、电流、事件报警  
    - **风门/阀门系统**：启停、切换、阀位与异常监测  
    - **环境与安全系统**：烟雾/温度/湿度/振动/报警、设定与历史趋势  
    - **逻辑控制系统**：多机逻辑切换、运行/停止逻辑、控制事件追踪  
    - **统计分析**：指定日/时/分区段、多指标对比、历史波动、报警频次/能耗排行

    <br>
    """)

    gr.ChatInterface(
        fn=predict,
        chatbot=gr.Chatbot(
            height=700,
        ),

        textbox=gr.Textbox(
            placeholder="例如：查询今天一号风机报警情况...",
            lines=1,
        
        ),

        examples=[
            ["查询今天1号空压机的报警记录"],
            ["查询最近2小时2号空压机的运行状态"],
            ["查询今天全部空压机的控制指令和切换过程"],
            ["查询风速大于5的所有空压机今日数据"],
            ["查询最近24小时变频器的运行情况"],
            ["查询高压柜当前实时状态"],
            ["查询风门系统今天的全部数据"],
            ["查询母联柜最近1小时的实时参数变化"],
            ["查询设备级参数设定值历史记录"],
            ["查询今天压风系统的全部运行情况"],
            ["统计最近三天空压机报警次数"],
            ["分析今天1号空压机是否存在异常波动"]
       
        ],

        submit_btn="发送",
        stop_btn="停止",

        api_name="chat",
    )
# ====================== 启动 ======================
if __name__ == "__main__":
    # 同时启动 Gradio + FastAPI（推荐方式）
    app = gr.mount_gradio_app(fastapi_app, demo, path="/web")

    uvicorn.run(
        app,
        host="10.11.3.210",
        port=7863,
        log_level="info"
    )