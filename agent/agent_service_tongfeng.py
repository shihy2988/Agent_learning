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

from tongfeng_stream import build_agent, mcp_client
import os

# ====================== 日志配置 ======================
import logging.handlers

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
你是一名资深的**矿井安全生产智能调度专家**，与用户沟通时请严格遵循以下规范：

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


【工具优先级与使用说明】
1. 查询风机运行数据  
   - 若需获取一号、二号或全部风机在指定时间段内的报警、控制指令、状态、切换过程、修正系数、数值等主要参数，可调用 `query_fengji_records(choose, start_time, end_time, subgroup_filters, value_filters)`。
   - 其中 choose 可选 "1"（仅一号风机）、"2"（仅二号风机）、"all"（全部，默认），start_time/end_time 支持 "YYYY-MM-DD HH:MM:SS" 格式，subgroup_filters 可选分组如"报警"、"控制指令"、"状态"、"切换过程"、"数值"。
   - value_filters 仅当 subgroup_filters 包含 "数值" 时可用，例如 {"风速": (">", -100)}，用于定子温度、轴温度、电流、电压、振动、功率、风速、静压、全压、效率、风量、动压等字段的条件过滤。

2. 获取系统基准时间  
   - 需要用于确定查询时段或对外展示服务当前时刻，可调用 `get_system_time()` 获取服务器时间，格式为 {"current_time": "YYYY-MM-DD HH:MM:SS", "weekday": "Monday"}。

3. 查询其他系统级数据  
   - 若需查询高压柜、阀门、进线柜、风门、母联柜等非风机类系统的主要参数，可使用 `query_others_system_records(choose, start_time, end_time)` 工具。
   - 其中 choose 可选 "system"（系统级别）、"gaoyagui"（高压柜）、"fameng"（阀门）、"jinxiangui"（进线柜）、"fengmen"（风门）、"mulian"（母联柜），也可为这些系统的字符串数组，或用 "all" 查询全部支持的系统。
   - start_time/end_time 支持 "YYYY-MM-DD HH:MM:SS" 格式，未指定时默认今日0时至现在。

4. 查询设备级参数/历史数据  
   - 获取设备级别的设定参数、报警限定、数值设定等，请使用 `query_shebei_system_records(start_time, end_time)` 工具。支持跨天数据统计及设备参数变动查询。

5. 查询变频器系统数据  
   - 若需查询变频器系统在指定时间段内的主要参数，可调用 `query_bianpinqi_system_records(start_time, end_time)` 工具。
   - start_time/end_time 支持 "YYYY-MM-DD HH:MM:SS" 格式，未指定时默认今日0时至现在。查询时间范围大于一天时，仅返回统计信息与归档结果，需要更详细数据请缩短时间段。

6. 查询系统支持的全部字段、分组与注释  
   - 若需获取当前系统支持的全部字段列表、分组及中文注释说明，可调用 `get_supported_fields()` 工具。
   - 该方法无需输入参数，返回 tongfeng_system.yaml 文件的完整结构（JSON格式），包括风机系统、设备级、变频器系统等所有分组、字段及其注释等元信息。
   - 返回示例: 
     {
       "fan_system": {...},
       "device_level": {...},
       "bianpinqi_system": {...},
       ...
     }
   - 特别说明: 本工具直接返回完整对象，用于自定义查询、字段筛选及前端字段说明等需求。

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
    ## 矿井安全生产智能调度专家系统

    **🚀 通风系统安全生产 一体化智能助手 🤓**  
    <br>

    | 🌪️ 风机运行监测 | ⚡ 变频器分析 | 🧰 设备参数 | 🏭 系统状态 | 🚨 报警分析 | 📈 历史趋势 |
    |:--------------:|:-----------:|:----------:|:----------:|:----------:|:-----------:|
    | 一号/二号风机 | 频率/电流/电压 | 参数设定 | 高压柜/风门 | 异常告警 | 跨时间统计 |

    <br>

    ### 🧠 系统能力
    - 🌪️ 查询主通风机报警、状态、控制指令、切换过程、风量、风速、电流、电压等运行数据
    - ⚡ 查询变频器运行状态、频率、电压、电流等关键参数
    - 🏭 查询高压柜、风门、阀门、母联柜、进线柜等系统级数据
    - 🧰 查询设备级设定参数、报警限定值、历史变动情况
    - 📈 支持指定时间段统计分析、异常筛选、趋势分析
    - 🚨 支持多条件组合查询与智能诊断分析

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
            ["查询今天一号风机的报警记录"],
            ["查询二号风机最近2小时的运行状态"],
            ["查询今天全部风机的控制指令和切换过程"],
            ["查询今天风速大于 5 的风机数据"],
            ["查询最近24小时变频器运行情况"],
            ["查询高压柜系统当前状态"],
            ["查询风门系统今天的数据"],
            ["查询母联柜最近1小时的参数变化"],
            ["查询设备级参数设定历史"],
            ["查询今天全部系统运行情况"],
            ["统计最近三天风机报警次数"],
            ["分析今天一号风机是否存在异常波动"]
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