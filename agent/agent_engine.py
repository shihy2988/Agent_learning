#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
矿井人员定位智能调度系统 - 生产级版本（FastAPI + Gradio + 流式 + 并发控制）
"""

import gradio as gr
import asyncio
import re
import logging
import json
import os

from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from demo_stream import build_agent, mcp_client

# ====================== 全局控制 ======================
MAX_CONCURRENT = 10
semaphore = asyncio.Semaphore(MAX_CONCURRENT)

# ====================== 日志 ======================
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "agent.log")

logger = logging.getLogger("agent")
logger.setLevel(logging.INFO)

formatter = logging.Formatter('%(levelname)s %(asctime)s: %(message)s')

if not logger.handlers:
    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    sh = logging.StreamHandler()
    sh.setFormatter(formatter)
    logger.addHandler(sh)

# ====================== 思考处理 ======================
def process_thinking_process(text: str) -> str:
    start = text.find("<think>")
    if start == -1:
        return text

    end = text.rfind("</think>")

    if end == -1 or end < start:
        prefix = text[:start]
        content = text[start + len("<think>"):]
        return (
            f"{prefix}"
            f"<details open style='color:#666;border-left:3px solid #888;padding-left:10px;'>"
            f"<summary>🤔 思考中...</summary>{content}</details>"
        )

    prefix = text[:start]
    content = text[start + len("<think>"):end]
    suffix = text[end + len("</think>"):]

    return (
        f"{prefix}"
        f"<details style='color:#555;border-left:3px solid #ccc;padding-left:10px;'>"
        f"<summary>📝 思考过程</summary>{content}</details>"
        f"{suffix}"
    )

# ====================== 核心生成 ======================
async def generate_response(langchain_history, stream=True):
    async with semaphore:  # ✅ 并发控制

        # 👉 每个请求独立 Agent（防串话）
        tools = await mcp_client.get_tools()
        agent = build_agent(tools)

        full_response = "<think> "

        try:
            async for chunk, _ in agent.astream(
                {"messages": langchain_history},
                stream_mode="messages"
            ):
                if isinstance(chunk, AIMessage) and chunk.content:
                    piece = chunk.content

                    if isinstance(piece, list):
                        piece = "".join(
                            p.get("text", "") if isinstance(p, dict) else str(p)
                            for p in piece
                        )

                    full_response += piece

                    if stream:
                        yield process_thinking_process(full_response)

        except Exception as e:
            logger.error(f"生成异常: {e}")
            yield f"<think>系统异常</think> ❌ {str(e)}"

        if not stream:
            yield process_thinking_process(full_response)

# ====================== 统一预测接口 ======================
async def predict(message: str, history, stream=True):
    logger.info(f"用户提问: {message}")

    system_instruction = """
你是一名资深的**矿井安全生产智能调度专家**，与用户沟通时请严格遵循以下规范：

【最高优先级规则 - 时间处理】
- 只要用户问题涉及**相对时间**（如今天、昨天、过去几天、上周、刚才、最近、本周、本月、“现在起三小时内”等），你必须始终**第一步调用** `get_system_time` 工具来确定服务器准确时间作为一切推算的基准。
- 严禁直接推测或手动计算时间起止，务必先获取基准时间后再推导 start_time 和 end_time。
- 计算人员在井时长时，需判断其出矿状态。如已出矿，出矿时间即为其最后一条有效记录时间。
- 判断时 如果涉及时间范围（如“过去一周”），需要将这一周内的每一天分别归类统计，再进行汇总总结
- 工具调用时，所有包含数字类参数的字段都须转换成字符串（str）类型后再传递
- 回复时请勿直接暴露任何工具名。
- 每次回复，思考过程须以一对 <think> ... </think> 标签包裹，并置于最前。

【工具优先级与使用说明】
1. 查询当前井下实时人数及各区域人员分布  
   - 推荐工具：`query_person_underground_status(now_only=True)`
2. 查询今日井下所有人员进出情况  
   - 推荐工具：`query_person_underground_status(now_only=False)`
3. 获取某个具体人员的详细轨迹与分段停留信息  
   - 推荐工具：`query_person_trajectory(name, start_time, end_time)`
4. 多条件筛选人员（如姓名、区域、工种、班次、部门、时间等）并获取明细  
   - 推荐工具：`query_personnel_list(names, areas, work_types, class_names, departments, start_time, end_time)`
5. 查询某人最近一次入井记录  
   - 推荐工具：`find_person_latest_entry(name)`
6. 获取服务器当前基准时间（每次涉及相对时间都必须优先调用）  
   - 推荐工具：`get_system_time`
7. 查询并获取基础数据字典或字段说明（如CLASSTIMENAME、DUTYNAME、AREANAME等释义）  
   - 推荐工具：`get_data_dictionary()`
8. 查询全矿各类基础名录（包括所有人员、车辆、工种、部门、区域、基站等信息）  
   - 推荐工具：`get_infos(type, name)`（type可为department/person/car/worktype/area_limit/station，name为可选模糊名）
9. 查询当前井下车辆实时分布或今日井下所有车辆统计  
   - 推荐工具：`query_car_underground_status(now_only=True/False)`
10. 分析某辆车在一天或指定时段的行驶轨迹与分段详情  
    - 推荐工具：`query_car_trajectory(cardID, start_time, end_time)` 
11. 批量筛选车辆（支持车辆ID、名称、类型、部门、区域、电量、时间区间等多维过滤）并获取属性与进出明细  
    - 推荐工具：`query_cars_list(cardids, car_names, car_types, departments, area_names, electricitys, start_time, end_time)`
12. 查询某一站点附近一定距离内的实时人员情况  
    - 推荐工具：`query_person_near_station(station_name, near_distance=50)`（支持模糊站点名，返回距离/属性等细节）

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
        role = h.get('role')

        if isinstance(content, list):
            content = "".join(
                p.get("text", "") if isinstance(p, dict) else str(p)
                for p in content
            )

        if role == 'user':
            langchain_history.append(HumanMessage(content=content))
        else:
            clean = re.sub(r"<details.*?</details>", "", str(content), flags=re.DOTALL)
            langchain_history.append(AIMessage(content=clean.strip()))

    langchain_history.append(HumanMessage(content=message))

    async for resp in generate_response(langchain_history, stream):
        yield resp

# ====================== FastAPI ======================
app = FastAPI(title="矿井调度API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/chat")
async def chat(request: Request):

    # ✅ JSON保护
    try:
        data = await request.json()
    except Exception:
        raise HTTPException(400, "Invalid JSON")

    message = data.get("message", "")
    history = data.get("history", [])
    stream = data.get("stream", False)

    # ========= 非流式 =========
    if not stream:
        final = None
        async for r in predict(message, history, stream=False):
            final = r
        return {"response": final}

    # ========= 流式 =========
    async def event_generator():
        try:
            async for r in predict(message, history, stream=True):

                # ✅ 客户端断开检测（解决卡死）
                if await request.is_disconnected():
                    logger.info("客户端断开")
                    break

                yield f"data: {json.dumps(r, ensure_ascii=False)}\n\n"

        except Exception as e:
            yield f"data: {json.dumps({'error': str(e)})}\n\n"

        finally:
            yield "data: [DONE]\n\n"

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
        }
    )

# ====================== Gradio ======================
with gr.Blocks(title="矿井人员定位智能调度系统") as demo:
    gr.Markdown("""
    # 数字人 👷‍♂️🤖🦺⛑️⛏️🚜🚦 矿井人员车辆定位智能调度查询

    **🚀 安全生产调度专家助手 🤓**  
    <br>
    | 👷‍♂️ 实时井下人数  | 🧑‍🤝‍🧑 人员轨迹  | 🚜 井下车辆  | 🛣️ 车辆轨迹  | 🗺️ 区域分布  | 🔍 多条件查询 |
    |:----------------:|:-------------:|:----------:|:------------:|:-----------:|:-------------:|
    | 统计 👥         | 查询 🕵️      | 监控 🚧    | 跟踪 📍     | 分布 🗾    | 组合筛选 🛠️  |
    ---
    🏭 本系统助力矿井安全高效运转，支持多种调度查询功能。  
    快来尝试输入你的问题吧，例如：  
    “”现在井下有多少人？哪辆车最近进入了三号巷？王小明今天的轨迹？“”
    """)

    gr.ChatInterface(
        fn=predict,
        chatbot=gr.Chatbot(height=680),
        textbox=gr.Textbox(placeholder="例如：现在井下有多少人？...", lines=1),
        examples=[...],   # 保留你的 examples
        submit_btn="发送",
        
    )


app = gr.mount_gradio_app(app, demo, path="/gradio")

# ====================== 启动 ======================
if __name__ == "__main__":
    uvicorn.run(
        app,
        host="172.31.254.186",
        port=7862,
        workers=2,               # ✅ 多进程
        limit_concurrency=20,    # ✅ 最大并发
        timeout_keep_alive=5,
        log_level="info"
    )