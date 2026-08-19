#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文件名:    agent_gradio.py
作者:      shihy (优化版)
创建日期:  2026-04-28
描述:      矿井人员定位系统 - Gradio Chat 界面（支持流式 + 一次性请求）
"""

import gradio as gr
import asyncio
import re
import logging
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

from demo_stream import build_agent, mcp_client

# ====================== 日志初始化 ======================
import os

log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "loggru_interact.log")

logger = logging.getLogger("loggru.interact")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(levelname)s %(asctime)s %(name)s: %(message)s')

if not logger.handlers:
    fhandler = logging.FileHandler(log_path, encoding="utf-8")
    fhandler.setFormatter(formatter)
    logger.addHandler(fhandler)

    shandler = logging.StreamHandler()
    shandler.setFormatter(formatter)
    logger.addHandler(shandler)

# ====================== 全局 Agent 单例 ======================
agent_instance = None


async def get_agent_instance():
    """单例模式获取 Agent 实例"""
    global agent_instance
    if agent_instance is None:
        print("正在初始化 Agent 并加载工具...")
        tools = await mcp_client.get_tools()
        agent_instance = build_agent(tools)
        print(f"Agent 初始化完成，共加载 {len(tools)} 个工具")
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


# ====================== 核心生成逻辑（可复用） ======================
async def generate_response(langchain_history, stream: bool = True):
    """
    核心回复生成函数
    - stream=True  → 流式返回（适合 Gradio 界面）
    - stream=False → 一次性返回完整内容（适合 API 调用）
    """
    agent = await get_agent_instance()
    full_response = "<think> "

    try:
        async for chunk, metadata in agent.astream(
            {"messages": langchain_history},
            stream_mode="messages",
        ):
            if isinstance(chunk, AIMessage) and chunk.content:
                content_piece = chunk.content

                # 处理可能的多模态 list 内容
                if isinstance(content_piece, list):
                    content_piece = "".join([
                        p.get("text", "") if isinstance(p, dict) else str(p)
                        for p in content_piece
                    ])

                full_response += content_piece

                if stream:
                    # 流式模式：实时输出
                    yield process_thinking_process(full_response)

        # 非流式模式：最后一次性返回完整内容
        if not stream:
            yield process_thinking_process(full_response)

    except Exception as e:
        logger.error(f"生成回复时发生错误: {e}", exc_info=True)
        error_msg = f"<think>工具调用或生成过程中发生错误: {str(e)}</think>\n\n抱歉，系统处理时出现异常，请稍后重试。"
        yield error_msg


# ====================== 聊天主函数 ======================
async def predict(
    message: str,
    history,
    request: gr.Request = None,
    stream: bool = True
):
    """
    Gradio 调用主函数
    - 支持流式和非流式两种模式
    """
    # ------------------ 获取客户端 IP 并记录日志 ------------------
    client_ip = "unknown"
    try:
        if request is not None:
            if hasattr(request, "client") and request.client:
                client_ip = getattr(request.client, "host", "unknown")
            elif hasattr(request, "headers"):
                xff = request.headers.get("x-forwarded-for")
                if xff:
                    client_ip = xff.split(',')[0].strip()
    except Exception as e:
        logger.warning(f"获取IP失败: {e}")

    logger.info(f"[IP:{client_ip}] 用户提问: {message}")

    # ------------------ 构建 LangChain 消息历史 ------------------
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
4. 多条件筛选人员（如姓名、工种、班次、部门、时间、主站区域、分站区域等）并获取明细  
   - 推荐工具：`query_personnel_list(names, work_types, class_names, departments, start_time, end_time, main_stations, sub_stations)`
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
    - 推荐工具：`query_car_trajectory(cardName,cardID, start_time, end_time)` 
11. 批量筛选车辆（支持车辆ID、名称、类型、部门、区域、电量、主站、分站、时间区间等多维过滤）并获取属性与进出明细  
    - 推荐工具：`query_cars_list(cardids, car_names, electricitys, area_names, departments, car_types, start_time, end_time, main_stations, sub_stations)`
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

    for h in history:
        content = h.get('content', "")
        if isinstance(content, list):
            text_parts = [part.get("text", "") if isinstance(part, dict) and part.get("type") == "text" else str(part) 
                         for part in content]
            content = "".join(text_parts)

        if h.get('role') == 'user':
            langchain_history.append(HumanMessage(content=content))
        else:
            # 清理历史记录中的 HTML 标签
            clean_content = re.sub(r"<details.*?</details>", "", str(content), flags=re.DOTALL).strip()
            langchain_history.append(AIMessage(content=clean_content))

    # 添加当前用户消息
    langchain_history.append(HumanMessage(content=message))

    # 调用核心生成函数
    async for response in generate_response(langchain_history, stream=stream):
        yield response


# ====================== Gradio 界面 ======================
with gr.Blocks(title="矿井人员定位智能调度系统") as demo:
    gr.Markdown("""
    # 👷‍♂️ 矿井人员定位智能调度系统
    **安全生产调度专家助手**  
    支持实时井下人数、人员轨迹、井下车辆、车辆轨迹、区域分布、多条件查询等功能
    """)

    # ==================== 主聊天界面（流式推荐） ====================
    with gr.Tab("智能对话 (流式)"):
        chat_interface = gr.ChatInterface(
            fn=predict,
            chatbot=gr.Chatbot(height=680, show_label=False),
            textbox=gr.Textbox(
                placeholder="例如：现在井下有多少人？• 陈玉岭今天去过哪些区域？",
                container=False,
                scale=7,
                lines=1
            ),
            examples=[
                ["现在井下实时人数和区域分布如何？"],
                ["陈玉岭今天在井下的活动轨迹是怎样的？"],
                ["昨天入井的总人数是多少？谁已经出井了？"],
                ["查询今天早班所有电工在 43204 区域的人员"],
                ["石小龙最近一次入井是什么时候？"],
            ],
            cache_examples=False,
            submit_btn="发送",
            stop_btn="停止",
            api_name="chat",
        )

    # ==================== 非流式 API 测试 Tab（已修复） ====================
    with gr.Tab("API 测试 (一次性完整返回)"):
        gr.Markdown("### 非流式接口测试 - 适合外部程序或脚本调用")

        with gr.Row():
            with gr.Column(scale=5):
                non_stream_input = gr.Textbox(
                    label="请输入问题",
                    placeholder="例如：现在井下有多少人？昨天入井总人数是多少？",
                    lines=3
                )
                non_stream_btn = gr.Button("提交并获取完整回复", variant="primary", size="large")

            with gr.Column(scale=5):
                non_stream_output = gr.Markdown(
                    label="完整回复结果",
                    value="**点击按钮后将在这里显示一次性完整结果**（包含思考过程）"
                )

        # 正确处理异步生成器的方式
        async def non_stream_predict(message: str, history=None):
            """专门用于非流式的一次性返回包装函数"""
            if history is None:
                history = []
            
            # 消费 async generator，取出最后一次 yield 的结果
            result = None
            async for response in predict(message, history, stream=False):
                result = response   # 保留最后一次（也是唯一一次）输出
            
            return result, []       # 返回 Markdown内容 + 空的 history state

        # 使用 .click 方式（推荐，更灵活）
        non_stream_btn.click(
            fn=non_stream_predict,
            inputs=[non_stream_input, gr.State([])],
            outputs=[non_stream_output, gr.State([])]
        )

        gr.Markdown("""
        ---
        **使用说明**：
        - 此 Tab 用于测试**一次性完整返回**模式
        - 外部系统推荐调用 API 接口：`/chat_non_stream`（如果后面你想加 FastAPI 可直接对接）
        """)

if __name__ == "__main__":
    demo.queue(default_concurrency_limit=5).launch(
        server_name="172.31.254.186",
        server_port=7861,
        share=False,
        show_error=True,
        # debug=True  # 开发时可开启
    )