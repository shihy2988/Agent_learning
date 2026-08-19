#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文件名:    agent_gradio.py
作者:      shihy (优化版)
创建日期:  2026-04-28
描述:      选品Agent 对话系统 - Gradio API接口测试（只保留非流式完整返回+图片）
"""

import gradio as gr
import asyncio
import re
import logging
import os
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage

from demo_stream import build_agent, mcp_client
from skill_loader import SkillManager

skill_manager = SkillManager()

# ====================== 日志初始化 ======================
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


def extract_images_and_text(text: str):
    """从AI输出内容中抽取图片链接和文本"""
    image_urls = []
    for m in re.findall(r'<img\s+[^>]*src=["\']([^"\']+)["\']', text):
        image_urls.append(m)
    for m in re.findall(r'!\[.*?\]\((.*?)\)', text):
        image_urls.append(m)
    for m in re.findall(r'(https?://[^\s)]+?\.(?:png|jpe?g|gif|webp|bmp|svg))', text):
        image_urls.append(m)

    image_urls = [
        u for u in dict.fromkeys(image_urls)
        if isinstance(u, str) and (
                (u.startswith("http://") or u.startswith("https://"))
                and '.' in u.split('//', 1)[-1]
                and not re.match(r'https?://\[(.*?)\]', u)
        )
    ]
    text_noimg = re.sub(r'(<img\s+[^>]*src=["\']([^"\']+)["\'][^>]*>|!\[.*?\]\((.*?)\))', '', text)
    return text_noimg, image_urls


def process_thinking_process(text: str) -> str:
    """只处理最外层的 <think>...</think>，支持富文本/带图片"""
    start = text.find("<think>")
    if start == -1:
        return text

    end = text.rfind("</think>")
    if end == -1 or end < start:
        return text

    prefix = text[:start]
    content = text[start + len("<think>"):end]
    suffix = text[end + len("</think>"):]
    content_text, content_imgs = extract_images_and_text(content)
    img_md = "\n".join([f"![图片]({u})" for u in content_imgs])
    return (
        f"{prefix}"
        f"<details style='color: #555; border-left: 3px solid #ccc; padding-left: 12px; margin: 8px 0; background: #f9f9f9;'>"
        f"<summary>📝 查看思考过程</summary>{content_text}{img_md}</details>"
        f"{suffix}"
    )


# ====================== 核心生成逻辑（非流式） ======================
async def generate_response(langchain_history, skill_dict=None):
    """
    核心回复生成函数（一次性返回完整内容，适合 API 调用）
    👈 增加了对 skill_dict 的支持并直接交付给 LangGraph 状态机
    """
    agent = await get_agent_instance()

    # 构造标准 LangGraph 初始化输入，把当前规则绑定进去
    initial_input = {
        "messages": langchain_history,
        "current_skill": skill_dict,
        "llm_calls": 0
    }

    full_response = "<think> "
    try:
        async for chunk, metadata in agent.astream(
                initial_input,
                stream_mode="messages",
        ):
            if isinstance(chunk, AIMessage) and chunk.content:
                content_piece = chunk.content
                if isinstance(content_piece, list):
                    text_parts = []
                    for p in content_piece:
                        if isinstance(p, dict) and p.get("type") == "text":
                            text_parts.append(p.get("text", ""))
                        else:
                            text_parts.append(str(p))
                    content_piece = "".join(text_parts)
                full_response += content_piece

        rich_out = process_thinking_process(full_response)
        txt, imgs = extract_images_and_text(rich_out)
        img_urls = [
            url for url in imgs
            if isinstance(url, str)
               and (url.startswith("http://") or url.startswith("https://"))
               and '.' in url.split('//', 1)[-1]
               and not re.match(r'https?://\[(.*?)\]', url)
        ]

        if img_urls:
            yield (txt, img_urls)
        else:
            yield (rich_out, None)

    except Exception as e:
        logger.error(f"生成回复时发生错误: {e}", exc_info=True)
        error_msg = f"<think>工具调用或生成过程中发生错误: {str(e)}</think>\n\n抱歉，系统处理时出现异常，请稍后重试。"
        yield (error_msg, None)


# ====================== 主调函数 ======================
async def predict_nonstream(
        message: str,
        history=None,
        request: gr.Request = None
):
    """Gradio调用主函数（面向底层标准的 API 单独暴露接口）"""
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

    # 识别并拼装 Skill 字典
    skill = skill_manager.select_skill(message)
    skill_dict = {
        "name": skill.name,
        "description": skill.description,
        "tools": skill.tools,
        "content": skill.content
    } if skill else None

    if history is None:
        history = []

    langchain_history = []
    for h in history:
        content = h.get('content', "")
        if isinstance(content, list):
            text_parts = [part.get("text", "") if isinstance(part, dict) and part.get("type") == "text" else str(part)
                          for part in content]
            content = "".join(text_parts)

        if h.get('role') == 'user':
            langchain_history.append(HumanMessage(content=content))
        else:
            # 清洗历史回复中的大段思考思考过程，降低上下文冗余
            clean_content = re.sub(r"<details.*?</details>", "", str(content), flags=re.DOTALL).strip()
            langchain_history.append(AIMessage(content=clean_content))

    langchain_history.append(HumanMessage(content=message))

    last_text = None
    last_imgs = []
    async for out, imgs in generate_response(langchain_history, skill_dict=skill_dict):
        last_text = out
        last_imgs = [
            img for img in (imgs or [])
            if isinstance(img, str)
               and (img.startswith("http://") or img.startswith("https://"))
               and '.' in img.split('//', 1)[-1]
               and not re.match(r'https?://\[(.*?)\]', img)
        ]

    if not last_imgs:
        last_imgs = None
    return last_text, last_imgs


# ====================== Gradio 界面 ======================
with gr.Blocks(title="矿山人员车辆智能助手") as demo:
    gr.Markdown("""
    # ⛏️ 矿山人员车辆智能助手
    支持：人员状态查询 | 人员轨迹分析 | 车辆状态查询 | 区域巡检 | 异常监控 | 应急分析
    系统会自动加载匹配的 Skill
    """)

    history_state = gr.State([])

    with gr.Tab("智能查询"):
        with gr.Row():
            with gr.Column(scale=4):
                non_stream_input = gr.Textbox(
                    label="请输入问题",
                    placeholder="例如：\n陈玉岭今天在井下的活动轨迹\n43203回风区有多少人\n当前有什么异常\n9号自行车在哪里",
                    lines=5
                )
                non_stream_btn = gr.Button("查询", variant="primary", size="lg")

            with gr.Column(scale=6):
                current_skill = gr.Textbox(label="当前加载Skill", interactive=False)
                non_stream_output = gr.Markdown(label="结果", value="等待输入...")
                non_stream_img_gallery = gr.Gallery(label="图片结果", show_label=True, columns=3, height=300)


    async def wrapper(message, history):
        # 1. 动态选品与识别对应 Skill
        skill = skill_manager.select_skill(query=message)
        skill_dict = {
            "name": skill.name,
            "description": skill.description,
            "tools": skill.tools,
            "content": skill.content
        } if skill else None

        # 2. 清洗历史状态中的思考标签，并转换为干净的 Langchain 序列送入模型
        cleaned_history = []
        for m in history:
            if isinstance(m, SystemMessage):
                continue
            if isinstance(m, AIMessage):
                clean_content = re.sub(r"<details.*?</details>", "", str(m.content), flags=re.DOTALL).strip()
                cleaned_history.append(AIMessage(content=clean_content))
            else:
                cleaned_history.append(m)

        cleaned_history.append(HumanMessage(content=message))

        result = "未获取到返回"
        imgs = None

        # 3. 传入调用链生成
        async for out, img in generate_response(cleaned_history, skill_dict=skill_dict):
            result = out
            imgs = img

        # 4. 把原始带有丰富格式/思考组件的内容存回历史状态，以保证前端UI能够查看思考链
        history.append(HumanMessage(content=message))
        history.append(AIMessage(content=result))

        return (
            skill.name if skill else "default",
            result,
            imgs,
            history
        )


    def nonstream_wrapper(message, history):
        return asyncio.run(wrapper(message, history))


    non_stream_btn.click(
        fn=nonstream_wrapper,
        inputs=[non_stream_input, history_state],
        outputs=[current_skill, non_stream_output, non_stream_img_gallery, history_state]
    )

if __name__ == "__main__":
    demo.queue(default_concurrency_limit=5).launch(
        server_name="0.0.0.0",
        server_port=7861,
        share=False,
        show_error=True,
    )