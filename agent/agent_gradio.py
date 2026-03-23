#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
文件名:	agent_gradio.py
作者:	shihy
创建日期:	2026-03-11
描述:	
"""


import gradio as gr
import asyncio
import re
from langchain_core.messages import HumanMessage, AIMessage
from demo_stream import build_agent, mcp_client 

agent_instance = None

async def get_agent_instance():
    global agent_instance
    if agent_instance is None:
        tools = await mcp_client.get_tools()
        agent_instance = build_agent(tools)
    return agent_instance

def process_thinking_process(text):
    """
    将 <think> 标签内容转换为 HTML 折叠框
    """
    # 检查是否包含思考标签
    if "<think>" in text:
        # 如果标签还没闭合（正在思考中）
        if "</think>" not in text:
            parts = text.split("<think>")
            # 这里的 style 确保思考过程呈现灰度，并自动展开
            return f"{parts[0]}<details open style='color: #666; border-left: 2px solid #ccc; padding-left: 10px;'><summary>正在思考...</summary>{parts[1]}</details>"
        else:
            # 思考已结束，将其折叠
            return re.sub(
                r"<think>(.*?)</think>", 
                r"<details style='color: #888; border-left: 2px solid #eee; padding-left: 10px;'><summary>查看思考过程</summary>\1</details>", 
                text, 
                flags=re.DOTALL
            )
    return text

async def predict(message, history):
    agent = await get_agent_instance()
    
    # 转换历史记录
    langchain_history = []
    for h in history:
        content = h.get('content', "")
        
        # --- 修复 TypeError: 处理 content 为 list 的情况 ---
        if isinstance(content, list):
            # 如果是列表，通常包含多个消息块，我们将文本块提取出来拼接
            text_parts = []
            for part in content:
                if isinstance(part, dict) and part.get("type") == "text":
                    text_parts.append(part.get("text", ""))
                elif isinstance(part, str):
                    text_parts.append(part)
            content = "".join(text_parts)
        
        if h['role'] == 'user':
            langchain_history.append(HumanMessage(content=content))
        else:
            # 现在 content 确定是字符串了，安全进行正则替换
            # 过滤掉 HTML 思考折叠框，只保留纯文本回答给 AI 当上下文
            clean_content = re.sub(r"<details.*?</details>", "", str(content), flags=re.DOTALL).strip()
            langchain_history.append(AIMessage(content=clean_content))
    
    langchain_history.append(HumanMessage(content=message))
    
    full_raw_response = ""
    
    # 使用 stream_mode="messages" 捕获每一个 token
    async for chunk, metadata in agent.astream(
        {"messages": langchain_history},
        stream_mode="messages",
    ):
        if isinstance(chunk, AIMessage) and chunk.content:
            # 注意：有些模型返回的 chunk.content 也可能是列表，这里强制转字符串
            content_piece = chunk.content
            if isinstance(content_piece, list):
                content_piece = "".join([p.get("text", "") if isinstance(p, dict) else str(p) for p in content_piece])
            
            full_raw_response += content_piece
            # 实时处理文本，将 <think> 转换为可视化折叠框
            yield process_thinking_process(full_raw_response)

# --- 界面部分 ---
with gr.Blocks(title="摄像头智能助手") as demo:
    gr.Markdown("# 🎥 摄像头 AI 智能控制台")

    
    chat_interface = gr.ChatInterface(
        fn=predict,
        chatbot=gr.Chatbot(height=600, show_label=False),
        textbox=gr.Textbox(placeholder="输入指令，如：查询 10.11.4.22 的运行状态", container=False, scale=7),
        examples=["列出所有在线摄像头", "10.11.4.22 部署了什么模型？", "统计所有摄像头的告警情况"],
        cache_examples=False,
    )

if __name__ == "__main__":
    # 启用队列以支持并发流式输出
    demo.queue().launch(server_name="172.31.254.186", server_port=7861, share=False)
    
   