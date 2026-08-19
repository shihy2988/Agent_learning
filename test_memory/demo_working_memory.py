#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Working Memory 演示 — LangGraph Checkpointing + Thread 状态持久化

对应 09-memory.md §9.3：
- MemorySaver 作为 checkpointer
- ThreadState 管理 messages / current_task / task_progress
- 同一 thread_id 可跨轮次恢复会话状态

运行: python demo_working_memory.py
"""

import gradio as gr
from langgraph.checkpoint.memory import MemorySaver

from memory_core import (
    WORKING_SYSTEM,
    build_working_memory_agent,
    chat_with_working_memory,
    get_checkpoint_snapshot,
    snapshot_to_markdown,
)

checkpointer = MemorySaver()
agent = build_working_memory_agent(WORKING_SYSTEM, checkpointer)


async def predict(message: str, history, thread_id: str, current_task: str):
    tid = (thread_id or "demo-thread-1").strip()
    task = (current_task or "记忆演示对话").strip()
    reply, snap = await chat_with_working_memory(agent, tid, message, task)
    history = history or []
    history.append({"role": "user", "content": message})
    history.append({"role": "assistant", "content": reply})
    return history, snapshot_to_markdown(snap)


async def refresh_state(thread_id: str):
    tid = (thread_id or "demo-thread-1").strip()
    snap = await get_checkpoint_snapshot(agent, tid)
    return snapshot_to_markdown(snap)


with gr.Blocks(title="Working Memory 演示") as demo:
    gr.Markdown("""
    # Working Memory（工作记忆）演示

    基于 **LangGraph MemorySaver Checkpoint**，在同一会话 `thread_id` 内持久化对话状态。

    | 能力 | 说明 |
    |------|------|
    | messages | 当前 thread 全部对话历史 |
    | current_task | 任务上下文 |
    | task_progress | 任务进度（每轮 +10%） |

    **测试建议**：保持 `thread_id` 不变，多轮对话后刷新页面再输入相同 thread_id，状态仍可恢复。
    """)

    with gr.Row():
        thread_id = gr.Textbox(label="Thread ID", value="demo-thread-1", scale=2)
        current_task = gr.Textbox(label="当前任务", value="了解用户偏好", scale=2)

    with gr.Row():
        with gr.Column(scale=2):
            chatbot = gr.Chatbot(height=480, label="对话")
            msg = gr.Textbox(label="输入", placeholder="例如：我叫小明，喜欢 Python...")
            with gr.Row():
                submit = gr.Button("发送", variant="primary")
                clear = gr.Button("清空对话显示")
            gr.Examples(
                examples=[
                    ["我叫张三，是一名后端工程师"],
                    ["我主要用什么编程语言？（测试是否记住上文）"],
                    ["帮我总结一下你目前了解到的关于我的信息"],
                ],
                inputs=msg,
            )
        with gr.Column(scale=1):
            state_panel = gr.Markdown(label="Checkpoint 状态", value="等待对话...")
            refresh_btn = gr.Button("刷新 Checkpoint 状态")

    submit.click(predict, [msg, chatbot, thread_id, current_task], [chatbot, state_panel]).then(
        lambda: "", outputs=msg
    )
    msg.submit(predict, [msg, chatbot, thread_id, current_task], [chatbot, state_panel]).then(
        lambda: "", outputs=msg
    )
    refresh_btn.click(refresh_state, thread_id, state_panel)
    clear.click(lambda: [], outputs=chatbot)


if __name__ == "__main__":
    demo.queue(default_concurrency_limit=5).launch(
        server_name="172.31.254.186",
        server_port=7870,
        share=False,
        show_error=True,
        # debug=True  # 开发时可开启
    )