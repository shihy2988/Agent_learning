#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Memory Consolidation 演示 — Working Memory → Long-term Memory 整合

对应 09-memory.md §9.5：
- 触发条件：token 阈值 / 任务完成 / 会话结束 / 手动
- 提取关键信息 → 合并长期记忆 → 裁剪 Working Memory

运行: python demo_consolidation.py
"""

import gradio as gr
from langgraph.checkpoint.memory import MemorySaver

from memory_core import (
    CONSOLIDATION_TRIGGERS,
    GLOBAL_MEMORY_INDEX,
    WORKING_SYSTEM,
    approx_token_count,
    build_working_memory_agent,
    chat_with_working_memory,
    consolidate,
    consolidation_report,
    get_checkpoint_snapshot,
    snapshot_to_markdown,
)

checkpointer = MemorySaver()
agent = build_working_memory_agent(WORKING_SYSTEM, checkpointer)


def list_memories_md() -> str:
    recs = GLOBAL_MEMORY_INDEX.list_all()
    if not recs:
        return "（长期记忆库为空，整合后将写入此处）"
    lines = ["| ID | 类型 | 来源 | 内容 |", "|:---|:---|:---|:---|"]
    for r in recs[:20]:
        lines.append(f"| {r.id} | {r.memory_type} | {r.source} | {r.content[:50]} |")
    return "\n".join(lines)


async def predict(message: str, history, thread_id: str, token_threshold: int):
    tid = (thread_id or "consolidation-demo").strip()
    reply, snap = await chat_with_working_memory(agent, tid, message)
    history = history or []
    history.append({"role": "user", "content": message})
    history.append({"role": "assistant", "content": reply})

    config = {"configurable": {"thread_id": tid}}
    state = await agent.aget_state(config)
    msgs = state.values.get("messages", []) if state and state.values else []
    tokens = sum(approx_token_count(str(m.content)) for m in msgs)

    status = snapshot_to_markdown(snap)
    status += f"\n\n**Token 估算**: {tokens} / 阈值 {int(token_threshold)}"
    if tokens >= int(token_threshold):
        status += "\n\n> ⚠️ 已达 token 阈值，建议执行整合"

    return history, status, list_memories_md()


async def run_consolidation(thread_id: str, trigger: str, token_threshold: int, keep_recent: int):
    tid = (thread_id or "consolidation-demo").strip()
    result = await consolidate(
        agent,
        tid,
        GLOBAL_MEMORY_INDEX,
        trigger=trigger,
        token_threshold=int(token_threshold),
        keep_recent=int(keep_recent),
    )
    report = consolidation_report(result)
    snap = await get_checkpoint_snapshot(agent, tid)
    status = snapshot_to_markdown(snap)
    return report, status, list_memories_md()


async def auto_consolidate_if_needed(thread_id: str, token_threshold: int, keep_recent: int):
    tid = (thread_id or "consolidation-demo").strip()
    result = await consolidate(
        agent,
        tid,
        GLOBAL_MEMORY_INDEX,
        trigger="token_threshold",
        token_threshold=int(token_threshold),
        keep_recent=int(keep_recent),
    )
    return consolidation_report(result), list_memories_md()


with gr.Blocks(title="Memory Consolidation 演示") as demo:
    gr.Markdown("""
    # Memory Consolidation（记忆整合）演示

    完整流程：**读取 Working Memory → 提取关键信息 → 合并 Long-term Memory → 裁剪冗余消息**

    | 触发方式 | 说明 |
    |----------|------|
    | `manual` | 手动点击整合 |
    | `token_threshold` | 上下文 token 超过阈值 |
    | `task_complete` / `session_end` | 任务完成 / 会话结束（演示用，逻辑同 manual） |
    """)

    with gr.Row():
        thread_id = gr.Textbox(label="Thread ID", value="consolidation-demo", scale=2)
        token_threshold = gr.Slider(500, 20000, value=8000, step=500, label="Token 阈值")
        keep_recent = gr.Slider(2, 10, value=4, step=1, label="整合后保留最近消息数")

    with gr.Row():
        with gr.Column(scale=2):
            chatbot = gr.Chatbot(height=400, label="Working Memory 对话")
            msg = gr.Textbox(label="输入", placeholder="多聊几轮后执行整合...")
            chat_btn = gr.Button("发送", variant="primary")
            gr.Examples(
                examples=[
                    ["我偏好 Markdown 格式的文档输出"],
                ],
                inputs=msg,
            )
        with gr.Column(scale=1):
            wm_status = gr.Markdown("等待对话...")
            gr.Markdown("### 整合操作")
            trigger = gr.Dropdown(
                label="触发方式",
                choices=list(CONSOLIDATION_TRIGGERS),
                value="manual",
            )
            consolidate_btn = gr.Button("执行整合", variant="primary")
            auto_btn = gr.Button("自动检测 Token 并整合")
            report_panel = gr.Markdown("整合报告将显示在这里")

    gr.Markdown("### 长期记忆库（整合后写入）")
    ltm_table = gr.Markdown(value=list_memories_md())

    chat_btn.click(
        predict, [msg, chatbot, thread_id, token_threshold], [chatbot, wm_status, ltm_table]
    ).then(lambda: "", outputs=msg)
    msg.submit(
        predict, [msg, chatbot, thread_id, token_threshold], [chatbot, wm_status, ltm_table]
    ).then(lambda: "", outputs=msg)

    consolidate_btn.click(
        run_consolidation,
        [thread_id, trigger, token_threshold, keep_recent],
        [report_panel, wm_status, ltm_table],
    )
    auto_btn.click(
        auto_consolidate_if_needed,
        [thread_id, token_threshold, keep_recent],
        [report_panel, ltm_table],
    )


if __name__ == "__main__":
    demo.launch(server_name="172.31.254.186", server_port=7872)
