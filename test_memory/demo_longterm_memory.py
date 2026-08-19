#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Long-term Memory 演示 — 记忆存储、多索引检索与 Recall 注入

对应 09-memory.md §9.4：
- MemoryRecord 结构（fact / preference / pattern）
- 向量(词袋) + 关键词倒排索引
- 检索结果注入 System Prompt 实现 Recall

运行: python demo_longterm_memory.py
"""

import gradio as gr

from memory_core import (
    GLOBAL_MEMORY_INDEX,
    MEMORY_TYPES,
    chat_with_recall,
    create_memory,
)

# 预置演示数据
_SEED = [
    ("用户姓名叫李明，在北京工作", "fact", "user"),
    ("用户偏好使用 Python 和 FastAPI 开发", "preference", "user"),
    ("用户习惯晚上 10 点后写代码", "pattern", "user"),
    ("项目代号 Agent memory，目标是构建企业级 Agent 平台", "fact", "agent"),
    ("用户不喜欢冗长的回复，偏好简洁条目式答案", "preference", "user"),
]


def _ensure_seed():
    if not GLOBAL_MEMORY_INDEX.records:
        for content, mtype, source in _SEED:
            create_memory(content, source=source, memory_type=mtype)


def list_memories_md() -> str:
    _ensure_seed()
    recs = GLOBAL_MEMORY_INDEX.list_all()
    if not recs:
        return "（记忆库为空）"
    lines = ["| ID | 类型 | 来源 | 置信度 | 内容 |", "|:---|:---|:---|:---|:---|"]
    for r in recs:
        lines.append(
            f"| {r.id} | {r.memory_type} | {r.source} | {r.confidence:.2f} | {r.content[:60]} |"
        )
    return "\n".join(lines)


def add_memory(content: str, memory_type: str, source: str, project_id: str):
    if not content.strip():
        return "请输入记忆内容", list_memories_md()
    rec = create_memory(
        content.strip(),
        source=source or "user",
        memory_type=memory_type,
        project_id=project_id or None,
    )
    return f"已添加记忆 `{rec.id}`", list_memories_md()


def delete_memory(memory_id: str):
    if GLOBAL_MEMORY_INDEX.delete(memory_id.strip()):
        return f"已删除 `{memory_id}`", list_memories_md()
    return f"未找到 ID `{memory_id}`", list_memories_md()


def search_memories(query: str, memory_type: str, top_k: int):
    _ensure_seed()
    mtype = None if memory_type == "全部" else memory_type
    hits = GLOBAL_MEMORY_INDEX.search(query, memory_type=mtype, top_k=int(top_k))
    if not hits:
        return "未检索到相关记忆"
    lines = [f"查询: **{query}**", ""]
    for rec, score in hits:
        lines.append(
            f"- **{rec.id}** [{rec.memory_type}] score={score:.3f}\n  {rec.content}"
        )
    return "\n".join(lines)


async def recall_chat(message: str, history, top_k: int):
    _ensure_seed()
    reply, recalled = await chat_with_recall(message, GLOBAL_MEMORY_INDEX, top_k=int(top_k))
    history = history or []
    history.append({"role": "user", "content": message})
    display = f"{reply}\n\n---\n**本次召回的记忆:**\n{recalled}"
    history.append({"role": "assistant", "content": display})
    return history, recalled


with gr.Blocks(title="Long-term Memory 演示") as demo:
    gr.Markdown("""
    # Long-term Memory（长期记忆）演示

    演示 **MemoryRecord** 存储结构与 **多策略检索**（词袋向量 + 关键词倒排），
    并将召回片段注入 Prompt，实现跨会话 Recall。
    """)

    with gr.Tabs():
        with gr.Tab("记忆库管理"):
            gr.Markdown("### 添加 / 查看 / 删除长期记忆")
            with gr.Row():
                add_content = gr.Textbox(label="记忆内容", placeholder="例如：用户是 Java 开发者", scale=3)
                add_type = gr.Dropdown(label="类型", choices=list(MEMORY_TYPES), value="fact")
                add_source = gr.Dropdown(label="来源", choices=["user", "agent", "consolidation"], value="user")
                add_project = gr.Textbox(label="项目 ID（可选）")
            add_btn = gr.Button("添加记忆", variant="primary")
            add_status = gr.Textbox(label="操作结果", interactive=False)
            memory_table = gr.Markdown(value=list_memories_md())
            with gr.Row():
                del_id = gr.Textbox(label="要删除的记忆 ID")
                del_btn = gr.Button("删除")
            add_btn.click(add_memory, [add_content, add_type, add_source, add_project], [add_status, memory_table])
            del_btn.click(delete_memory, del_id, [add_status, memory_table])

        with gr.Tab("记忆检索"):
            gr.Markdown("### 语义 + 关键词混合检索")
            with gr.Row():
                search_query = gr.Textbox(label="查询", placeholder="例如：用户喜欢什么语言？", scale=3)
                search_type = gr.Dropdown(label="类型过滤", choices=["全部"] + list(MEMORY_TYPES), value="全部")
                search_topk = gr.Slider(1, 10, value=5, step=1, label="Top-K")
            search_btn = gr.Button("检索", variant="primary")
            search_result = gr.Markdown()
            search_btn.click(search_memories, [search_query, search_type, search_topk], search_result)
            gr.Examples(
                examples=[
                    ["用户叫什么名字"],
                    ["编程语言偏好"],
                    ["工作习惯"],
                    ["Agent 项目"],
                ],
                inputs=search_query,
            )

        with gr.Tab("带 Recall 对话"):
            gr.Markdown("### 自动检索长期记忆并注入上下文")
            recall_topk = gr.Slider(1, 8, value=3, step=1, label="召回 Top-K")
            recall_chatbot = gr.Chatbot(height=420)
            recall_msg = gr.Textbox(label="输入", placeholder="例如：帮我推荐一个适合我的技术栈")
            recall_panel = gr.Markdown(label="召回详情", value="")
            recall_btn = gr.Button("发送", variant="primary")
            recall_btn.click(
                recall_chat, [recall_msg, recall_chatbot, recall_topk], [recall_chatbot, recall_panel]
            ).then(lambda: "", outputs=recall_msg)
            recall_msg.submit(
                recall_chat, [recall_msg, recall_chatbot, recall_topk], [recall_chatbot, recall_panel]
            ).then(lambda: "", outputs=recall_msg)
            gr.Examples(
                examples=[
                    ["我晚上一般什么时候写代码？"],
                    ["根据我的偏好，回复尽量简短"],
                ],
                inputs=recall_msg,
            )

    demo.load(list_memories_md, outputs=memory_table)


if __name__ == "__main__":
    demo.launch(server_name="172.31.254.186", server_port=7871)
