#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""记忆系统核心模块 — Working / Long-term / Consolidation 共享逻辑"""

from __future__ import annotations

import json
import math
import re
import uuid
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from datetime import datetime
from functools import partial
from typing import Annotated, Literal, Optional

import operator
from langchain.chat_models import init_chat_model
from langchain.messages import AIMessage, AnyMessage, HumanMessage, RemoveMessage, SystemMessage
from langgraph.graph.message import REMOVE_ALL_MESSAGES
from langgraph.checkpoint.memory import MemorySaver
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

# ── LLM ──────────────────────────────────────────────────────────────────────

_model = None


def get_model():
    global _model
    if _model is None:
        _model = init_chat_model(
            "openai:qwen3.5-27b",
            base_url="http://10.11.3.210:9702/v1",
            api_key="EMPTY",
            temperature=0.3,
            max_tokens=4096,
        )
    return _model


# ── 数据结构 ─────────────────────────────────────────────────────────────────

MEMORY_TYPES = ("fact", "preference", "pattern")
CONSOLIDATION_TRIGGERS = ("token_threshold", "task_complete", "session_end", "manual")


@dataclass
class MemoryRecord:
    id: str
    content: str
    source: str = "user"
    memory_type: str = "fact"
    project_id: Optional[str] = None
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    last_accessed: str = field(default_factory=lambda: datetime.now().isoformat())
    access_count: int = 0
    confidence: float = 0.8
    verified: bool = False
    _tokens: list[str] = field(default_factory=list, repr=False)

    def to_dict(self) -> dict:
        d = asdict(self)
        d.pop("_tokens", None)
        return d


def _tokenize(text: str) -> list[str]:
    text = text.lower()
    en = re.findall(r"[a-z0-9]+", text)
    zh = re.findall(r"[\u4e00-\u9fff]", text)
    return en + zh


def approx_token_count(text: str) -> int:
    return max(1, len(text) // 2)


# ── Long-term Memory Index ────────────────────────────────────────────────────

class MemoryIndex:
    """多索引长期记忆：向量(词袋) + 关键词倒排"""

    def __init__(self):
        self.records: dict[str, MemoryRecord] = {}
        self.keyword_index: dict[str, set[str]] = defaultdict(set)

    def add(self, record: MemoryRecord) -> MemoryRecord:
        record._tokens = _tokenize(record.content)
        self.records[record.id] = record
        for tok in set(record._tokens):
            self.keyword_index[tok].add(record.id)
        return record

    def delete(self, memory_id: str) -> bool:
        rec = self.records.pop(memory_id, None)
        if not rec:
            return False
        for tok in set(rec._tokens):
            self.keyword_index[tok].discard(memory_id)
        return True

    def _vector_score(self, query_tokens: list[str], record: MemoryRecord) -> float:
        if not query_tokens or not record._tokens:
            return 0.0
        q, d = Counter(query_tokens), Counter(record._tokens)
        dot = sum(q[t] * d[t] for t in q)
        norm_q = math.sqrt(sum(v * v for v in q.values()))
        norm_d = math.sqrt(sum(v * v for v in d.values()))
        return dot / (norm_q * norm_d) if norm_q and norm_d else 0.0

    def search(
        self,
        query: str,
        memory_type: Optional[str] = None,
        project_id: Optional[str] = None,
        top_k: int = 5,
    ) -> list[tuple[MemoryRecord, float]]:
        query_tokens = _tokenize(query)
        candidates: set[str] = set()
        for tok in query_tokens:
            candidates.update(self.keyword_index.get(tok, []))
        if not candidates:
            candidates = set(self.records.keys())

        scored: list[tuple[MemoryRecord, float]] = []
        for mid in candidates:
            rec = self.records[mid]
            if memory_type and rec.memory_type != memory_type:
                continue
            if project_id and rec.project_id != project_id:
                continue
            kw_hits = sum(1 for t in query_tokens if t in rec._tokens)
            vec = self._vector_score(query_tokens, rec)
            score = vec * 0.7 + (kw_hits / max(len(query_tokens), 1)) * 0.3
            if score > 0:
                scored.append((rec, score))

        scored.sort(key=lambda x: x[1], reverse=True)
        for rec, _ in scored[:top_k]:
            rec.access_count += 1
            rec.last_accessed = datetime.now().isoformat()
        return scored[:top_k]

    def list_all(self) -> list[MemoryRecord]:
        return sorted(self.records.values(), key=lambda r: r.created_at, reverse=True)

    def format_records(self, records: list[tuple[MemoryRecord, float]] | list[MemoryRecord]) -> str:
        if not records:
            return "（暂无相关记忆）"
        lines = []
        items = records
        if records and isinstance(records[0], tuple):
            items = records  # type: ignore
            for rec, score in items:  # type: ignore
                lines.append(
                    f"- [{rec.memory_type}|{rec.source}|score={score:.2f}] {rec.content}"
                )
        else:
            for rec in records:  # type: ignore
                lines.append(f"- [{rec.memory_type}|{rec.source}] {rec.content}")
        return "\n".join(lines)


# 全局长期记忆库（演示用单例）
GLOBAL_MEMORY_INDEX = MemoryIndex()


def create_memory(
    content: str,
    source: str = "user",
    memory_type: str = "fact",
    project_id: Optional[str] = None,
    confidence: float = 0.8,
) -> MemoryRecord:
    rec = MemoryRecord(
        id=str(uuid.uuid4())[:8],
        content=content.strip(),
        source=source,
        memory_type=memory_type if memory_type in MEMORY_TYPES else "fact",
        project_id=project_id,
        confidence=confidence,
    )
    return GLOBAL_MEMORY_INDEX.add(rec)


# ── Working Memory (LangGraph + Checkpoint) ───────────────────────────────────

class ThreadState(TypedDict):
    messages: Annotated[list[AnyMessage], operator.add]
    current_task: str
    task_progress: float
    llm_calls: int


def _messages_to_text(messages: list[AnyMessage]) -> str:
    parts = []
    for m in messages:
        if isinstance(m, HumanMessage):
            parts.append(f"用户: {m.content}")
        elif isinstance(m, AIMessage):
            parts.append(f"助手: {m.content}")
    return "\n".join(parts)


async def llm_node(state: ThreadState, system_prompt: str) -> dict:
    model = get_model()
    msgs = state["messages"]
    if not any(isinstance(m, SystemMessage) for m in msgs):
        msgs = [SystemMessage(content=system_prompt)] + msgs
    response = await model.ainvoke(msgs)
    progress = min(1.0, state.get("task_progress", 0.0) + 0.1)
    return {"messages": [response], "llm_calls": state.get("llm_calls", 0) + 1, "task_progress": progress}


def build_working_memory_agent(system_prompt: str, checkpointer: MemorySaver):
    builder = StateGraph(ThreadState)
    builder.add_node("llm", partial(llm_node, system_prompt=system_prompt))
    builder.add_edge(START, "llm")
    builder.add_edge("llm", END)
    return builder.compile(checkpointer=checkpointer)


WORKING_SYSTEM = """你是一名记忆系统演示助手。你拥有 Working Memory（工作记忆），能记住当前会话 thread 内的全部对话历史。
请简洁回答，并在合适时引用用户之前说过的信息来证明你记住了上下文。"""

RECALL_SYSTEM = """你是一名具备长期记忆的助手。系统会从记忆库检索与用户问题相关的历史事实、偏好和模式，并注入到你的上下文中。
请优先依据「相关长期记忆」回答；若无相关记忆，请如实说明并正常对话。"""


async def chat_with_working_memory(
    agent,
    thread_id: str,
    user_message: str,
    current_task: str = "",
) -> tuple[str, dict]:
    config = {"configurable": {"thread_id": thread_id}}
    inp: dict = {"messages": [HumanMessage(content=user_message)]}
    if current_task:
        inp["current_task"] = current_task
    result = await agent.ainvoke(inp, config=config)
    reply = result["messages"][-1].content
    snapshot = await get_checkpoint_snapshot(agent, thread_id)
    return reply, snapshot


async def get_checkpoint_snapshot(agent, thread_id: str) -> dict:
    config = {"configurable": {"thread_id": thread_id}}
    state = await agent.aget_state(config)
    if not state or not state.values:
        return {"message_count": 0, "messages_preview": [], "current_task": "", "task_progress": 0.0}
    msgs = state.values.get("messages", [])
    preview = []
    for m in msgs[-6:]:
        role = "user" if isinstance(m, HumanMessage) else "assistant"
        content = str(m.content)[:120]
        preview.append({"role": role, "content": content})
    return {
        "message_count": len(msgs),
        "messages_preview": preview,
        "current_task": state.values.get("current_task", ""),
        "task_progress": state.values.get("task_progress", 0.0),
        "llm_calls": state.values.get("llm_calls", 0),
    }


# ── Consolidation ─────────────────────────────────────────────────────────────

async def extract_key_information(messages: list[AnyMessage]) -> list[str]:
    """从 Working Memory 消息中提取值得长期保留的信息"""
    if not messages:
        return []
    conv = _messages_to_text(messages[-20:])
    model = get_model()
    prompt = f"""从以下对话中提取值得写入长期记忆的关键信息（用户事实、偏好、重要结论）。
每条一行，不要编号，不要解释。若无有效信息，只输出「无」。

对话：
{conv}"""
    resp = await model.ainvoke([HumanMessage(content=prompt)])
    text = str(resp.content).strip()
    if text in ("无", "None", ""):
        return []
    lines = [ln.strip().lstrip("-•0123456789. ") for ln in text.splitlines() if ln.strip()]
    return [ln for ln in lines if ln and ln != "无"]


async def merge_with_existing(new_facts: list[str], index: MemoryIndex) -> list[MemoryRecord]:
    merged: list[MemoryRecord] = []
    for fact in new_facts:
        existing = index.search(fact, top_k=1)
        if existing and existing[0][1] > 0.85:
            old = existing[0][0]
            old.content = f"{old.content}；{fact}"
            old.confidence = min(1.0, old.confidence + 0.05)
            old._tokens = _tokenize(old.content)
            merged.append(old)
        else:
            merged.append(
                create_memory(fact, source="consolidation", memory_type="fact", confidence=0.75)
            )
    return merged


async def trim_working_memory(agent, thread_id: str, keep_recent: int = 4) -> int:
    """整合后裁剪 Working Memory，仅保留最近若干条消息"""
    config = {"configurable": {"thread_id": thread_id}}
    state = await agent.aget_state(config)
    if not state or not state.values:
        return 0
    msgs = state.values.get("messages", [])
    if len(msgs) <= keep_recent:
        return 0
    trimmed = len(msgs) - keep_recent
    kept = msgs[-keep_recent:]
    await agent.aupdate_state(
        config,
        {"messages": [RemoveMessage(id=REMOVE_ALL_MESSAGES), *kept]},
    )
    return trimmed


async def consolidate(
    agent,
    thread_id: str,
    index: MemoryIndex,
    trigger: str = "manual",
    token_threshold: int = 8000,
    keep_recent: int = 4,
) -> dict:
    """
    记忆整合流程（对应 09-memory.md §9.5）：
    1. 读取 Working Memory
    2. 提取关键信息
    3. 与 Long-term Memory 合并
    4. 写入长期记忆
    5. 裁剪 Working Memory
    """
    config = {"configurable": {"thread_id": thread_id}}
    state = await agent.aget_state(config)
    if not state or not state.values:
        return {"ok": False, "reason": "thread 无状态", "trigger": trigger}

    msgs = state.values.get("messages", [])
    total_tokens = sum(approx_token_count(str(m.content)) for m in msgs)

    if trigger == "token_threshold" and total_tokens < token_threshold:
        return {
            "ok": False,
            "reason": f"token 未达阈值 ({total_tokens}/{token_threshold})",
            "trigger": trigger,
            "total_tokens": total_tokens,
        }

    extracted = await extract_key_information(msgs)
    merged = await merge_with_existing(extracted, index) if extracted else []
    trimmed = await trim_working_memory(agent, thread_id, keep_recent=keep_recent)

    snapshot = await get_checkpoint_snapshot(agent, thread_id)
    return {
        "ok": True,
        "trigger": trigger,
        "total_tokens": total_tokens,
        "message_count_before": len(msgs),
        "extracted": extracted,
        "merged_count": len(merged),
        "merged_records": [r.to_dict() for r in merged],
        "trimmed_messages": trimmed,
        "snapshot_after": snapshot,
    }


async def chat_with_recall(user_message: str, index: MemoryIndex, top_k: int = 3) -> tuple[str, str]:
    """带长期记忆召回的对话"""
    hits = index.search(user_message, top_k=top_k)
    memory_block = index.format_records(hits)
    system = f"{RECALL_SYSTEM}\n\n## 相关长期记忆\n{memory_block}"
    model = get_model()
    resp = await model.ainvoke([
        SystemMessage(content=system),
        HumanMessage(content=user_message),
    ])
    return str(resp.content), memory_block


def snapshot_to_markdown(snap: dict) -> str:
    if not snap:
        return "暂无 checkpoint 数据"
    lines = [
        f"**消息数**: {snap.get('message_count', 0)}",
        f"**当前任务**: {snap.get('current_task', '-')}",
        f"**任务进度**: {snap.get('task_progress', 0):.0%}",
        f"**LLM 调用次数**: {snap.get('llm_calls', 0)}",
        "",
        "**最近消息预览**:",
    ]
    for m in snap.get("messages_preview", []):
        lines.append(f"- **{m['role']}**: {m['content']}")
    return "\n".join(lines)


def consolidation_report(result: dict) -> str:
    if not result.get("ok"):
        return f"整合未执行：{result.get('reason', '未知原因')}"
    lines = [
        f"### 整合报告 (trigger={result.get('trigger')})",
        f"- 整合前 token 估算: **{result.get('total_tokens', 0)}**",
        f"- 整合前消息数: **{result.get('message_count_before', 0)}**",
        f"- 提取关键信息 **{len(result.get('extracted', []))}** 条",
        f"- 写入/合并长期记忆 **{result.get('merged_count', 0)}** 条",
        f"- 裁剪 Working Memory **{result.get('trimmed_messages', 0)}** 条",
        "",
        "**提取内容:**",
    ]
    for item in result.get("extracted", []):
        lines.append(f"- {item}")
    lines.append("\n**整合后 Checkpoint:**")
    lines.append(snapshot_to_markdown(result.get("snapshot_after", {})))
    return "\n".join(lines)
