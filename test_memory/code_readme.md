# Code README — Demo 代码设计原理与功能说明

> 本文档面向**阅读源码**的开发者，详细说明 `test_memory/` 目录下各 Demo 文件的设计思路、模块职责、数据流与关键函数。  
> 概念层面的「是什么 / 为什么 / 怎么做」见 [`README.md`](README.md)；。

---

## 目录

1. [整体架构](#1-整体架构)
2. [共享核心：memory_core.py](#2-共享核心memory_corepy)
3. [Demo 1：demo_working_memory.py](#3-demo-1demo_working_memorypy)
4. [Demo 2：demo_longterm_memory.py](#4-demo-2demo_longterm_memorypy)
5. [Demo 3：demo_consolidation.py](#5-demo-3demo_consolidationpy)
6. [三个 Demo 的协作关系](#6-三个-demo-的协作关系)
7. [关键函数速查表](#7-关键函数速查表)


---

## 1. 整体架构

### 1.1 文件分层

```
┌─────────────────────────────────────────────────────────────┐
│                     Gradio UI 层（三个独立 Demo）              │
│  demo_working_memory.py │ demo_longterm_memory.py │ demo_consolidation.py │
└──────────────────────────────┬──────────────────────────────┘
                               │ import
                               ▼
┌─────────────────────────────────────────────────────────────┐
│                     memory_core.py（业务核心）                │
│  MemoryRecord / MemoryIndex / ThreadState / consolidate ...  │
└──────────────────────────────┬──────────────────────────────┘
                               │
              ┌────────────────┼────────────────┐
              ▼                ▼                ▼
        LangGraph          LangChain LLM     内存数据结构
     (Checkpoint/Graph)   (qwen3.5-27b)    (MemoryIndex 单例)
```

**设计原则**：

- **UI 与逻辑分离**：三个 Demo 只负责 Gradio 布局与事件绑定，核心逻辑全部在 `memory_core.py`。
- **Demo 独立可运行**：每个 Demo 是单独进程、单独端口，便于分主题学习；Consolidation Demo 复用了 Working + Long-term 的能力。
- **教学优先于生产**：向量检索用词袋余弦代替 Embedding；Checkpoint 用内存 `MemorySaver` 代替 Redis/PostgreSQL。

### 1.2 技术栈

| 组件 | 用途 |
|------|------|
| `gradio` | Web UI、异步回调、Chatbot 组件 |
| `langgraph` | 状态图、`MemorySaver` Checkpoint、消息裁剪 |
| `langchain` | LLM 调用、`HumanMessage` / `AIMessage` / `SystemMessage` |
| 纯 Python | `MemoryIndex` 倒排索引、词袋向量、Token 估算 |

---

## 2. 共享核心：memory_core.py

`memory_core.py` 是整个记忆演示的**唯一业务内核**，三个 Demo 均依赖它。

### 2.1 模块结构总览

```
memory_core.py
├── LLM 单例           get_model()
├── 长期记忆
│   ├── MemoryRecord   单条记忆数据结构
│   ├── MemoryIndex    多索引存储与检索
│   ├── GLOBAL_MEMORY_INDEX  进程内单例
│   └── create_memory()      创建并入库
├── 工作记忆
│   ├── ThreadState    LangGraph 状态 Schema
│   ├── llm_node()       图节点：调用 LLM
│   ├── build_working_memory_agent()  编译带 Checkpoint 的 Agent
│   ├── chat_with_working_memory()    对话入口
│   └── get_checkpoint_snapshot()     读取 Checkpoint 快照
├── 记忆整合
│   ├── extract_key_information()  LLM 提取关键信息
│   ├── merge_with_existing()      与长期记忆去重合并
│   ├── trim_working_memory()      裁剪短期消息
│   └── consolidate()              整合主流程
└── 辅助
    ├── chat_with_recall()         带 Recall 的对话
    ├── snapshot_to_markdown()     Checkpoint → Markdown
    └── consolidation_report()   整合结果 → Markdown
```

### 2.2 长期记忆：`MemoryRecord` + `MemoryIndex`

#### MemoryRecord — 单条记忆

字段含义：

| 字段 | 类型 | 说明 |
|------|------|------|
| `id` | str | 8 位 UUID，唯一标识 |
| `content` | str | 记忆正文 |
| `source` | str | 来源：`user` / `agent` / `consolidation` |
| `memory_type` | str | 类型：`fact` / `preference` / `pattern` |
| `project_id` | Optional[str] | 项目隔离（演示预留，检索可过滤） |
| `created_at` / `last_accessed` | str | ISO 时间戳 |
| `access_count` | int | 被检索命中次数 |
| `confidence` | float | 置信度 0.0–1.0 |
| `verified` | bool | 是否人工确认 |
| `_tokens` | list[str] | 内部分词结果，不参与序列化 |

#### MemoryIndex — 双索引检索

**设计原理**：生产环境通常用 Embedding + FAISS + 倒排 + 图索引；演示版实现了其中两个：

1. **关键词倒排索引** `keyword_index: dict[token → set[memory_id]]`
   - `add()` 时对 content 分词，每个 token 指向对应 memory_id
   - 检索时先通过 query 分词得到候选集，缩小搜索范围

2. **词袋向量（Bag-of-Words Cosine）** `_vector_score()`
   - 对 query 与 record 的分词结果做 Counter，计算余弦相似度
   - 不依赖外部 Embedding API，适合教学环境

**综合得分公式**：

```
score = vector_score × 0.7 + (keyword_hits / len(query_tokens)) × 0.3
```

向量语义占 70%，关键词精确命中占 30%。检索命中后自动更新 `access_count` 和 `last_accessed`。

#### 分词策略 `_tokenize()`

```python
# 英文/数字：按单词切分
# 中文：按单字切分（简单但足够演示）
en = re.findall(r"[a-z0-9]+", text.lower())
zh = re.findall(r"[\u4e00-\u9fff]", text)
```

### 2.3 工作记忆：LangGraph + Checkpoint

#### ThreadState — 图状态 Schema

```python
class ThreadState(TypedDict):
    messages: Annotated[list[AnyMessage], operator.add]  # 追加式 reducer
    current_task: str
    task_progress: float
    llm_calls: int
```

**关键设计**：

- `messages` 使用 `operator.add` 作为 reducer → 新消息**追加**到历史，而非覆盖。
- `current_task` / `task_progress` 为普通字段，由节点返回值更新。


#### Agent 图结构

```
START → llm_node → END
```

极简单节点图，不含工具调用。`build_working_memory_agent()` 编译时注入 `checkpointer=MemorySaver()`，使每次 `ainvoke` 按 `thread_id` 持久化状态。

#### llm_node 行为

1. 若 messages 中无 `SystemMessage`，自动在头部插入 `system_prompt`。
2. 调用 LLM，返回新的 `AIMessage`（追加到 messages）。
3. `task_progress` 每轮 +0.1，上限 1.0。
4. `llm_calls` 计数 +1。

#### chat_with_working_memory 对话入口

```python
config = {"configurable": {"thread_id": thread_id}}
inp = {"messages": [HumanMessage(content=user_message)]}
if current_task:
    inp["current_task"] = current_task
result = await agent.ainvoke(inp, config=config)
```

**注意**：

- 只传入**本轮新消息**；历史消息由 Checkpoint 自动合并（`operator.add`）。
- 不传入 `task_progress`，避免每轮重置进度；进度由 Checkpoint 中已有值 + `llm_node` 递增。
- 返回 `(reply, snapshot)`，snapshot 供 UI 展示。

#### get_checkpoint_snapshot — 状态可视化

从 Checkpoint 读取当前 thread 的完整状态，返回：

```python
{
    "message_count": int,        # 总消息数
    "messages_preview": [...],   # 最近 6 条，每条截断 120 字符
    "current_task": str,
    "task_progress": float,
    "llm_calls": int,
}
```

### 2.4 记忆整合：Consolidation 流水线

由 `consolidate()` 串联：

```
┌──────────────────┐
│ 1. aget_state    │  读取 Working Memory 全部 messages
└────────┬─────────┘
         ▼
┌──────────────────┐
│ 2. token 检查    │  trigger=token_threshold 时，未达阈值则跳过
└────────┬─────────┘
         ▼
┌──────────────────┐
│ 3. extract_key   │  LLM 从最近 20 条对话提取关键信息（逐行输出）
└────────┬─────────┘
         ▼
┌──────────────────┐
│ 4. merge_with    │  与长期记忆去重：相似度 > 0.85 则合并更新，否则新建
└────────┬─────────┘
         ▼
┌──────────────────┐
│ 5. trim_working  │  RemoveMessage 删除旧消息，保留最近 keep_recent 条
└──────────────────┘
```

#### extract_key_information

- 输入：Working Memory 的 messages 列表。
- 只取**最近 20 条**转文本，控制 LLM 输入长度。
- Prompt 要求：每条一行、无编号；无有效信息时输出「无」。
- 输出：`list[str]`，每条为一个待写入长期记忆的事实。

#### merge_with_existing — 去重合并

```python
existing = index.search(fact, top_k=1)
if existing and existing[0][1] > 0.85:
    # 高相似 → 追加内容、提升 confidence
    old.content = f"{old.content}；{fact}"
    old.confidence = min(1.0, old.confidence + 0.05)
else:
    # 新事实 → 创建 MemoryRecord，source="consolidation"
    create_memory(fact, source="consolidation", ...)
```

#### trim_working_memory — 消息裁剪

LangGraph 的 `messages` 使用 `operator.add` reducer，不能直接覆盖列表。正确做法：

```python
await agent.aupdate_state(config, {
    "messages": [RemoveMessage(id=REMOVE_ALL_MESSAGES), *kept_messages]
})
```

`REMOVE_ALL_MESSAGES` 清空全部历史，再追加保留的最近 N 条。返回裁剪条数 `trimmed = len(msgs) - keep_recent`。

#### chat_with_recall — Recall 注入

Long-term Memory Demo 使用的对话路径，**不经过 LangGraph**：

1. `index.search(user_message, top_k)` 检索相关记忆
2. 格式化为 Markdown 文本块
3. 拼入 System Prompt：`RECALL_SYSTEM + 相关长期记忆`
4. 直接 `model.ainvoke([SystemMessage, HumanMessage])`

每次对话**无状态**（不累积 Working Memory），完全依赖长期记忆库召回。

---

## 3. Demo 1：demo_working_memory.py

### 3.1 设计目标

**单独演示 Working Memory**：让用户直观看到 LangGraph Checkpoint 如何在同一 `thread_id` 下持久化对话状态。

### 3.2 启动配置

| 项 | 值 |
|----|-----|
| 端口 | 7870 |
| Agent | `build_working_memory_agent(WORKING_SYSTEM, MemorySaver())` |
| 进程内单例 | 模块加载时创建 `checkpointer` 和 `agent` |

### 3.3 UI 布局

```
┌─────────────────────────────────────────────────────────┐
│  Thread ID  │  当前任务                                    │
├──────────────────────────┬──────────────────────────────┤
│  Chatbot（对话区）         │  Checkpoint 状态面板           │
│  输入框 + 发送/清空         │  [刷新 Checkpoint 状态]        │
│  Examples                │                              │
└──────────────────────────┴──────────────────────────────┘
```

### 3.4 核心函数

| 函数 | 输入 | 输出 | 职责 |
|------|------|------|------|
| `predict()` | message, history, thread_id, current_task | history, markdown | 发送消息 → 调用 `chat_with_working_memory` → 更新 UI |
| `refresh_state()` | thread_id | markdown | 不发送消息，仅刷新 Checkpoint 快照 |

### 3.5 数据流

```
用户输入 message
    │
    ▼
predict(message, history, thread_id, current_task)
    │
    ├─→ chat_with_working_memory(agent, tid, message, task)
    │       │
    │       ├─ agent.ainvoke({messages: [HumanMessage]}, config={thread_id})
    │       │     └─ Checkpoint 自动合并历史 messages
    │       │
    │       └─ get_checkpoint_snapshot() → snap
    │
    ├─→ history.append(user + assistant)
    │
    └─→ 返回 (history, snapshot_to_markdown(snap))
```

### 3.6 关键设计点

1. **Gradio Chatbot 与 Checkpoint 是两套状态**
   - Chatbot 的 `history` 仅用于前端展示；刷新页面后 Chatbot 清空，但 Checkpoint 仍在。
   - 用相同 `thread_id` 发新消息，Agent 仍能从 Checkpoint 恢复完整上下文。
   - 「刷新 Checkpoint 状态」按钮用于验证这一点。

2. **`current_task` 由 UI 传入**
   - 每轮对话可更新任务描述，写入 ThreadState。
   - 右侧状态面板展示 `current_task` 和 `task_progress`。

3. **「清空对话显示」不影响 Checkpoint**
   - 只清空 Gradio Chatbot 组件，不调用任何清除 Checkpoint 的逻辑。

---

## 4. Demo 2：demo_longterm_memory.py

### 4.1 设计目标

**单独演示 Long-term Memory**：记忆的 CRUD、多策略检索、Recall 注入对话。

### 4.2 启动配置

| 项 | 值 |
|----|-----|
| 端口 | 7871 |
| 记忆库 | `GLOBAL_MEMORY_INDEX`（进程内单例） |
| 预置数据 | `_SEED` 5 条，首次访问时自动写入 |

### 4.3 UI 三 Tab 设计

#### Tab 1：记忆库管理

| 组件 | 绑定函数 | 功能 |
|------|----------|------|
| 添加表单 | `add_memory()` | 创建 MemoryRecord 并入库 |
| 记忆表格 | `list_memories_md()` | Markdown 表格展示全部记忆 |
| 删除 | `delete_memory()` | 按 ID 删除，同步更新倒排索引 |

`list_memories_md()` 每次调用前执行 `_ensure_seed()`，保证空库时有演示数据。

#### Tab 2：记忆检索

| 组件 | 绑定函数 | 功能 |
|------|----------|------|
| 查询框 + 类型过滤 + Top-K | `search_memories()` | 调用 `MemoryIndex.search()` |
| Examples | — | 预设查询样例 |

返回格式：每条记忆带 `score` 分数，便于理解排序逻辑。

#### Tab 3：带 Recall 对话

| 组件 | 绑定函数 | 功能 |
|------|----------|------|
| Chatbot + Top-K 滑块 | `recall_chat()` | 检索 → 注入 Prompt → LLM 回复 |
| 召回详情面板 | — | 展示本次命中了哪些记忆 |

`recall_chat()` 在 assistant 回复末尾附加 `--- 本次召回的记忆:` 块，方便对照 Recall 效果。

### 4.4 数据流（Recall 对话）

```
用户输入 message
    │
    ▼
recall_chat(message, history, top_k)
    │
    ├─→ chat_with_recall(message, GLOBAL_MEMORY_INDEX, top_k)
    │       │
    │       ├─ index.search(message, top_k)     → hits
    │       ├─ index.format_records(hits)       → memory_block
    │       ├─ system = RECALL_SYSTEM + memory_block
    │       └─ model.ainvoke([SystemMessage, HumanMessage]) → reply
    │
    └─→ 返回 (history + reply + 召回块, memory_block)
```

### 4.5 关键设计点

1. **与 Working Memory 完全解耦**
   - 不使用 LangGraph，不维护 thread 状态。
   - 每次对话独立，跨「会话」能力完全来自 `GLOBAL_MEMORY_INDEX`。

2. **预置 Seed 数据**
   - 覆盖三种 `memory_type`（fact / preference / pattern）和两种 `source`（user / agent）。
   - 方便开箱测试检索与 Recall。

3. **`demo.load()` 钩子**
   - 页面加载时自动刷新记忆表格，避免首次渲染空白。

---

## 5. Demo 3：demo_consolidation.py

### 5.1 设计目标

**演示 Working Memory → Long-term Memory 的完整整合链路**，是三个 Demo 中唯一同时使用 Checkpoint Agent 和 MemoryIndex 的样例。

### 5.2 启动配置

| 项 | 值 |
|----|-----|
| 端口 | 7872 |
| Agent | 同 Demo 1，`MemorySaver()` + `build_working_memory_agent` |
| 长期记忆 | 共享 `GLOBAL_MEMORY_INDEX`（注意：与 Demo 2 同进程时才共享；不同进程各自独立） |
| 默认 thread_id | `consolidation-demo` |

### 5.3 UI 布局

```
┌─────────────────────────────────────────────────────────┐
│  Thread ID  │  Token 阈值滑块  │  保留最近消息数滑块        │
├──────────────────────────┬──────────────────────────────┤
│  Working Memory 对话区   │  WM 状态 + Token 估算         │
│                          │  触发方式下拉框                │
│                          │  [执行整合] [自动检测并整合]    │
│                          │  整合报告面板                  │
├──────────────────────────┴──────────────────────────────┤
│  长期记忆库表格（整合后实时更新）                            │
└─────────────────────────────────────────────────────────┘
```

### 5.4 核心函数

| 函数 | 职责 |
|------|------|
| `predict()` | 对话 + 计算 Token 估算 + 阈值告警 |
| `run_consolidation()` | 按用户选择的 trigger 执行 `consolidate()` |
| `auto_consolidate_if_needed()` | 固定 `trigger=token_threshold`，未达阈值则拒绝 |
| `list_memories_md()` | 展示整合后的长期记忆库 |

### 5.5 数据流（整合操作）

```
用户点击「执行整合」
    │
    ▼
run_consolidation(thread_id, trigger, token_threshold, keep_recent)
    │
    ├─→ consolidate(agent, tid, GLOBAL_MEMORY_INDEX, ...)
    │       │
    │       ├─ aget_state → messages
    │       ├─ (可选) token 阈值检查
    │       ├─ extract_key_information(messages) → extracted[]
    │       ├─ merge_with_existing(extracted) → merged[]
    │       ├─ trim_working_memory(agent, tid, keep_recent) → trimmed
    │       └─ 返回 result dict
    │
    ├─→ consolidation_report(result) → Markdown 报告
    ├─→ get_checkpoint_snapshot() → 整合后 WM 状态
    └─→ list_memories_md() → 更新 LTM 表格
```

### 5.6 predict 中的 Token 监控

```python
tokens = sum(approx_token_count(str(m.content)) for m in msgs)
```

- `approx_token_count` 用 `len(text) // 2` 粗估中文 Token。
- 当 `tokens >= token_threshold` 时在状态面板显示告警，提示用户执行整合。
- 这只是一种**前端提示**，不会自动触发整合（自动整合需点「自动检测 Token 并整合」）。

### 5.7 两种整合按钮的区别

| 按钮 | trigger | 行为 |
|------|---------|------|
| 执行整合 | 用户下拉选择（默认 `manual`） | 跳过 token 检查，直接整合 |
| 自动检测 Token 并整合 | 固定 `token_threshold` | 未达阈值则返回「整合未执行」 |

`task_complete` 和 `session_end` 在演示版中与 `manual` 行为相同（不做额外判断），生产环境可扩展钩子逻辑。

### 5.8 关键设计点

1. **整合前必须先有对话**
   - 空 thread 调用 `consolidate()` 返回 `{ok: False, reason: "thread 无状态"}`。

2. **整合后 Working Memory 变短、Long-term Memory 变长**
   - 右侧面板 message_count 应明显下降（保留 keep_recent 条）。
   - 底部 LTM 表格出现 `source=consolidation` 的新记录。

3. **与 Demo 2 的 GLOBAL_MEMORY_INDEX**
   - 同一 Python 进程中，若先跑 Demo 2 再跑 Demo 3（不现实，因独立进程），记忆不共享。
   - 每个 Demo 进程有独立的 `GLOBAL_MEMORY_INDEX` 实例；Checkpoint 同理在进程内有效。

---

## 6. 三个 Demo 的协作关系

```
                    ┌─────────────────────┐
                    │   memory_core.py    │
                    └─────────┬───────────┘
                              │
         ┌────────────────────┼────────────────────┐
         ▼                    ▼                    ▼
  demo_working_memory   demo_longterm_memory   demo_consolidation
  ─────────────────     ──────────────────     ─────────────────
  只用：                 只用：                  同时使用：
  · MemorySaver          · MemoryIndex           · MemorySaver (WM)
  · ThreadState          · chat_with_recall      · MemoryIndex (LTM)
  · chat_with_working    · create_memory         · consolidate()
                         · search()              · chat_with_working
                                                 · extract/merge/trim
```

**学习路径建议**：

1. 先跑 **Demo 1**，理解 Checkpoint 与 thread_id。
2. 再跑 **Demo 2**，理解记忆存储、检索、Recall。
3. 最后跑 **Demo 3**，看 WM 如何通过 Consolidation 沉淀到 LTM。

---

## 7. 关键函数速查表

### memory_core.py

| 函数 / 类 | 签名要点 | 调用方 |
|-----------|----------|--------|
| `MemoryIndex.add()` | `(record) → record` | `create_memory` |
| `MemoryIndex.search()` | `(query, memory_type?, top_k) → [(record, score)]` | Demo 2 检索、Recall、merge |
| `MemoryIndex.delete()` | `(memory_id) → bool` | Demo 2 删除 |
| `create_memory()` | `(content, source, memory_type, ...) → MemoryRecord` | Demo 2 添加、Consolidation 写入 |
| `build_working_memory_agent()` | `(system_prompt, checkpointer) → CompiledGraph` | Demo 1、Demo 3 |
| `chat_with_working_memory()` | `(agent, thread_id, message, current_task?) → (reply, snap)` | Demo 1、Demo 3 |
| `get_checkpoint_snapshot()` | `(agent, thread_id) → dict` | Demo 1 刷新、Demo 3 状态 |
| `chat_with_recall()` | `(message, index, top_k) → (reply, memory_block)` | Demo 2 Recall 对话 |
| `consolidate()` | `(agent, thread_id, index, trigger, ...) → dict` | Demo 3 |
| `consolidation_report()` | `(result) → markdown str` | Demo 3 报告展示 |
| `snapshot_to_markdown()` | `(snap) → markdown str` | Demo 1、Demo 3 状态面板 |

### Demo 层 UI 回调

| 文件 | 函数 | 触发方式 |
|------|------|----------|
| `demo_working_memory.py` | `predict` | 发送 / Enter |
| `demo_working_memory.py` | `refresh_state` | 刷新按钮 |
| `demo_longterm_memory.py` | `add_memory` / `delete_memory` | 管理 Tab 按钮 |
| `demo_longterm_memory.py` | `search_memories` | 检索 Tab 按钮 |
| `demo_longterm_memory.py` | `recall_chat` | Recall Tab 发送 |
| `demo_consolidation.py` | `predict` | 对话发送 |
| `demo_consolidation.py` | `run_consolidation` | 执行整合 |
| `demo_consolidation.py` | `auto_consolidate_if_needed` | 自动检测整合 |

---



## 附录：启动命令

```bash
cd test_memory

python demo_working_memory.py      # http://172.31.254.186:7870
python demo_longterm_memory.py     # http://172.31.254.186:7871
python demo_consolidation.py       # http://172.31.254.186:7872
```

