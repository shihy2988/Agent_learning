# Memory 记忆系统 — 原理 · 为什么 · 怎么做

> 本目录是 **DeerFlow / Agent 记忆系统** 的学习与演示代码，对应教程 `09-memory.md`。  
> 通过三个 Gradio 样例，分别演示 **Working Memory**、**Long-term Memory**、**Memory Consolidation**。

---

## 一、原理是什么

Agent 的记忆系统模拟人类认知中的「短期工作区 + 长期知识库 + 定期整理」，在**有限的上下文窗口**内，维持对任务与用户的持续理解。

### 1.1 分层架构

```
┌─────────────────────────────────────────────────────────────┐
│                      Memory System                            │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────────┐    ┌─────────────────┐                │
│  │ Working Memory  │    │ Long-term Memory│                │
│  │ (当前会话)       │    │ (持久化知识)     │                │
│  │ - messages      │    │ - facts         │                │
│  │ - context       │    │ - preferences   │                │
│  │ - task state    │    │ - patterns      │                │
│  └────────┬────────┘    └────────┬────────┘                │
│           └──────────┬───────────┘                          │
│                      ▼                                       │
│           ┌─────────────────┐                               │
│           │ Memory Controller│                               │
│           │ - consolidation │                               │
│           │ - retrieval     │                               │
│           │ - compression   │                               │
│           └─────────────────┘                               │
└─────────────────────────────────────────────────────────────┘
```

| 层级 | 本质 | 存储什么 | 生命周期 |
|------|------|----------|----------|
| **Working Memory** | 当前 thread 的运行时状态 | 对话消息、任务进度、中间产物 | 会话内；Checkpoint 可跨请求恢复 |
| **Long-term Memory** | 结构化持久知识 | 用户事实、偏好、行为模式 | 跨会话、跨天持久保存 |
| **Consolidation** | 记忆控制器 | 从短期提取精华 → 写入长期 → 裁剪冗余 | 按触发条件执行 |

### 1.2 三个核心机制

#### Working Memory — Checkpoint 状态机

- 利用 **LangGraph `MemorySaver`** 按 `thread_id` 保存图状态。
- 每次对话不是「无状态请求」，而是对同一条状态链路的追加更新。
- 状态字段示例：`messages`、`current_task`、`task_progress`。

#### Long-term Memory — 多索引检索 + Recall

- 每条记忆是一个 `MemoryRecord`（内容、类型、来源、置信度、时间戳等）。
- 检索策略：**向量相似度（词袋余弦）+ 关键词倒排**，合并去重后排序。
- **Recall**：把检索到的记忆片段注入 System Prompt，让模型「想起来」跨会话信息。

#### Consolidation — 记忆整合

整合流程（对应 `memory_core.py` 中 `consolidate()`）：

1. 读取当前 Working Memory 全部消息  
2. 用 LLM **提取关键信息**（用户事实、偏好、结论）  
3. 与已有 Long-term Memory **去重合并**（高相似度则更新而非重复写入）  
4. **写入长期记忆库**  
5. **裁剪 Working Memory**，只保留最近 N 条，防止上下文膨胀  

触发条件：

| 触发器 | 含义 |
|--------|------|
| `token_threshold` | 上下文 token 超过阈值（默认 8000） |
| `task_complete` | 任务完成时 |
| `session_end` | 会话结束时 |
| `manual` | 手动触发 |

---

## 二、为什么需要 Memory

### 2.1 核心问题：上下文窗口有限

长时任务 Agent 随着对话增长会遇到：

- 早期重要信息被**稀释**，模型「遗忘」用户核心需求  
- 多轮、多子任务之间**状态不一致**  
- 直接把全部历史塞进 Prompt → **Token 成本飙升、响应变慢**

Memory 的目标：**在有限窗口内，保留「该记住的」、丢弃「可压缩的」**。

### 2.2 各层解决什么问题

| 问题 | 对应方案 |
|------|----------|
| 同一会话内多轮对话要记住上文 | Working Memory（Checkpoint） |
| 关闭页面 / 隔天再来仍记得用户偏好 | Long-term Memory（持久化 + Recall） |
| 对话太长导致 Token 爆炸 | Consolidation（提取 + 压缩 + 裁剪） |
| 企业场景知识隔离与复用 | 项目级记忆、权限控制（见 `09-memory.md` §9.6） |

### 2.3 设计原则（最佳实践）

- Working Memory 默认保留约 **50 条消息**；长时任务建议 **100–150 条** 或启用自动摘要。  
- **重要信息应显式写入长期记忆**，不要无限堆叠短期上下文。  
- Consolidation 会消耗额外 Token，高频场景建议**异步执行**，避免阻塞主链路。  

---

## 三、怎么做（本目录实现）

### 3.1 文件说明

```
test_memory/
├── 09-memory.md              # 理论教程（DeerFlow 记忆章节）
├── memory_core.py            # 共享核心：MemoryIndex / Checkpoint / Consolidation
├── demo_working_memory.py    # 样例 1：Working Memory
├── demo_longterm_memory.py   # 样例 2：Long-term Memory
├── demo_consolidation.py     # 样例 3：Memory Consolidation
├── demo_stream.py            # LangGraph Agent 基础（MCP + 流式）
└── service.py                # 矿井调度 Gradio 服务（参考用）
```

### 3.2 环境依赖

与 `demo_stream.py` 相同，需安装：

- `gradio`
- `langchain` / `langchain-core`
- `langgraph`

LLM 默认配置（`memory_core.py`）：

- 模型：`qwen3.5-27b`
- 地址：`http://10.11.3.210:9702/v1`

### 3.3 启动三个演示

```bash
cd test_memory

# 样例 1：Working Memory          → http://0.0.0.0:7870
python demo_working_memory.py

# 样例 2：Long-term Memory         → http://0.0.0.0:7871
python demo_longterm_memory.py

# 样例 3：Memory Consolidation     → http://0.0.0.0:7872
python demo_consolidation.py
```

### 3.4 推荐测试路径

#### 样例 1 — Working Memory

1. 保持 `thread_id = demo-thread-1`  
2. 输入：`我叫张三，是一名后端工程师`  
3. 再问：`我主要做什么工作？`（验证同 thread 内记住上文）  
4. 点击「刷新 Checkpoint 状态」，查看消息数、任务进度  
5. 刷新浏览器后，用**相同 thread_id** 继续对话，验证 Checkpoint 恢复  

#### 样例 2 — Long-term Memory

1. 打开「记忆库管理」，查看预置 5 条记忆  
2. 在「记忆检索」输入 `编程语言偏好`，查看 Top-K 结果  
3. 在「带 Recall 对话」问：`根据我的偏好推荐技术栈`  
4. 观察回复下方「本次召回的记忆」是否命中相关条目  

#### 样例 3 — Consolidation

1. 多轮对话，例如依次输入姓名、项目、偏好、会议习惯  
2. 观察右侧 Token 估算是否接近阈值  
3. 选择触发方式 `manual`，点击「执行整合」  
4. 查看整合报告：提取了哪些信息、写入了多少长期记忆、裁剪了多少条短期消息  
5. 下方长期记忆表应出现 `source=consolidation` 的新记录  

### 3.5 核心代码入口

| 能力 | 关键函数 / 类 | 文件 |
|------|---------------|------|
| 长期记忆存储与检索 | `MemoryRecord`, `MemoryIndex.search()` | `memory_core.py` |
| Working Memory 对话 | `build_working_memory_agent()`, `chat_with_working_memory()` | `memory_core.py` |
| 带 Recall 的对话 | `chat_with_recall()` | `memory_core.py` |
| 记忆整合 | `consolidate()`, `extract_key_information()`, `trim_working_memory()` | `memory_core.py` |

### 3.6 整合流程代码示意

```python
# 1. 读取 Working Memory
state = await agent.aget_state({"configurable": {"thread_id": thread_id}})
messages = state.values["messages"]

# 2. LLM 提取关键信息
extracted = await extract_key_information(messages)

# 3. 与长期记忆合并（相似度 > 0.85 则更新，否则新建）
merged = await merge_with_existing(extracted, GLOBAL_MEMORY_INDEX)

# 4. 裁剪 Working Memory，保留最近 4 条
trimmed = await trim_working_memory(agent, thread_id, keep_recent=4)
```

Recall 注入示意：

```python
hits = GLOBAL_MEMORY_INDEX.search(user_query, top_k=3)
system_prompt = f"{RECALL_SYSTEM}\n\n## 相关长期记忆\n{format(hits)}"
# 将 system_prompt 与用户消息一并送入 LLM
```

---

## 四、从演示到生产

本目录是**教学级简化实现**，与生产系统的差异：

| 维度 | 本演示 | 生产建议 |
|------|--------|----------|
| 向量检索 | 词袋 + 关键词 | Embedding 模型 + FAISS / Milvus / pgvector |
| Checkpoint 存储 | 内存 `MemorySaver` | Redis / PostgreSQL 持久化 |
| 图索引 | 未实现 | 实体关系图（NetworkX / 图数据库） |
| 权限隔离 | 未实现 | 租户 / 项目级 RBAC |
| Consolidation | 同步 LLM 调用 | 异步队列（Celery / 后台任务） |

更完整的企业级设计见 `09-memory.md` 第 9.6–9.7 节（知识库集成、项目级记忆、上下文压缩）。

---

## 五、延伸阅读

- 教程原文：`09-memory.md`
- LangGraph Checkpoint：[官方文档](https://langchain-ai.github.io/langgraph/concepts/persistence/)
- 下一章：上下文工程与 Token 预算管理（教程第十章）
