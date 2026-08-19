# 矿井人员车辆定位 MCP 系统

本项目为矿井安全生产场景提供 **人员定位 + 车辆定位** 的智能查询能力，通过 MCP（Model Context Protocol）将 ClickHouse 历史数据、实时 API 与 AI Agent 对接，支持自然语言查询井下人员/车辆状态、轨迹分析与区域巡检。

核心模块分为三层：

| 模块 | 职责 |
|------|------|
| `person_tools/` | MCP 工具层：对外暴露标准化 Tool / Resource |
| `person_utils/` | 数据层：ClickHouse 查询、轨迹分段、SQLite 日缓存、统计过滤 |
| `test_mcp/` | 应用层：Skill 编排、LangGraph Agent、Gradio 对话界面 |

---

## 目录结构

```
QYXT_MCP/
├── person_tools/          # MCP 服务注册与工具定义
│   ├── base_tool.py       # 基类：通用查询、Redis 缓存、轨迹分段
│   └── person_tool.py     # PersonnelMCPService：注册全部 MCP Tools
│
├── person_utils/          # 业务数据处理与缓存
│   ├── base_utils.py      # 模糊匹配、数值过滤、人员统计
│   ├── car_utils.py       # 车辆统计
│   ├── person_bases.py    # PersonBase：人员日缓存 + 多维过滤
│   ├── car_bases.py       # CarBase：车辆日缓存 + 多维过滤
│   ├── person_sqls.py     # ClickHouse SQL 模板
│   ├── person_analysis_cache.db   # 人员日缓存（运行时生成）
│   └── car_analysis_cache.db      # 车辆日缓存（运行时生成）
│
├── test_mcp/              # Agent 集成与 Skill 编排
│   ├── skill_loader.py    # Skill 加载器
│   ├── skills/            # Skill 定义（Markdown + YAML Front Matter）
│   ├── demo_stream.py     # LangGraph Agent 核心
│   ├── agent_gradio.py    # Gradio Web 对话界面
│   └── desc_mcp.py        # MCP 客户端探测脚本
│
└── main.py                # MCP 服务启动入口（HTTP :8677）
```

---

## 一、MCP 设计

### 1.1 设计目标

将矿井业务查询能力封装为 **大模型可直接调用的标准工具**，使 Agent 无需了解 ClickHouse SQL、外部 API 地址或字段含义，即可通过自然语言完成复杂查询。

### 1.2 架构分层

```
┌─────────────────────────────────────────────────────────┐
│  AI Agent (LangGraph + Skill)                           │
│  test_mcp/demo_stream.py                                │
└────────────────────┬────────────────────────────────────┘
                     │ HTTP MCP (FastMCP)
┌────────────────────▼────────────────────────────────────┐
│  MCP 工具层  person_tools/PersonnelMCPService           │
│  ├── @mcp.tool()   10+ 业务工具                          │
│  └── @mcp.resource 数据字典                              │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│  数据层  person_utils/                                   │
│  PersonBase / CarBase + SQLite 日缓存 + Redis 元数据缓存  │
└────────────────────┬────────────────────────────────────┘
                     │
┌────────────────────▼────────────────────────────────────┐
│  数据源                                                  │
│  ClickHouse (PS.HISTORY_PERSONNEL_LOCATION 等)          │
│  + 实时定位 API + 出入井 API + Redis                     │
└─────────────────────────────────────────────────────────┘
```

### 1.3 核心类：`PersonnelMCPService`

继承 `Base_tool`，在初始化时完成三件事：

1. **连接 ClickHouse** — 历史轨迹、站点字典
2. **实例化 `PersonBase` / `CarBase`** — 带 SQLite 缓存的复杂查询
3. **注册 MCP 能力** — `_register_resources()` + `_register_tools()`

```python
# main.py 启动方式
mcp = FastMCP(name="人员车辆查询mcp", version="1.0.0")
PersonnelMCPService(mcp=mcp, host=..., port=..., ...)
mcp.run(transport="http", port=8677, host="0.0.0.0")
```

### 1.4 三类 MCP 能力

#### Resource（静态知识）

| URI | 用途 |
|-----|------|
| `docs://personnel/data-dictionary` | 字段说明（NAME、AREANAME、CARDID 等），模型不确定字段含义时可查阅 |

#### Tool（业务查询，共 10 个）

| 工具名 | 场景 |
|--------|------|
| `get_system_time` | 相对时间换算基准（"昨天"、"三小时前"） |
| `query_person_underground_status` | 实时/今日井下人员分布 |
| `query_person_trajectory` | 单人轨迹（按入井段分组） |
| `query_personnel_list` | 多条件人员批量查询 + 统计 |
| `find_person_latest_entry` | 某人最近一次入井记录 |
| `query_car_underground_status` | 实时/今日井下车辆分布 |
| `query_car_trajectory` | 单车轨迹 |
| `query_cars_list` | 多条件车辆批量查询 + 统计 |
| `query_person_near_station` | 某站点附近人员 |
| `get_infos` | 基础字典（部门/人员/车辆/工种/区域/站点） |

#### 工具设计原则

1. **Docstring 即契约** — 每个 Tool 的 docstring 包含功能说明、参数格式、返回 JSON 结构示例，直接作为大模型的工具描述
2. **默认当日** — 未传时间参数时自动查询当天 `00:00:00 ~ 23:59:59`
3. **返回 JSON 字符串** — 统一 `json.dumps(..., ensure_ascii=False)`，便于模型解析
4. **分级压缩** — 数据超限时自动降级：
   - 轨迹 > 80KB → 精简为 `[班次, 位置, 开始, 结束]` 数组
   - 列表 > 30KB → 仅保留统计摘要
   - 仍超限 → 仅返回「总人数 + 入井次数」核心字段

### 1.5 双数据源融合

每个查询工具内部组合两类数据源：

| 类型 | 来源 | 用途 |
|------|------|------|
| 实时 API | `getLocationWeb` / `getCarLocationWeb` | 当前井下状态 |
| 历史 ClickHouse | `PS.HISTORY_PERSONNEL_LOCATION` | 轨迹分段、统计 |
| 出入井 API | `rydw_getHisInOutMineWeb_for_AI` | 入井/出井时间对齐 |
| Redis 缓存 | `mcp:person` / `mcp:car` 等 | 基础字典，7 天过期 |

### 1.6 MCP 设计优点

| 优点 | 说明 |
|------|------|
| **协议标准化** | 基于 FastMCP，任何支持 MCP 的客户端（Cursor、LangGraph、自定义 Agent）均可接入 |
| **业务与协议解耦** | Tool 只做参数适配和 JSON 序列化，复杂逻辑下沉到 `person_utils` |
| **自描述工具** | 详尽的 docstring 降低模型误用率，减少 Prompt 工程成本 |
| **Token 友好** | 内置多级数据压缩，避免大轨迹撑爆上下文窗口 |
| **可扩展** | `main.py` 预留 `register_other_tools(mcp)` 扩展点 |

---

## 二、SQLite 设计

### 2.1 设计目标

矿井历史查询（如「近 30 天某区域人员统计」）若每次都穿透 ClickHouse，成本高、延迟大。SQLite 作为 **本地日粒度原子缓存**，实现：

- 历史天数据一次查询、多次复用
- 过滤/统计在内存完成，不重复打库
- 今日数据始终实时查库，保证新鲜度

### 2.2 缓存策略：「日原子缓存 + 读时过滤」

```
请求 query_personnel_list(start=06-01, end=06-30, 部门=机电队)
        │
        ▼
┌─ 按天拆分 ─────────────────────────────────────────┐
│  06-01  06-02  ...  06-29  [今天]                  │
│    ↓      ↓            ↓      ↓                    │
│  SQLite SQLite      SQLite  跳过缓存，直接查 ClickHouse │
└────────────────────────────────────────────────────┘
        │
        ▼
  person_filter() 多线程过滤 + generate_statistics()
        │
        ▼
  {"每日数据": { "2026-06-15": { statistics: {...} } }}
```

**关键设计决策：缓存存全量、过滤在读取时做**

- 缓存 Key 仅为 `day_str`（如 `2026-06-15`），不绑定任何过滤条件
- 同一天的「查机电队」和「查掘进工」共享同一份缓存
- 避免「过滤条件组合爆炸」导致缓存失效

### 2.3 表结构

#### 人员缓存 — `person_analysis_cache.db`

```sql
CREATE TABLE IF NOT EXISTS person_atom_cache (
    day_str     TEXT PRIMARY KEY,   -- 日期，如 "2026-06-15"
    result_json TEXT                -- 当日全量人员数据 JSON
);
```

#### 车辆缓存 — `car_analysis_cache.db`

```sql
CREATE TABLE IF NOT EXISTS car_atom_cache (
    day_str     TEXT PRIMARY KEY,
    result_json TEXT
);
```

### 2.4 缓存 JSON 数据结构

以人员为例，每个 `day_str` 存储：

```json
{
  "张三_6153": {
    "姓名": "张三",
    "卡号": "6153",
    "部门": "综掘队",
    "工种": "掘进工",
    "班次": "早班",
    "职位": "班组长",
    "出入井记录": [
      {
        "入井时间": "2026-06-15T08:13:00",
        "出井时间": "2026-06-15T16:20:00",
        "入井时长(秒)": 29220,
        "入井地点": "副井口2号",
        "出井地点": "副井口2号",
        "具体轨迹变化": [
          {
            "区域名称": "43203回风掘面",
            "主站名称": "5煤一连巷18号",
            "轨迹开始时间": "2026-06-15 08:35:31",
            "轨迹结束时间": "2026-06-15 08:36:35",
            "停留时长/s": 64,
            "距离主站距离/m": 66.1
          }
        ]
      }
    ]
  }
}
```

数据生成链路：

```
ClickHouse 原始点位
    → SQL 窗口函数分段（主站变化 / 日期变化 → segment_id）
    → 出入井 API 对齐（classify_segments_by_inout）
    → 写入 SQLite
```

### 2.5 读取时过滤：`person_filter` / `car_filter`

过滤维度支持：

| 类别 | 字段示例 |
|------|----------|
| 文本模糊 | 姓名、部门、班次、工种、职位、区域、站点 |
| 地点 | 入井地点、出井地点 |
| 数值/时间 | `numeric_filters`：停留时长、主站距离、入井时长等 |
| 统计 | `statistics_filter`：按指定统计项返回 |

过滤实现特点：

- **fuzzywuzzy 模糊匹配** — 容忍用户输入不完全精确
- **多线程** — `ThreadPoolExecutor(max_workers=16)` 并行过滤
- **统计可选** — 传入 `statistics_filter` 时只返回 `statistics`，不返回原始明细

### 2.6 预热机制

`PersonBase` / `CarBase` 启动时自动开启后台线程：

```python
# 每天凌晨 2:00 自动预热近 30 天缓存
start_auto_analysis_thread()
  → _run_daily_auto_analysis()
    → get_person_infos_daytype_with_cache(start=30天前, end=今天)
```

用户白天查询历史区间时，大概率直接命中 SQLite，无需等待 ClickHouse 重算。

### 2.7 今日数据特殊处理

```python
if day == today_str:
    missed_days.append(day)  # 今日永远不走缓存，强制查库
```

历史天写入 SQLite 后不再更新；当天数据实时变化，保证查询准确性。

### 2.8 SQLite 设计优点

| 优点 | 说明 |
|------|------|
| **查询性能** | 历史天 O(1) 本地读取，30 天跨月统计从分钟级降到秒级 |
| **缓存复用率高** | 日原子设计使不同过滤条件共享同一份缓存 |
| **存储简单** | 单表 + JSON，无需维护复杂关系 schema |
| **过滤灵活** | 缓存与过滤解耦，新增过滤维度不改表结构 |
| **冷热分离** | 历史天走 SQLite，今天走 ClickHouse，兼顾性能与实时性 |
| **零运维** | 嵌入式 SQLite，无需额外部署数据库服务 |
| **自动预热** | 后台线程提前填充缓存，高峰期用户体验更稳定 |

---

## 三、Skill 设计

### 3.1 设计目标

MCP Tool 解决了「能查什么」，Skill 解决「怎么查」—— 指导大模型：

- 识别用户意图
- 选择合适的工具组合
- 正确处理时间、消歧、数据校验
- 输出可读的业务结论而非原始 JSON

### 3.2 Skill 文件格式

采用 **Markdown + YAML Front Matter**，与 Cursor Agent Skills 规范一致：

```markdown
---
name: person-vehicle-query
description: 矿井人员车辆查询的系统性方法论...
tools:
  - get_system_time
  - query_person_underground_status
  - query_person_trajectory
  ...
---

# Personnel & Vehicle Query Skill

## Overview
...
```

| 字段 | 作用 |
|------|------|
| `name` | 唯一标识，用于路由选择 |
| `description` | 简要说明，辅助意图匹配 |
| `tools` | 该 Skill 允许使用的 MCP 工具白名单 |
| 正文 `content` | 详细 SOP：意图识别、工具组合、输出规范 |

### 3.3 Skill 加载器：`SkillManager`

```python
# test_mcp/skill_loader.py
class SkillManager:
    def __init__(self, skill_dir="skills"):
        # 扫描 skills/*.md，解析 Front Matter
        ...

    def select_skill(self, query: str) -> Skill:
        
        return "person-vehicle-query"  # 默认
```

### 3.4 Skill 注入 Agent 的方式

```
用户提问 → SkillManager.select_skill()
         → 构造 skill_dict { name, description, tools, content }
         → 传入 LangGraph State: current_skill
         → llm_call 节点动态装配 SystemMessage
```

```python
# demo_stream.py 核心逻辑
if current_skill:
    # 1. 工具白名单裁剪 — 只绑定 Skill 声明的工具，降低误调用
    active_tools = [t for t in tools if t.name in current_skill["tools"]]

    # 2. 动态 System Prompt — 将 Skill 正文注入系统消息
    system_content = f"""你是矿山人员车辆智能助手...
    Loaded Skill: {current_skill['name']}
    {current_skill['content']}"""
```

### 3.5 Skill 内容结构（以 `person-vehicle-query` 为例）

| 章节 | 内容 |
|------|------|
| **When to Use** | 人员/车辆/区域/安全四类触发场景与示例 |
| **Core Principle** | 禁止单工具作答，提倡统计+明细+交叉验证 |
| **Phase 1: Intent Recognition** | 意图 → 主工具映射表 |
| **Phase 2: Information Collection** | 每个 Tool 的参数说明、`numeric_filters` / `statistics_filter` 示例 |
| **Phase 3: Validation** | 结果校验清单（消歧、时间、一致性） |
| **Search Strategy Rules** | 姓名消歧、相对时间处理、区域巡检流程 |
| **Output Guidelines** | 结构化中文输出模板 |
| **Common Mistakes** | 禁止猜测时间、禁止输出原始 JSON 等 |

### 3.6 三阶段查询方法论

```
Phase 1 意图识别
  "陈玉岭今天在哪" → query_person_underground_status + find_person_latest_entry
  "昨天张三轨迹"   → get_system_time → query_person_trajectory
  "机电队超时人员" → get_system_time → query_personnel_list + numeric_filters

Phase 2 信息收集
  多工具并行/串行调用，统计与明细兼顾

Phase 3 校验
  消歧 → get_infos
  时间核对 → get_system_time
  状态与轨迹一致性检查
```

### 3.7 Skill 设计优点

| 优点 | 说明 |
|------|------|
| **工具编排而非单点调用** | 避免模型只调一个 Tool 就草率作答 |
| **工具白名单** | 按场景裁剪可用工具，减少误调用和 Token 浪费 |
| **动态 Prompt 注入** | Skill 不污染多轮历史，每轮按需加载 |
| **可热更新** | 修改 `skills/*.md` 即可调整业务 SOP，无需改代码 |
| **可扩展** | 新增 Skill 只需添加 Markdown 文件 |
| **人机共建** | 业务专家可直接编写/审阅 Skill 文档 |
| **输出质量可控** | 明确的输出模板和 Quality Checklist 约束回答格式 |

---

## 四、三层协作流程

以用户提问 **「查询机电队今天井下的人员分布」** 为例：

```
1. test_mcp/agent_gradio.py
   └─ SkillManager 匹配 → person-vehicle-query
   └─ skill_dict 注入 LangGraph

2. demo_stream.py (Agent)
   └─ SystemMessage 加载 Skill SOP
   └─ 模型决策调用：
       ├─ get_system_time()
       └─ query_personnel_list(
              department_filters=["机电队"],
              start_date="今天 00:00:00",
              statistics_filter=["总人数","部门分布/人","区域分布/条"]
          )

3. MCP HTTP → person_tools/person_tool.py
   └─ query_personnel_list Tool
   └─ 调用 person_base.get_person_infos_daytype_with_cache()

4. person_utils/person_bases.py
   └─ 今日数据 → get_persons_by_filters() → ClickHouse
   └─ person_filter(department=机电队)
   └─ generate_statistics() → 返回统计

5. Agent 按 Skill Output Guidelines 组织中文回答
```

---

## 五、快速启动

### 启动 MCP 服务

```bash
cd QYXT_MCP
python main.py
# 服务监听 http://0.0.0.0:8677/mcp
```

### 探测 MCP 工具列表

```bash
cd test_mcp
python desc_mcp.py
```

### 启动 Gradio 对话界面

```bash
cd test_mcp
python agent_gradio.py
# 访问 http://0.0.0.0:7861
```

### 命令行流式测试

```bash
cd test_mcp
python demo_stream.py
```

---

## 六、依赖说明

| 组件 | 用途 |
|------|------|
| `fastmcp` | MCP 服务端 |
| `clickhouse-connect` | 历史轨迹查询 |
| `redis` | 基础字典缓存 |
| `fuzzywuzzy` | 模糊匹配 |
| `langgraph` + `langchain-mcp-adapters` | Agent 编排与 MCP 客户端 |
| `gradio` | Web 对话界面 |
| `pyyaml` | Skill Front Matter 解析 |
| `sqlite3`（标准库） | 日粒度分析缓存 |

---

## 七、设计总结

本项目的核心设计哲学是 **「协议标准化 + 数据预计算 + 知识流程化」**：

```
MCP 层    →  把复杂矿井数据能力封装为 AI 可理解的工具接口
SQLite 层 →  用日原子缓存换取历史查询性能，读时过滤保证灵活性
Skill 层  →  用文档化 SOP 约束 Agent 行为，保证查询质量和输出可读性
```

三层各司其职、互相解耦，使得系统同时具备：

- **对 AI 友好** — 标准 MCP + 自描述 Tool + Skill 编排
- **对业务友好** — 中文统计维度、出入井语义、区域巡检流程
- **对性能友好** — Redis 字典缓存 + SQLite 日缓存 + 分级数据压缩
- **对维护友好** — 业务逻辑在 `person_utils`，协议在 `person_tools`，流程在 `skills/*.md`，可独立演进
