# nao_aqjc_mcp 与配套 Skill 使用说明

> 煤矿（淖尔壕）安全监测监控系统 MCP 服务 + Hermes Agent 配套查询技能
> 服务端：`nao_aqjc_mcp_service` @ `http://10.11.3.210:7666/mcp`
> 数据源：ClickHouse 库 `PS_NAO`（10.11.3.210:8123）

---

## 一、MCP 是什么

`nao_aqjc_mcp` 是一个基于 **FastMCP** 的 MCP（Model Context Protocol）服务，把矿井安全监控系统的
ClickHouse 数据库封装成 AI Agent 可调用的工具，提供：

- **环境参数实时/历史监测**：瓦斯（甲烷）/ CO / CO₂ / O₂ / 粉尘 / 烟雾 / 温湿度 / 风速 / 负压 / 水位
- **设备状态**：风门 / 风筒 / 主通风机 / 馈电器 / 断电器 / 设备开停
- **异常预警**：超限报警、断电报警、馈电异常、传感器断线、基站断电/不通、标校、超量程、超上/下限预警
- **分站运行排查**：在线/通信中断/供电故障

### 1. 六個核心工具

| 工具 | 用途 | 关键参数 |
|---|---|---|
| `get_system_time` | 获取服务器当前时间（相对时间查询的**第一步**）| 无参 |
| `query_station_point_tree` | 分站↔测点拓扑、枚举清单、编码消歧 | `fields` 字段投影（必用精确全称）|
| `query_todayornow_points` | 今日（00:00~now）或当前（近10分钟）测点明细/统计 | `today_or_now`、各 filters |
| `query_points_list` | 任意时间段历史明细 + 统计聚合 | `start_date` / `end_date` |
| `query_point_yb_info` | 测点异常/预警/标校/故障记录 | `point_code`、`real_status`（true=近12h）|
| `query_station_status` | 分站运行（通信）/供电状态 | `substation_code`（模糊）、`run_state`、`power_state` |

### 2. 通用过滤参数（todayornow / points_list 共用）

- **模糊匹配**（支持 str 或 list）：
  - `point_code_filters`（测点编码，相似度阈值 95）
  - `sensor_type_filters`（传感器类型，阈值 70）
  - `location_filters`（测点安装位置，阈值 70）
  - `substation_code_filters`（分站编码，模糊）
- **`numeric_filters`**：字典 `{"字段名": {"op": 操作符, "value": 值}}`
  - 可筛字段：`高量程`/`低量程`/`上限报警门限`/`上限解报门限`/`下限报警门限`/`下限解报门限`/`上限断电门限`/`上限复电门限`/`下限断电门限`/`下限复电门限`/`平均值`/`中位数`/`标准差`/`最小值`/`最大值`/`起始时间`/`结束时间`/`持续秒数`/`持续小时`/`状态`
  - 支持 op：`>` `>=` `<` `<=` `==` `=` `!=` `between`(value=[start,end]) `not_between` `in`(value=列表) `after`/`since`(=>=) `before`/`until`(=<=)
  - ⚠️ 状态字段名是 **`状态`**（不是"测点状态"，写错会静默返回空 `{}`）
  - 状态值：`正常` / `报警` / `断电` / `标校` / `传感器故障`
- **`statistics_filters`**：统计聚合项，避免拉海量明细
  - 可用：`总测点数` / `测点列表` / `传感器类型分布/个` / `测点状态分布/条`

### 3. query_station_point_tree 的 `fields` 投影（⚠️ 必须用精确全称）

| 正确值 | 含义 |
|---|---|
| `传感器类型列表` / `传感器类型数量` | 枚举/计数传感器类型（实测 18 类）|
| `测点编码列表` / `测点数量` | 枚举/计数全部测点编码（实测 266 个）|
| `测点安装位置列表` / `测点安装位置数量` | 枚举/计数安装位置 |
| `分站编码列表` / `分站数量` / `分站安装位置列表` / `分站安装位置数量` | 分站枚举/计数 |
| `测点位置——编码对应关系` | 测点位置→编码映射 |
| `分站编码-安装位置对应关系` | 分站编码→安装位置映射 |

> ❌ 写短名如 `fields="测点编码"` / `fields="传感器类型名称"` 会**静默返回 `{}`**。
> ⚠️ 不传任何参数 = 返回分站+测点全量（266 测点，payload 巨大），要清单务必单字段投影。

### 4. 返回结构

数据类工具统一返回：

```json
{"每日数据": {"2026-09-11": {<测点编码或statistics>: {...}}}, "总共天数": N}
```

看"当前值"取每测点的 `最新值` / `最新值时间` 字段。

### 5. 大响应自动压缩

- 单次返回 JSON 软上限 **30000 字符**（`MAX_JSON_SIZE`）。
- `query_station_status` 超限时自动压缩为按分站聚合的摘要（时段数/运行状态分布/供电状态分布/总持续小时/最新时段）。
- `query_point_yb_info` 超 20000 字符自动压缩为按测点/异常类型聚合摘要。
- 无记录返回 `{"message": "未查到异常信息"}`。

### 6. 服务端额外的 prompt 与 resource（Hermes 不自动加载）

| 类型 | 名称/URI | 说明 |
|---|---|---|
| prompt | `analysis_guide` | 数据分析工作流指南（时间对齐→意图路由→参数构建→结果解释）|
| resource | `docs://aqjc/data-dictionary` | 数据字典：字段名、操作符、统计项说明 |

### 7. 直连调用（curl / JSON-RPC 兜底）

Hermes 里 MCP 工具调不到时（`hermes mcp test nao_aqjc_mcp` 诊断），可走 HTTP JSON-RPC：

```bash
# 1. initialize 拿 session-id（-D 导出响应头，取 mcp-session-id，32位hex）
curl -sSk -D h.txt -X POST http://10.11.3.210:7666/mcp \
  -H 'content-type: application/json' -H 'accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"curl","version":"1.0"}}}'
SID=$(grep -i mcp-session-id h.txt | awk '{print $2}' | tr -d '\r')

# 2. 通知 initialized（202 空 body 正常）
curl -sSk -X POST http://10.11.3.210:7666/mcp -H "mcp-session-id: $SID" \
  -H 'content-type: application/json' -H 'accept: application/json, text/event-stream' \
  -d '{"jsonrpc":"2.0","method":"notifications/initialized"}'

# 3. 调工具（响应是 SSE：event: message + data: {json}，取 data 行再 json.loads，
#    结果在 result.content[0].text，本身是 JSON 字符串需二次解析）
curl -sSk -X POST http://10.11.3.210:7666/mcp \
  -H 'content-type: application/json' -H 'accept: application/json, text/event-stream' \
  -H "mcp-session-id: $SID" \
  -d '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"get_system_time","arguments":{}}}'
```

**并发陷阱**：同会话连发请求若未带唯一 `id` 或间隔过快，服务端可能丢/串响应（返回裸 32 位 hex 而非 SSE）。
对策：每次请求用不同 `id`、间隔约 1s；若某次返回裸 hex，说明会话状态坏了，重新 initialize 换新 sid（不要复用）。

---

## 二、配套 Skill：`nao-aqjc-quick-query`

位置：`~/.hermes/profiles/qyxtwork/skills/coal-mine/nao-aqjc-quick-query/SKILL.md`

这是针对该 MCP 的**实测迭代过的速查技能**，在 Hermes 里查询瓦斯/测点/分站数据时自动触发加载。
它的价值不在重复 MCP 说明，而在**避坑经验和调用纪律**：

### 1. 调用次数纪律（2026-09-11 实测：当日概览 7 次 → 最优 2 次）

| 场景 | 推荐链路 |
|---|---|
| 今日测点总览 | ① `query_todayornow_points {today_or_now:true, statistics_filters:["总测点数","传感器类型分布/个","测点状态分布/条"]}` 拿全局画像；② 按需 +1 次 `numeric_filters:{"状态":{"op":"in","value":["报警","断电","标校","传感器故障"]}}, statistics_filters:["测点列表"]` 拿全部非正常测点 |
| 多日/月统计 | `query_points_list`（时间窗 + 仅统计项）；单测点明细让用户点名后再拉 |
| 单/多测点明细 | `point_code_filters=[编码…]` **list 一次拉多个**，别逐测点循环 |
| 今日全分站状态 | `query_station_status`（start/end=当日）单次全量，响应自带 `分站安装位置`，不用再查 tree |
| 某分站现在在线吗 | `query_station_status {real_status:true, substation_code:…}` |
| 编码不确定 | 先 `query_station_point_tree {fields:"测点编码列表"}` 消歧，再查 |

### 2. 关键避坑点

- ⚠️ **`fields` 必须精确全称**（`测点编码列表`，不是 `测点编码`），否则静默空 `{}`。
- ⚠️ **实时数据延迟极大**（实测 ~8.5h）：`today_or_now=false`（近10分钟）可能返回空 `{"每日数据":{"…":{}}}`，**别直接判"无数据"**——先 `today_or_now=true` 看 `最新值时间` 确认真实写入截止，并主动告知用户数据截止时刻。
- ⚠️ **枚举测点别逐分站循环**：传 `substation_code` 实测返回全量分站明细（单分站过滤不裁剪）；要清单只用单字段 `fields` 投影。
- ⚠️ **同名测点多条**：同一安装位置可能挂多个测点（如"北翼采区变电所激光甲烷"同有 1100002 与 1100004 两个分站编码），聚合/展示按 `测点编码` 去重。
- ⚠️ **三套时间参数别混**：todayornow 用 `today_or_now`；历史用 `start_date`/`end_date`；异常/分站状态用 `start_time`/`end_time` + `real_status`。
- ⚠️ **`query_points_list` + `statistics_filters` 可能返回空统计**（实测），长窗口先小时间窗探针确认结构，别直接当有效数据。
- ⚠️ 模糊阈值各异：tree 过滤 >60；station_status >90；todayornow/points 位置/类型 70、编码 95。名称尽量用系统原始全称（如 "4-2煤掘进工作面"，别简写）。
- 数百测点的结果交给 `execute_code` 聚合，不肉眼数。

### 3. 测点编码速读（秒判类型，无需查库）

编码 `1101` 前缀后第 5-6 位：`KG`=开关量 / `MN`=模拟量；第 7-10 位为传感器类型码：

- KG：02=风门, 03=风筒状态, 04=设备开停, 08=烟雾, 09=断电器, 10=主通风机, 11=馈电器
- MN：01=环境瓦斯, 02=风速, 03=环境温度, 04=一氧化碳, 06=负压, 07=水池水位, 12=氧气, 13=二氧化碳, 14=粉尘, 18=湿度, 43=激光甲烷

例：`1101KG100800000026` = 开关量-烟雾；`1101MN000400002274` = 模拟量-一氧化碳。

### 4. 在 Hermes 里的使用方式

1. 直接用自然语言提问即可，例如："现在井下有哪些测点在报警？"、"1100012 分站现在在线吗？"、
   "今天 4-2煤掘进工作面的瓦斯数据怎么样？"、"最近 12 小时 XX 测点有没有异常记录？"
2. 技能自动加载，按纪律选工具、控调用次数、处理实时延迟。
3. 手动查看：Hermes 里执行 `skill_view(name='nao-aqjc-quick-query')`。
4. 排查连接：`hermes mcp ls`（看 enabled 状态）、`hermes mcp test nao_aqjc_mcp`（连接测试）。

---

## 三、典型问题 → 调用速查

| 问题 | 调用 |
|---|---|
| 今天测点整体情况 | `query_todayornow_points {today_or_now:true, statistics_filters:[…统计项…]}` |
| 现在（实时）某类气体读数 | `query_todayornow_points {sensor_type_filters:"激光甲烷", today_or_now:false}`（注意延迟，空则改 true 看最新值时间）|
| 哪些测点不正常 | `query_todayornow_points {today_or_now:true, numeric_filters:{"状态":{"op":"in","value":["报警","断电","标校","传感器故障"]}}, statistics_filters:["测点列表"]}` |
| 某测点历史曲线 | `query_points_list {start_date, end_date, point_code_filters:["<编码>"]}` |
| 某测点有没有异常/预警 | `query_point_yb_info {point_code:"<编码>", real_status:true}` |
| 全站近期异常概览 | `query_point_yb_info {real_status:true}` |
| 某分站在线/通信/供电 | `query_station_status {substation_code:"1100012", real_status:true}` |
| 有哪些分站/测点/类型 | `query_station_point_tree {fields:"分站编码列表"}` 等单字段投影 |
| 某分站挂哪些测点 | `query_station_point_tree {substation_code:"1100012", fields:["测点编码列表","传感器类型列表"]}` |

---

## 四、源码目录

MCP 服务端源码位于 `/home/user/公共/shihy/Nao_mcp`，目录结构与各模块说明见
**`Nao_mcp/目录结构说明.md`**（同目录）。
