---
name: nao-aqjc-quick-query
description: "Use when 查询煤矿安全监控(nao_aqjc_mcp)的分站/测点/实时读数/历史数据/异常预警/设备状态. 选对工具+正确fields投影,避免裸拉全树."
---

# 安全监控系统（nao_aqjc_mcp）快速查询

MCP server: `nao_aqjc_mcp` @ `http://10.11.3.210:7666/mcp`（已 enabled）。服务端标识 `nao_aqjc_mcp_service v3.2.4`。煤矿井下环境参数（瓦斯/甲烷/CO/CO₂/O₂/粉尘/烟雾/温湿度/风速/负压/水位）+ 设备状态（风门/风筒/主通风机/馈电器/断电器等）实时监测与安全预警。

服务端额外注册了 **prompt**（`analysis_guide`，无参数据分析工作流指南）和 **resource**（`docs://aqjc/data-dictionary`，数据字典与查询指南），Hermes 不会自动加载，需 curl 手动获取。下方内容已整合两者的权威字段/操作符列表。

## 提速第一原则
1. **工具按需加载**：`tool_search` 找名字 → `tool_describe` 拿 schema → `tool_call`。已知 6 个核心工具名时，直接批量 `tool_describe(names=[...])` 再 `tool_call`，省掉 search。
2. **绝不裸拉全树**：`query_station_point_tree` 不传参 = 分站+测点全量（266 测点，巨大）。要清单 → 只传 `fields` 单字段投影。
3. **后处理聚合**：全量数据先 `execute_code` 聚合再呈现，别在脑子里数。

## 工具总览（先选对工具，再填参）

| 类别 | 工具 | 一句话职责 | 关键参数 |
|---|---|---|---|
| 基础 | `get_system_time` | 取服务器基准时间 | 无参 |
| 基础 | `query_station_point_tree` | 分站↔测点拓扑/枚举/消歧 | `fields` 投影 |
| 数据 | `query_todayornow_points` | 今日 / 近10分钟实时读数+报警 | `today_or_now` |
| 数据 | `query_points_list` | 历史区间明细+统计 | `start_date`/`end_date` |
| 异常 | `query_point_yb_info` | 测点异常/预警/标校/故障 | `real_status` |
| 异常 | `query_station_status` | 分站在线/通信/供电状态 | `substation_code` 模糊 |

**选型决策**：
- 相对时间（今天/昨天/最近N小时）→ **先 `get_system_time` 取基准**，再选数据工具。
- “现在实时值” → `query_todayornow_points`（`today_or_now=false` 近10分钟；注意实时延迟，见易错点）。
- “今天到现在” → `query_todayornow_points`（`today_or_now=true`）。
- “某时间段历史/统计” → `query_points_list`（显式时间窗）。
- “某测点有没有异常/报警/故障记录” → `query_point_yb_info`。
- “某分站是否在线/通信/供电是否正常” → `query_station_status`。
- “有哪些分站/类型/测点、某分站挂哪些测点、编码消歧” → `query_station_point_tree`。

## 真实参数 schema（字段名严格按此，勿臆造）

### get_system_time
`{}` 无参。返回 `{current_time:"YYYY-MM-DD HH:MM:SS", weekday:"Monday"}`。

### query_station_point_tree（基础信息/拓扑）
全部可选：`substation_code`(模糊>60) / `substation_location`(模糊>60) / `point_location`(模糊>60) / `point_code`(模糊>60) / `sensor_type`(模糊>60, str|list) / `fields`(str|list 投影) / `with_points`(默认True)。
- **`fields` 只能取以下精确值（带 列表/数量/对应关系 后缀；短名会返回 `{}`）**：
  `传感器类型列表` / `测点编码列表` / `测点安装位置列表` / `分站编码列表` / `分站安装位置列表` / `测点位置——编码对应关系` / `分站编码-安装位置对应关系` / `传感器类型数量` / `测点数量` / `测点安装位置数量` / `分站数量` / `分站安装位置数量`
- 无任何参数 → 返回全量枚举清单（多个 列表/数量/对应关系 字段），**payload 巨大，能避免就避免**。
- 仅 `fields` → 返回该字段去重集合（最轻）。
- 有其它过滤参数 → 按分站聚合返回测点明细（受 `fields` 投影控制）。

### query_todayornow_points（今日/实时）
- `point_code_filters`(str|list, 模糊阈值95) / `sensor_type_filters`(阈值70) / `location_filters`(阈值70) / `substation_code_filters`(模糊)
- `numeric_filters`(dict: `{"字段名": {"op": 操作符, "value": 值}}`
  - **可筛字段（来自 data-dictionary resource）**：`高量程`, `低量程`, `上限报警门限`, `上限解报门限`, `下限报警门限`, `下限解报门限`, `上限断电门限`, `上限复电门限`, `下限断电门限`, `下限复电门限`, `平均值`, `中位数`, `标准差`, `最小值`, `最大值`, `起始时间`, `结束时间`, `持续秒数`, `持续小时`, `状态`
  - **支持的 op**：`>`, `>=`, `<`, `<=`, `==`, `=`, `!=`, `between`(闭区间,value为[start,end]), `not_between`, `in`(列表), `after`/`since`(等同>=), `before`/`until`(等同<=)
- `statistics_filters`(str|list: `总测点数` / `测点列表` / `传感器类型分布/个` / `测点状态分布/条`)
- `today_or_now`(默认 **false**=近10分钟即当前；**true**=今日00:00~now)
- 返回 `{"每日数据":{day:{point_code|statistics:{...}}}, "总共天数":N}`；看“当前值”取每测点 `最新值` / `最新值时间` 字段。

### query_points_list（历史区间）
- `start_date` / `end_date`（格式 `YYYY-MM-DD HH:MM:SS`，默认当天 00:00:00~23:59:59）
- 其余 filters 与 todayornow 相同：`point_code_filters` / `sensor_type_filters` / `location_filters` / `substation_code_filters` / `numeric_filters` / `statistics_filters`
- 返回 `{"每日数据":{day:{...}}, "总共天数":N}`

### query_point_yb_info（测点异常/预警）
- `point_code`(精确, 可选; 不传=全部) / `start_time` / `end_time`(格式同上) / `real_status`(默认false; **true→忽略时间强制近12h**)
- 返回：≤30000字符给完整记录列表；>30000字符自动压缩为按测点/异常类型聚合摘要；无记录 `{"message":"未查到异常信息"}`。

### query_station_status（分站运行/供电状态）
- `substation_code`(模糊, 相似度>90) / `start_time` / `end_time` / `run_state`(`通信正常`/`通信中断`/`故障`/`未知`) / `power_state`(`交流供电`/`直流供电`/`电源故障`/`未知`) / `real_status`(默认false; **true→近12h**)
- 返回以分站编码为 Key 的分段状态记录字典。

## 调用次数纪律（2026-09-11 实测会话提炼：当日概览 7 次调用 → 最优 2 次）
1. **“今日测点数据”类总览 = 最多 2 次调用**：
   - 第 1 次：`query_todayornow_points {today_or_now:true, statistics_filters:["总测点数","传感器类型分布/个","测点状态分布/条"]}` 拿全局画像。
   - 第 2 次（按需）：`numeric_filters:{"状态":{"op":"in","value":["报警","断电","标校","传感器故障"]}}, statistics_filters:["测点列表"]}` 一次拿全非正常测点清单（实测返回 16 个）。
   - 不要按传感器类型逐个拉（环境瓦斯/激光甲烷/CO 分次拉 = 浪费 3 次），类型维度统计已在第 1 次返回。
2. **多日 / 一个月等长窗口：只出统计，不出逐测点明细**。`query_points_list`(时间窗) + 聚合统计项即可；用户要看单个测点时再提示其指定具体测点编码/位置做特定查询（单点 `point_code_filters` 明细 payload 巨大，须用户点名才拉）。
3. **单个测点数据 = 先向用户确认具体测点**（编码或安装位置），再 `point_code_filters` 单/多编码一次拉明细；不要替用户猜着拉。
4. **`point_code_filters` 支持 list，多测点明细一次调用**（实测 6 个编码一次返回全量统计），别逐测点循环。
5. **`query_station_status` 全量响应已含每分站 `分站安装位置`**，无需再调 `query_station_point_tree` 补映射（本会话多花 1 次）。
6. 长窗口统计项别乱填：`query_points_list` + `statistics_filters` 实测可能返回空 `{}`（见易错点），长窗口先小窗探针再扩。

## 常用调用样例（fields 值已修正为精确全称）
```jsonc
// 全量测点编码清单（最轻，单字段投影；问“有哪些测点”优先用这个）
query_station_point_tree { "fields": "测点编码列表" }              // 返回 266 个编码
// 枚举传感器类型清单（快，小 payload）
query_station_point_tree { "fields": "传感器类型列表" }             // 返回 18 类
// 某分站挂哪些测点（多字段投影）
query_station_point_tree { "substation_code": "1100012", "fields": ["测点编码列表","传感器类型列表"] }
// 当前(近10分钟)全部激光甲烷实时值
query_todayornow_points { "sensor_type_filters": "激光甲烷", "today_or_now": false }
// 今日某分站状态分布
query_todayornow_points { "substation_code_filters": "1100012", "statistics_filters": ["总测点数","传感器类型分布/个"], "today_or_now": true }
// 某测点近12h异常
query_point_yb_info { "point_code": "<编码>", "real_status": true }
// 某分站通信/供电状态
query_station_status { "substation_code": "1100012", "real_status": true }
```

## 常用组合套路
| 需求 | 工具链 |
|---|---|
| 今日测点数据总览（1~2 次） | `get_system_time` → `query_todayornow_points`(today_or_now=true, statistics_filters=[总测点数,传感器类型分布/个,测点状态分布/条])；按需 +1 次 numeric_filters 状态 in [报警,断电,标校,传感器故障] 拿非正常测点列表 |
| 多日/月统计 | `query_points_list`(时间窗, 仅统计项)；单测点明细提示用户点名后 `point_code_filters` 特定查询 |
| 单/多测点当日明细 | `query_todayornow_points`(point_code_filters=[编码…], today_or_now=true) 一次调用 |
| 今日全分站运行/供电 | `query_station_status`(start_time/end_time=当日) 单次全量，响应自带安装位置 |
| 今天某类气体是否超限 | `get_system_time` → `query_todayornow_points`(today_or_now=true, sensor_type_filters=["激光甲烷"]) |
| 某分站现在在线吗 | `query_station_status`(real_status=true, substation_code=…) |
| 近N天某工作面某参数历史 | `get_system_time` → `query_points_list`(时间窗, location_filters/sensor_type_filters) |
| 全部测点近期异常概览 | `query_point_yb_info`(real_status=true) |
| 不确定测点/分站编码 | 先 `query_station_point_tree`(fields=测点编码列表/分站编码列表) 消歧，再查 |

## 易错点
- **⚠️ `fields` 必须用精确全称（带后缀）**：实测 `fields="测点编码"` / `fields="传感器类型名称"` 均返回 `{}`（静默空）；正确值是 `测点编码列表` / `传感器类型列表` 等（见 schema 列表）。**这是旧版 skill 样例的真实 bug，已修正**。
- **枚举“有哪些测点”别逐分站循环**：`query_station_point_tree` 传 `substation_code` 实测返回的是**全量分站**明细（单分站过滤不裁剪），逐分站拉既慢又重复。要清单只用单字段 `fields` 投影；要“测点↔分站”归属，一次拉多字段投影（含 `测点位置——编码对应关系` 或 `分站编码-安装位置对应关系`）再本地 join。
- **实时数据延迟极大（实测 ~8.5h）**：2026-09-11 17:38 查“今日”，各测点数据仅写到 09:09:50（554 个采样≈每分钟1条，00:00~09:09）。`today_or_now=false`（近10分钟）返回空 `{"每日数据":{"2026-09-11":{}}}`。**别直接判“无数据”**，先 `today_or_now=true` 看 `最新值时间` 确认真实写入截止；回答“今日/当前值”时主动说明数据截止时刻，避免用户误以为数据是实时的。
- **同名测点多条**：同一安装位置可能挂多个测点（如“北翼采区变电所激光甲烷”同时有 1100002 与 1100004 两个分站编码）。聚合/展示按 `测点编码` 去重，别按位置名合并。
- **时间参数三套别混**：`todayornow_points` 用 `today_or_now`；历史用 `start_date`/`end_date`；异常/分站状态用 `start_time`/`end_time` + `real_status`。
- **模糊阈值各异**：tree 过滤阈值>60；station_status 相似度>90；todayornow/points 的位置/传感器类型阈值70、测点编码阈值95。名称尽量用系统原始全称（如"4-2煤掘进工作面"，别简写）。
- **numeric_filters 字段名/操作符**：完整列表已在 query_todayornow_points schema 段列出（来自 data-dictionary resource）。`op:"in"` + `value` 数组有效；**状态字段名是 `状态`，不是 `测点状态`**（2026-09-11 实测 `测点状态` 静默返回空 `{}`）。状态值：`正常/报警/断电/标校/传感器故障`（断线类事件记在测点状态序列里，非独立状态值）。`between` 的 value 为 `[start, end]` 闭区间。
- **`query_points_list` + `statistics_filters` 可能返回空统计**：2026-09-11 实测（时间窗=全天，sensor_type_filters=[环境瓦斯,激光甲烷]，statistics_filters=[测点列表,最大值,…]）返回 `每日数据` 里 statistics 为 `{}`，无测点明细。日级统计优先 `query_todayornow_points(today_or_now=true)`；长窗口（多日/月）用 points_list 前先拿小时间窗探针确认返回结构，别直接当有效数据。
- **全量后处理**：数百测点（2026-09 实测 266）的结果交给 `execute_code` 聚合计数，不肉眼数。
- **MCP 工具不在列表**：`enabled` 但调不到时，跑 `hermes mcp test nao_aqjc_mcp` 诊断，或走下方 curl 兜底。

## 测点编码快速解读（2026-09 实测验证）
编码 `1101` 前缀后第 5-6 位：`KG`=开关量 / `MN`=模拟量；第 7-10 位为传感器类型码：
- KG: 02=风门, 03=风筒状态, 04=设备开停, 08=烟雾, 09=断电器, 10=主通风机, 11=馈电器
- MN: 01=环境瓦斯, 02=风速, 03=环境温度, 04=一氧化碳, 06=负压, 07=水池水位, 12=氧气, 13=二氧化碳, 14=粉尘, 18=湿度, 43=激光甲烷
例: `1101KG100800000026`=开关量-烟雾；`1101MN000400002274`=模拟量-一氧化碳。按前缀分组即可秒判类型，无需查库。

## MCP prompts 和 resources（Hermes 不自动加载，需 curl 获取）

| 类型 | 名称/URI | 说明 |
|------|----------|------|
| prompt | `analysis_guide` | 数据分析工作流指南（无参，`prompts/get` 直接调用） |
| resource | `docs://aqjc/data-dictionary` | 数据字典：字段名、操作符、统计项、档案类型（`resources/read` 读取） |

curl 获取示例（需 session-id，见下方兜底）：
```bash
# 获取 prompt
curl -sSk -X POST http://10.11.3.210:7666/mcp -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' -H "mcp-session-id: <sid>" \
  -d '{"jsonrpc":"2.0","id":1,"method":"prompts/get","params":{"name":"analysis_guide","arguments":{}}}'

# 读取 resource
curl -sSk -X POST http://10.11.3.210:7666/mcp -H 'content-type: application/json' \
  -H 'accept: application/json, text/event-stream' -H "mcp-session-id: <sid>" \
  -d '{"jsonrpc":"2.0","id":2,"method":"resources/read","params":{"uri":"docs://aqjc/data-dictionary"}}'
```

注：上方 data-dictionary 的完整字段列表和操作符已整合到 query_todayornow_points schema 段中，无需每次都 curl 拉取。

## MCP 工具调不到时的 curl 兜底（2026-09-11 实测可用）
`hermes mcp ls` 显示 enabled 但本会话工具列表里没有 `mcp__nao_aqjc_mcp__*` 时，直接走 HTTP JSON-RPC：
1. `POST http://10.11.3.210:7666/mcp` 发 `initialize`，用 `curl -D h.txt` 导出响应头，取 `mcp-session-id`（32 位 hex）。
2. 带该 header 发 `notifications/initialized`（202 空 body 属正常）。
3. 每次 `tools/call` 都带 `mcp-session-id` header。响应是 SSE：`event: message\r\ndata: {json}`，取 `data:` 行再 json.loads，结果在 `result.content[0].text`（本身是 JSON 字符串，**需二次解析**）。
4. **并发陷阱**：同会话内连发多个未带唯一 `id` 的请求或间隔过快，服务端可能丢/串响应（返回裸 32 位 hex 而非 SSE）。每次请求用不同 `id`、间隔约 1s、失败即重新 initialize 换新 sid。
```bash
curl -sSk -X POST http://10.11.3.210:7666/mcp \
  -H 'content-type: application/json' -H 'accept: application/json, text/event-stream' \
  -H "mcp-session-id: <sid>" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_system_time","arguments":{}}}'
```
坑：若某次返回体不是 SSE 而是裸 32 位 hex 字符串，说明会话状态坏了，重新走 initialize 拿新 sid（不要复用旧 sid）。
