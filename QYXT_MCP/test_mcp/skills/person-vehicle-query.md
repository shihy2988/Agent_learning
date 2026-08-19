---
name: person-vehicle-query
description: Use this skill for ANY personnel or vehicle monitoring request in the mine system. Trigger on queries such as "where is someone", "who is underground", "vehicle status", "person trajectory", "area inspection", or "abnormal monitoring". Provides systematic multi-tool orchestration for personnel and vehicle data instead of isolated tool calls.
---

# Personnel & Vehicle Query Skill

## Overview

This skill provides a systematic methodology for querying and analyzing personnel and vehicle information in the mine monitoring system.

**Always load this skill BEFORE starting personnel, vehicle, trajectory, or area analysis tasks.**

Do not rely on a single tool call. Most user questions require combining multiple tools and synthesizing results.

---

## When to Use This Skill

### Personnel Queries

Use when the user asks:

* Where is someone now
* Is someone underground
* When did someone enter the mine
* Show personnel trajectory
* Show personnel activity
* Find personnel near a location

Examples:

* 陈玉岭现在在哪里
* 张三是否在井下
* 查看李四今天轨迹
* 谁在43203回风区域

---

### Vehicle Queries

Use when the user asks:

* Vehicle status
* Vehicle location
* Vehicle trajectory
* Underground vehicle information

Examples:

* 查看井下车辆
* 9号自行车在哪里
* 查询运输车轨迹

---

### Area Queries

Use when the user asks:

* Area inspection
* Area personnel count
* Area vehicle count
* Area monitoring

Examples:

* 查看43203回风掘面情况
* 5煤一连巷有多少人
* 当前某区域车辆分布

---

### Safety / Monitoring Queries

Use when the user asks:

* Abnormal situations
* Long underground duration
* Low battery conditions
* Personnel concentration
* Real-time monitoring

Examples:

* 当前是否有异常
* 查看超时人员
* 查看低电量车辆

---

## Core Principle

**Never answer based on a single tool result when multiple tools can improve accuracy.**

Prefer:

* statistics + details
* trajectory + summary
* personnel + vehicle correlation

instead of isolated information.

---

## Query Methodology

### Phase 1: Intent Recognition

Determine the user intent category:

| Intent                             | Primary Tool                           |
|------------------------------------|----------------------------------------|
| today or now person Status         | query_person_underground_status        |
| Person lateset Status              | find_person_latest_entry               |
| One Person Trajectory              | query_person_trajectory                |
| Multi Person Trajectory            | query_personnel_list                   |
| Multiple conditions Personnel List | query_personnel_list                   |
| today or now Vehicle Status        | query_car_underground_status           |
| One Vehicle Trajectory             | query_car_trajectory                   |
| Multi Vehicle Trajectory           | query_cars_list                        |
| Multiple Vehicle   List            | query_cars_list                        |
| Area Inspection                    | query_personnel_list + query_cars_list |
| Nearby Personnel or  Vehicle       | query_person_near_station              |
| get Personnel  or  Vehicle infos   | get_infos                              |


If time expressions appear:

Examples:

* yesterday
* last week
* three hours ago
* today morning

First call:

get_system_time

to calculate precise time ranges.

---

### Phase 2: Information Collection

针对每一个 tool，说明如下：

#### 1. query_person_underground_status
- 用途：查询当前在井下的人员状态。
- 典型参数：`now_only=true` 用于仅返回当前地下人员，无需历史数据；`now_only=false` 用于返回当天地下人员数据。
- 使用场景：判断某人是否目前在井下。

#### 2. query_personnel_list
- 用途：根据筛选条件（姓名、部门、日期范围等）获取人员出入井明细和统计。
- 典型传入参数示例：

    - 当有判断条件时，需要传入numeric_filters,其支持字段如下，操作符有 >, >=, <, <=, ==, between, not_between ：
      - "入井时间": str
      - "出井时间": str
      - "入井时长": str
      - "入井时长(秒)": str
      - "轨迹开始时间": str
      - "轨迹结束时间": str
      - "距离主站距离/m": float
      - "距离分站距离/m": float
      - "变化次数": int
      - "停留时长/s": int
    最终的样式如下：
    ```python
    numeric_filters = {
        "距离主站距离/m": {
            "op": "<=",
            "value": 150
        },
        "停留时长/s": {
            "op": "between",
            "value": [300, 1800]
        }
    }
    ```
           
     - 当有数量统计时，需要传入statistics_filter: 控制返回哪些统计信息的字段，需要精确对应字段，具体字段如下:（每次选择时不超过6个，最好精确到1-3个）：
        [
            "总人数",
            "人员列表_姓名_卡号_入井次数",
            "入井时长分布/人次",
            "入井时间段分布/人次",
            "出井时间段分布/人次",
            "入井地点分布/人次",
            "出井地点分布/人次",
            "区域分布/条",
            "主站分布/条",
            "分站分布/条",
            "站点停留时长分布/条",
            "部门分布/人",
            "职位分布/人",
            "工种分布/人",
            "班次分布/人",
        ]
      为["all"] 时，返回全部统计项。

     - 使用场景：人员多条件检索、历史轨迹、多天数据统计、区域巡检人员列表。

#### 3. find_person_latest_entry
- 用途：获取人员的最近一次入井或出井状态。
- 典型参数：支持按姓名、卡号等查询。
- 使用场景：快速查找某人的最新井下动态。

#### 4. query_person_trajectory
- 用途：获取某人在指定时间段内的详细轨迹。
- 典型参数：人员姓名（或卡号）、起止时间。
- 使用场景：分析某人一天的轨迹详情，如果分析多天使用query_personnel_list。

#### 5. query_car_underground_status
- 用途：查询当前在井下运行的车辆状态。
- 典型参数：`now_only=true` 用于仅返回当前地下车辆列表，`now_only=true` 用于返回当天的地下车辆列表。
- 使用场景：车辆当前状态统计。

#### 6. query_cars_list
- 用途：车辆多条件检索、历史统计。
- 典型参数：车辆编号、类型、所属部门、时间范围等过滤。
    - 当有判断条件时，需要传入numeric_filters,支持字段，操作符有 >, >=, <, <=, ==, between, not_between 等：
      - "入井时间": str
      - "出井时间": str
      - "入井时长": str
      - "入井时长(秒)": str
      - "轨迹开始时间": str
      - "轨迹结束时间": str
      - "距离主站距离/m": float
      - "距离分站距离/m": float
      - "变化次数": int
      - "停留时长/s": int
    使用时需要根据字段条件生成下面的numeric_filters
     ```python
    numeric_filters = {
        "距离主站距离/m": {
            "op": "<=",
            "value": 150
        },
        "停留时长/s": {
            "op": "between",
            "value": [300, 1800]
        },...
    }
    ```
    - 当有数量统计时，需要传入statistics_filter: 控制返回哪些统计信息的字段，需要精确对应字段，具体字段如下:（每次选择时不超过5个，最好精确到1-2个）：
        - "总车辆数"
        - "车辆总览"
        - "车辆列表_名称_编号_出入井次数"
        - "出入井时长分布/辆次"
        - "入井时间段分布/辆次"
        - "出井时间段分布/辆次"
        - "入井地点分布/辆次"
        - "出井地点分布/辆次"
        - "区域分布/条"
        - "主站分布/条"
        - "分站分布/条"
        - "站点停留时长分布/条"
        - "所属部门分布/辆"
        - "车辆类型分布/辆"
   
    以 ["all"] 时，返回全部车辆统计项。
        
    - 使用场景：车辆历史轨迹、类型统计、区域统计，多车列表获取。

#### 7. query_car_trajectory
- 用途：获取车辆在指定时间段的详细轨迹。
- 典型参数：车辆编号、起止时间。
- 使用场景：车辆路线回溯、小时级行驶分析、日级分析，超过一日使用query_cars_list。

#### 8. get_infos
- 用途：模糊查询人/车基本信息，辅助消歧或补全信息。
- 典型参数：
    ```json
    {
        "type": "person" 或 "car",
        "name": "<输入的姓名或车辆名>"
    }
    ```
- 使用场景：当名字、编号等信息模糊或用户给出的有歧义时，调用此工具获得候选列表。

#### 9. query_person_near_station
- 用途：查询指定站点附近的人员或车辆。
- 典型参数：站点名称、半径范围等。
- 使用场景：现场调度、应急响应，“某站附近有哪些人在活动”。

#### 10. get_system_time
- 用途：获取当前系统/服务器时间，用于进行各种时间表达的精确换算。
- 典型用法：遇到“昨天”、“三小时前”等相对时间表达时，需先取系统时间，再配合进行时间区间计算。

---

**信息收集基本思路：**

- 通过上述工具组合使用，获取多源信息，交叉验证，保障结果准确。
- 不依赖单一接口的结果；如可用时，至少2个视角获取信息（如人员统计+明细、车辆状态+轨迹）。

---

**案例示范：**

- 人员当前状态查询
    1. 先用 query_person_underground_status(now_only=true) 获取最新在井下状态。
    2. 可用 find_person_latest_entry 或 query_personnel_list 进一步交叉验证。
    3. 可对比实时列表确保准确性。

- 区域巡检
    1. 调用 query_personnel_list 获取当前区域人员。
    2. 调用 query_cars_list 获取车辆。
    3. 按部门、工种、类型等聚合统计。

- 轨迹分析
    1. 先用 get_system_time 算出需查询的时间区间。
    2. 用 query_person_trajectory 或 query_car_trajectory 拉取细节。
    3. 汇总轨迹，分析行为（如是否覆盖所有必经点、驻留时长等）。

- 超时分析
    1. 先用 get_system_time 算出需查询的时间区间。
    2. 用 query_personnel_list 或 query_cars_list 拉取细节，传入numeric_filters = {
        "入井时长(秒)": {
            "op": ">",
            "value": 28800
        },
      
    }
    3. 汇总信息，分析行为 是否超过8小时。

- 情况分析
    1. 先用 get_system_time 算出需查询的时间区间。
    2. 用 query_personnel_list 或 query_cars_list 拉取细节。
    3. 汇总信息，分析情况。
---

### Phase 3: Validation

生成最终结果前需严格校验：

* 目标对象是否唯一并被查找到（防歧义）
* 返回数据是否有歧义、重名、重复等问题
* 时间区间是否和用户意图严格匹配
* 获取的数据是否完整覆盖
* 当前状态与轨迹数据是否一致（如状态为"已出井"但轨迹显示在井下需进一步核查）

如校验不通过：

- 自动调用 get_infos 进一步消歧
- 自动扩展时间区间或补充明细列表
- 再次拉取缺失数据

---

## Search Strategy Rules

### Person Name Ambiguity

若精确匹配姓名失败，则需按如下方式处理：

1. 调用 get_infos 工具，参数示例：

    ```json
    {
        "type": "person",
        "name": "<输入内容>"
    }
    ```

2. 对返回的人名候选结果做模糊匹配和 disambiguation。

3. 明确用户提问目标后再发起后续工具调用。

---

### Time Processing

Relative time expressions must not be guessed.

Examples:

User:

昨天张三去了哪里

Workflow:

Step1:

get_system_time

Step2:

calculate actual datetime

Step3:

query trajectory

---

### Area Queries

Area inspection should include:

Personnel:

* count
* department
* work type

Vehicle:

* count
* type
* status

---

## Output Guidelines

Prefer summarized information first.

Good example:

人员状态：

姓名：陈玉岭

状态：井下

当前位置：43203回风区域

入井时间：

2026-07-06 08:13:00

部门：

机电队

工种：

维修工

---

区域情况：

区域：

43203回风区域

人员数量：

18

车辆数量：

4

主要人员：

* 张三（掘进工）
* 李四（班组长）

车辆：

* 防爆运输车
* 9号自行车

---

## Quality Checklist

Before responding ensure:

* [ ] correct person or vehicle identified
* [ ] time range processed correctly
* [ ] multiple relevant tools used where needed
* [ ] statistics and details are both available
* [ ] results are concise and readable

---

## Common Mistakes to Avoid

* ❌ Using only one tool when multiple tools improve confidence
* ❌ Guessing time ranges
* ❌ Returning raw JSON
* ❌ Showing internal tool execution details
* ❌ Ignoring ambiguity in names
* ❌ Ignoring missing information
* ❌ Dumping full trajectory data without summarization

---

## Output

After completing execution provide:

1. Current status information
2. Key statistics
3. Summary insights
4. Important alerts if detected

Only expose useful conclusions and readable summaries.
