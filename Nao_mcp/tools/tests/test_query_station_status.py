#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件名: test_query_station_status.py
描述: query_station_status 工具的完整测试覆盖。
      包含: 实时/历史分支、substation_code 模糊过滤、run_state / power_state 过滤、
            多参数组合、空结果与边界。
"""

import asyncio
import json
import traceback
from datetime import datetime, timedelta

from fastmcp import FastMCP


# ====================== 输出美化 ======================
def _pretty(title: str, result):
    print("\n" + "=" * 70)
    print(f"🧪 {title}")
    print("=" * 70)
    try:
        # FastMCP 返回可能是 str / list / dict
        if isinstance(result, str):
            data = json.loads(result)
        elif isinstance(result, list):
            texts = []
            for item in result:
                if isinstance(item, dict) and "text" in item:
                    texts.append(item["text"])
                else:
                    texts.append(str(item))
            data = json.loads("".join(texts)) if texts else {}
        else:
            data = result

        if isinstance(data, dict):
            # 空结果 / 错误
            if "message" in data:
                print(f"⚠️ message: {data['message']}")
                return data
            if "error" in data:
                print(f"❌ error: {data['error']}")
                return data

            print(f"✅ 返回 {len(data)} 个分站")
            total_periods = 0
            for fz_code, periods in list(data.items())[:5]:
                pcount = len(periods) if isinstance(periods, list) else 0
                total_periods += pcount
                sample = periods[0] if pcount else None
                print(f"   ▸ {fz_code}: {pcount} 段")
                if sample:
                    print(f"       sample = {json.dumps(sample, ensure_ascii=False)[:200]}")
            if len(data) > 5:
                print(f"   ... 共 {len(data)} 个分站")

            # 状态分布摘要
            run_counter, power_counter = {}, {}
            for periods in data.values():
                if not isinstance(periods, list):
                    continue
                for p in periods:
                    if not isinstance(p, dict):
                        continue
                    rs = p.get("分站运行状态", "-")
                    ps = p.get("分站供电状态", "-")
                    run_counter[rs] = run_counter.get(rs, 0) + 1
                    power_counter[ps] = power_counter.get(ps, 0) + 1
            if run_counter:
                print(f"   运行状态分布: {run_counter}")
            if power_counter:
                print(f"   供电状态分布: {power_counter}")
        else:
            print(f"✅ 返回: {str(data)[:500]}")
        return data
    except Exception as e:
        print(f"⚠️ 输出解析失败: {e}，原始返回: {str(result)[:500]}")
        return result


# ====================== 主测试 ======================
async def test_query_station_status(mcp_app: FastMCP):
    print("\n" + "🔥" * 20)
    print("  query_station_status 全量测试")
    print("🔥" * 20)

    summary = []

    async def run(title, params):
        try:
            res = await mcp_app.call_tool("query_station_status", params)
            data = _pretty(title, res)
            # 判定 OK / 空 / 错误
            if isinstance(data, dict) and ("error" in data):
                status = "ERROR"
            elif isinstance(data, dict) and ("message" in data) and len(data) == 1:
                status = "EMPTY"
            else:
                status = "OK"
            summary.append((title, status, data))
            return data
        except Exception as e:
            print("\n" + "=" * 70)
            print(f"❌ {title} 异常")
            print("=" * 70)
            print(traceback.format_exc())
            summary.append((title, "EXCEPTION", str(e)))
            return None

    # ============================================================
    # 一、实时查询（real_status=True）
    # ============================================================
    print("\n\n########## 一、实时查询（real_status=True） ##########")

    # await run("1.1 实时 — 全量（无过滤）",
    #           {"real_status": True})

    # await run("1.2 实时 — 指定分站编码 1100001",
    #           {"real_status": True, "substation_code": "1100001"})

    # await run("1.3 实时 — 分站编码前缀模糊 110",
    #           {"real_status": True, "substation_code": "110"})

    # await run("1.4 实时 — 运行状态=通信正常",
    #           {"real_status": True, "run_state": "通信正常"})

    # await run("1.5 实时 — 运行状态=通信中断",
    #           {"real_status": True, "run_state": "通信中断"})

    # await run("1.6 实时 — 供电状态=交流供电",
    #           {"real_status": True, "power_state": "交流供电"})

    # await run("1.7 实时 — 供电状态=直流供电",
    #           {"real_status": True, "power_state": "直流供电"})

    # await run("1.8 实时 — 运行+供电 组合过滤",
    #           {"real_status": True, "run_state": "通信正常", "power_state": "直流供电"})

    # await run("1.9 实时 — 分站+运行状态",
    #           {"real_status": True, "substation_code": "1100001",
    #            "run_state": "通信正常"})

    # # ============================================================
    # # 二、历史查询（指定时间窗口）
    # # ============================================================
    # print("\n\n########## 二、历史查询 ##########")

    now = datetime.now()
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    today_end = now.strftime("%Y-%m-%d %H:%M:%S")

    # await run("2.1 历史 — 昨天至今 全量",
    #           {"start_time": yesterday, "end_time": today_end})

    # await run("2.2 历史 — 昨天至今 指定分站",
    #           {"substation_code": "1100001",
    #            "start_time": yesterday, "end_time": today_end})

    # await run("2.3 历史 — 昨天至今 运行状态=通信中断",
    #           {"start_time": yesterday, "end_time": today_end,
    #            "run_state": "通信正常"})

    # await run("2.4 历史 — 昨天至今 供电状态=电源故障",
    #           {"start_time": yesterday, "end_time": today_end,
    #            "power_state": "电源故障"})

    # await run("2.5 历史 — 仅 start_time（默认最新）",
    #           {"start_time": yesterday})

    # await run("2.6 历史 — 仅 end_time",
    #           {"end_time": today_end})

    # # ============================================================
    # # 三、无参数（默认走历史分支，无时间窗）
    # # ============================================================
    # print("\n\n########## 三、无参数 ##########")

    # await run("3.1 无参数（默认 real_status=False 且无时间窗）", {})

    # # ============================================================
    # # 四、substation_code 模糊匹配
    # # ============================================================
    # print("\n\n########## 四、substation_code 模糊匹配 ##########")

    # await run("4.1 substation_code=1100002 (精确)",
    #           {"real_status": True, "substation_code": "1100002"})
    # await run("4.2 substation_code=11000 (多位前缀)",
    #           {"real_status": True, "substation_code": "11000"})
    # await run("4.3 substation_code=不存在XYZ",
    #           {"real_status": True, "substation_code": "不存在XYZ"})
    # await run("4.4 substation_code='' 空串 → 视为无过滤",
    #           {"real_status": True, "substation_code": ""})

    # # ============================================================
    # # 五、run_state 穷举
    # # ============================================================
    # print("\n\n########## 五、run_state 穷举 ##########")

    # for rs in ["通信正常", "通信中断", "故障", "未知"]:
    #     await run(f"5.x real_status=True + run_state={rs}",
    #               {"real_status": True, "run_state": rs})

    # # ============================================================
    # # 六、power_state 穷举
    # # ============================================================
    # print("\n\n########## 六、power_state 穷举 ##########")

    # for ps in ["交流供电", "直流供电", "电源故障", "未知"]:
    #     await run(f"6.x real_status=True + power_state={ps}",
    #               {"real_status": True, "power_state": ps})

    # # ============================================================
    # # 七、组合过滤（多参数）
    # # ============================================================
    # print("\n\n########## 七、组合过滤 ##########")

    # await run("7.1 分站 + 运行 + 供电 三条件",
    #           {"real_status": True, "substation_code": "1100001",
    #            "run_state": "通信正常", "power_state": "直流供电"})

    # await run("7.2 时间窗 + 分站 + 运行",
    #           {"substation_code": "1100001",
    #            "start_time": yesterday, "end_time": today_end,
    #            "run_state": "通信正常"})

    # await run("7.3 时间窗 + 运行 + 供电",
    #           {"start_time": yesterday, "end_time": today_end,
    #            "run_state": "通信正常", "power_state": "交流供电"})

    # # ============================================================
    # # 八、边界与异常
    # # ============================================================
    # print("\n\n########## 八、边界与异常 ##########")

    # real_status 覆盖 start/end（无论传入什么都应被覆盖）
    await run("8.1 real_status=True 且传入时间窗 → 应被覆盖为近12h",
              {"real_status": True,
               "start_time": "2000-01-01 00:00:00",
               "end_time": "2000-01-02 00:00:00"})

    # 未来时间窗 → 应无结果
    future_start = (now + timedelta(days=365)).strftime("%Y-%m-%d 00:00:00")
    future_end = (now + timedelta(days=366)).strftime("%Y-%m-%d 00:00:00")
    await run("8.2 未来时间窗 → 无结果",
              {"start_time": future_start, "end_time": future_end})

    # 时间窗倒置（end < start）
    await run("8.3 时间窗倒置 (end < start)",
              {"start_time": today_end, "end_time": yesterday})

    # 非法时间格式 → 由 SQL 或空结果兜底
    await run("8.4 非法时间格式 'not-a-time'",
              {"start_time": "not-a-time", "end_time": "not-a-time"})

    # 不存在的运行状态
    await run("8.5 不存在的 run_state → 空结果",
              {"real_status": True, "run_state": "绝无此状态"})

    # 不存在的供电状态
    await run("8.6 不存在的 power_state → 空结果",
              {"real_status": True, "power_state": "绝无此状态"})

    # 组合不存在 → 空结果
    await run("8.7 substation_code 不存在 + run_state 正常 → 空结果",
              {"real_status": True, "substation_code": "ZZZ_NOT_EXIST",
               "run_state": "通信正常"})

    # ============================================================
    # 汇总
    # ============================================================
    print("\n\n" + "=" * 70)
    print("📊 测试汇总")
    print("=" * 70)
    total = len(summary)
    ok = sum(1 for _, s, _ in summary if s == "OK")
    empty = sum(1 for _, s, _ in summary if s == "EMPTY")
    err = sum(1 for _, s, _ in summary if s == "ERROR")
    exc = sum(1 for _, s, _ in summary if s == "EXCEPTION")

    print(f"总用例: {total}")
    print(f"  ✅ OK:        {ok}")
    print(f"  ⚠️ EMPTY:     {empty}  (预期空结果 / 过滤条件无命中)")
    print(f"  ❌ ERROR:     {err}  (返回 error 字段)")
    print(f"  💥 EXCEPTION: {exc}  (Python 异常)")

    if err or exc:
        print("\n问题用例列表:")
        for title, status, detail in summary:
            if status in ("ERROR", "EXCEPTION"):
                print(f"  [{status}] {title}")
                print(f"      {str(detail)[:200]}")

    # 空结果用例参考（便于目检）
    empty_titles = [t for t, s, _ in summary if s == "EMPTY"]
    if empty_titles:
        print("\n以下用例返回空（符合预期时可忽略）:")
        for t in empty_titles:
            print(f"  - {t}")


# ====================== 启动 ======================
if __name__ == "__main__":
    import sys
    import os
    sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    from aqjc_tools import AQJCMcpService   # ★ 换成你的模块名

    mcp_app = FastMCP("MineAQJCService")
    AQJCMcpService(
        mcp=mcp_app,
        host="10.11.3.210",
        port=8123,
        database="PS_NAO",
        user="default",
        password="xt123456"
    )

    asyncio.run(test_query_station_status(mcp_app))