#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
文件名: test_query_point_yb_info.py
描述: query_point_yb_info 工具的完整测试覆盖，含压缩分支。
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

        if isinstance(data, dict) and "message" in data and len(data) <= 3:
            print(f"⚠️ message: {data.get('message')}")
            if "total_nums" in data:
                print(f"   total_nums = {data['total_nums']}")
            return data
        if isinstance(data, dict) and "error" in data:
            print(f"❌ error: {data['error']}")
            return data

        # 正常结果：list 或 dict
        if isinstance(data, list):
            print(f"✅ 返回 {len(data)} 条异常记录")
            for rec in data[:3]:
                print(f"   ▸ {json.dumps(rec, ensure_ascii=False)[:200]}")
            if len(data) > 3:
                print(f"   ... 共 {len(data)} 条")
        elif isinstance(data, dict):
            print(f"✅ 返回 {len(data)} 个顶层 key")
            print(f"   keys = {list(data.keys())[:10]}")
            if "total_nums" in data:
                print(f"   total_nums = {data['total_nums']}")
            # 打印一个样本
            for k, v in data.items():
                if k in ("total_nums", "message"):
                    continue
                print(f"   ▸ 样本 {k} = {json.dumps(v, ensure_ascii=False)[:250]}")
                break
        else:
            print(f"✅ 返回: {str(data)[:500]}")
        return data
    except Exception as e:
        print(f"⚠️ 输出解析失败: {e}，原始返回: {str(result)[:500]}")
        return result


# ====================== 主测试 ======================
async def test_query_point_yb_info(mcp_app: FastMCP):
    print("\n" + "🔥" * 20)
    print("  query_point_yb_info 全量测试")
    print("🔥" * 20)

    summary = []

    async def run(title, params):
        try:
            res = await mcp_app.call_tool("query_point_yb_info", params)
            data = _pretty(title, res)
            if isinstance(data, dict) and "error" in data:
                status = "ERROR"
            elif isinstance(data, dict) and "message" in data and len(data) <= 3:
                status = "EMPTY"
            elif isinstance(data, dict) and "message" in data and "total_nums" in data:
                status = "COMPACT"
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

    now = datetime.now()
    yesterday = (now - timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    today_end = now.strftime("%Y-%m-%d %H:%M:%S")
    future_start = (now + timedelta(days=365)).strftime("%Y-%m-%d 00:00:00")
    future_end = (now + timedelta(days=366)).strftime("%Y-%m-%d 00:00:00")

    # # ============================================================
    # # 一、实时查询
    # # ============================================================
    # print("\n\n########## 一、实时查询（real_status=True） ##########")
    # await run("1.1 实时 — 全量", {"real_status": True})
    # await run("1.2 实时 — 指定测点",
    #           {"real_status": True, "point_code": "1101MN000200001635"})
    # await run("1.3 实时 — 不存在的测点 → 空",
    #           {"real_status": True, "point_code": "NOT_EXIST_XYZ"})

    # # ============================================================
    # # 二、历史查询
    # # ============================================================
    # print("\n\n########## 二、历史查询 ##########")
    # await run("2.1 历史 — 昨天至今 全量",
    #           {"start_time": yesterday, "end_time": today_end})
    # await run("2.2 历史 — 昨天至今 指定测点",
    #           {"point_code": "1101MN000200001635",
    #            "start_time": yesterday, "end_time": today_end})
    # await run("2.3 历史 — 仅 start_time", {"start_time": yesterday})
    # await run("2.4 历史 — 仅 end_time", {"end_time": today_end})

    # ============================================================
    # 三、无参数
    # ============================================================
    print("\n\n########## 三、无参数 ##########")
    await run("3.1 无参数", {})

    # ============================================================
    # 四、时间窗边界
    # ============================================================
    print("\n\n########## 四、时间窗边界 ##########")
    await run("4.1 未来时间窗 → 空",
              {"start_time": future_start, "end_time": future_end})
    await run("4.2 时间窗倒置 → 空或异常",
              {"start_time": today_end, "end_time": yesterday})
    await run("4.3 非法时间格式",
              {"start_time": "not-a-time", "end_time": "not-a-time"})

    # ============================================================
    # 五、压缩触发（宽时间窗 + 全量）
    # ============================================================
    print("\n\n########## 五、压缩触发 ##########")
    await run("5.1 近 30 天全量（大概率触发压缩）",
              {"start_time": (now - timedelta(days=30)).strftime("%Y-%m-%d 00:00:00"),
               "end_time": today_end})
    await run("5.2 近 7 天全量",
              {"start_time": (now - timedelta(days=7)).strftime("%Y-%m-%d 00:00:00"),
               "end_time": today_end})
    await run("5.3 近 90 天全量（大概率触发极简）",
              {"start_time": (now - timedelta(days=90)).strftime("%Y-%m-%d 00:00:00"),
               "end_time": today_end})

    # ============================================================
    # 六、参数组合
    # ============================================================
    print("\n\n########## 六、参数组合 ##########")
    await run("6.1 real_status=True 覆盖时间窗",
              {"real_status": True,
               "start_time": "2000-01-01 00:00:00",
               "end_time": "2000-01-02 00:00:00"})
    await run("6.2 real_status=False + 指定测点 + 时间窗",
              {"point_code": "1101MN000300000273", "real_status": False,
               "start_time": yesterday, "end_time": today_end})

    # ============================================================
    # 七、空字符串 / None
    # ============================================================
    print("\n\n########## 七、空值处理 ##########")
    await run("7.1 point_code='' 空串", {"point_code": ""})
    await run("7.2 point_code=None", {"point_code": None})
    await run("7.3 全 None", {"point_code": None, "start_time": None, "end_time": None})

    # ============================================================
    # 汇总
    # ============================================================
    print("\n\n" + "=" * 70)
    print("📊 测试汇总")
    print("=" * 70)
    total = len(summary)
    counts = {"OK": 0, "EMPTY": 0, "COMPACT": 0, "ERROR": 0, "EXCEPTION": 0}
    for _, s, _ in summary:
        counts[s] = counts.get(s, 0) + 1

    print(f"总用例: {total}")
    print(f"  ✅ OK        (完整返回): {counts['OK']}")
    print(f"  📦 COMPACT   (压缩摘要): {counts['COMPACT']}")
    print(f"  ⚠️ EMPTY     (无记录):   {counts['EMPTY']}")
    print(f"  ❌ ERROR     (返回错误): {counts['ERROR']}")
    print(f"  💥 EXCEPTION (Python异常): {counts['EXCEPTION']}")

    if counts["ERROR"] or counts["EXCEPTION"]:
        print("\n问题用例:")
        for title, status, detail in summary:
            if status in ("ERROR", "EXCEPTION"):
                print(f"  [{status}] {title}")
                print(f"      {str(detail)[:200]}")

    empty_or_compact = [t for t, s, _ in summary if s in ("EMPTY", "COMPACT")]
    if empty_or_compact:
        print("\n空/压缩用例（符合预期时可忽略）:")
        for t in empty_or_compact:
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
        password="xt123456",
    )

    asyncio.run(test_query_point_yb_info(mcp_app))