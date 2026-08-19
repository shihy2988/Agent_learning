# -*- coding: utf-8 -*-
'''
@File    : call_tools.py
@IDE     : PyCharm
@Author  : shihongyu
@Date    : 2026/03/02
@Describe:
'''
import asyncio
from fastmcp import Client

client = Client("http://20.24.31.20:8677/mcp")

# async def call_tool(name: str):
#     async with client:
#         result = await client.call_tool("greet", {"name": name})
#         print(result)

async def call_tool(name: str):
    async with client:
        result = await client.call_tool("calculate", {"a": 5,"b":78,"op":"add"})
        print(result)


asyncio.run(call_tool("Ford"))