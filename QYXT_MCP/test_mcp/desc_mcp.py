import asyncio
from fastmcp import Client

async def main():
    # 连接本地 MCP 服务
    client = Client("http://127.0.0.1:8677/mcp")

    async with client:
        # 查看服务器工具列表
        tools = await client.list_tools()

        print("可用工具:")
        for tool in tools:
            print(f"- {tool.name}")
            print(f"  描述: {tool.description}")

asyncio.run(main())