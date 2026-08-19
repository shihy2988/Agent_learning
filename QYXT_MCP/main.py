from fastmcp import FastMCP
from person_tools import PersonnelMCPService

def create_mcp_server():
    """初始化并返回 FastMCP 服务器实例"""
    mcp = FastMCP(
        name="人员车辆查询mcp",
        version="1.0.0"
    )
    # 注册选品相关 Tools
    PersonnelMCPService(
        mcp=mcp,
        host="10.11.22.80",
        port=9120,
        user="nethouse",
        password="CGC%EVXr.ET10Y_N",
        database="PS"
    )
    # 未来可扩展：
    # register_other_tools(mcp)
    return mcp

def main():
    mcp = create_mcp_server()
    mcp.run(transport="http", port=8677, host="0.0.0.0")

if __name__ == "__main__":
    main()