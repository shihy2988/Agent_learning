from fastmcp import FastMCP
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.aqjc_tools import AQJCMcpService


# 初始化 FastMCP 服务器
mcp = FastMCP(
    "nao_aqjc_mcp_service",
    "A service to manage and query 安全监测系统测点和分站 database and statuses."
)

# 注册各个模块的 Tools
# register_camera_tools(mcp)
 # 实例化服务并自动注册工具
AQJCMcpService(
    mcp=mcp,
    host="10.11.3.210",
    port=8123,
    database="PS_NAO",
    user="default",
    password="xt123456"
)

# register_other_tools(mcp) # 预留给未来扩展

if __name__ == "__main__":
    mcp.run(transport="http", port=7666,host="0.0.0.0")