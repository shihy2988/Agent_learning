from fastmcp import FastMCP
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tools.person_tools import PersonnelMCPService


# 初始化 FastMCP 服务器
mcp = FastMCP(
    "mcp_person_car_service",
    "A service to manage and query person or car database and statuses."
)

# 注册各个模块的 Tools
# register_camera_tools(mcp)
 # 实例化服务并自动注册工具
PersonnelMCPService(
    mcp=mcp,
    host="10.11.3.210",
    port=8123,
    database="PS",
    user="default",
    password="xt123456"
)

# register_other_tools(mcp) # 预留给未来扩展

if __name__ == "__main__":
    mcp.run(transport="http", port=8677,host="0.0.0.0")