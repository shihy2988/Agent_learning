from fastmcp import FastMCP
from tools.camera_tools import register_camera_tools
from tools.syg_tools import  PersonnelMCPService
from database import engine
from models.base import Base

# (可选) 初始化数据库表（如果数据库还未建表）
Base.metadata.create_all(bind=engine)

# 初始化 FastMCP 服务器
mcp = FastMCP(
    name="Camera_Management_Service",
    description="A service to manage and query camera database and statuses.",
    version="1.0.0"
)

# 注册各个模块的 Tools
register_camera_tools(mcp)
# register_other_tools(mcp) # 预留给未来扩展

if __name__ == "__main__":
    mcp.run(transport="http", port=8677,host="20.24.31.20")