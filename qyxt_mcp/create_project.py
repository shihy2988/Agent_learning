# -*- coding: utf-8 -*-
'''
@File    : create_project.py.py
@IDE     : PyCharm
@Author  : shihongyu
@Date    : 2026/03/10
@Describe: 
'''
import os
from pathlib import Path

# 定义项目根目录名称
PROJECT_NAME = "mcp_camera_service"

# 定义目录结构 (键为文件夹路径，值为该文件夹下的文件列表)
PROJECT_STRUCTURE = {
    "": ["requirements.txt", "main.py", "database.py"],
    "models": ["__init__.py", "base.py", "camera.py", "other_models.py"],
    "tools": ["__init__.py", "camera_tools.py", "other_tools.py"],
    "utils": ["__init__.py", "image_helper.py"]
}


def create_structure(base_path: str, structure: dict):
    base_dir = Path(base_path)

    # 创建根目录
    base_dir.mkdir(parents=True, exist_ok=True)
    print(f"📁 创建根目录: {base_dir.absolute()}")

    for folder, files in structure.items():
        # 构建当前子目录路径
        current_dir = base_dir / folder
        if folder:  # 如果不是根目录，则创建子文件夹
            current_dir.mkdir(parents=True, exist_ok=True)
            print(f"  📁 创建子目录: {folder}/")

        # 创建文件
        for file_name in files:
            file_path = current_dir / file_name
            # 如果文件不存在则创建
            if not file_path.exists():
                with open(file_path, "w", encoding="utf-8") as f:
                    # 如果是 .py 文件，写入一行简单的注释作为占位
                    if file_name.endswith(".py"):
                        f.write(f"# {file_name} - 自动生成的模块文件\n")
                    elif file_name == "requirements.txt":
                        f.write("fastmcp\nsqlalchemy\n")
                print(f"    📄 创建文件: {file_path.relative_to(base_dir)}")
            else:
                print(f"    ⚠️ 文件已存在, 跳过: {file_path.relative_to(base_dir)}")


if __name__ == "__main__":
    print(f"🚀 开始生成项目结构: {PROJECT_NAME}\n" + "-" * 40)
    create_structure(PROJECT_NAME, PROJECT_STRUCTURE)
    print("-" * 40 + "\n✅ 项目目录结构创建完毕！你可以开始复制粘贴代码了。")