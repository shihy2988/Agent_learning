#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# 图表 MCP 服务 - tmux 后台启动脚本
# ============================================================
# 用法:
#   ./start_chart.sh start    启动服务
#   ./start_chart.sh status   查看状态
#   ./start_chart.sh attach   进入会话查看日志
#   ./start_chart.sh stop     停止服务
#   ./start_chart.sh restart  重启服务

SESSION_NAME="${SESSION_NAME:-chart_mcp}"
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON_BIN="${PYTHON_BIN:-python}"
MAIN_SCRIPT="mains/main_chart.py"
SERVICE_PORT=9900

need_tmux() {
    if ! command -v tmux >/dev/null 2>&1; then
        echo "错误: tmux 未安装。直接运行: cd '$PROJECT_DIR' && $PYTHON_BIN $MAIN_SCRIPT"
        exit 1
    fi
}

session_exists() {
    tmux has-session -t "$SESSION_NAME" 2>/dev/null
}

check_dependencies() {
    echo "检查 Python 依赖..."
    local missing=0
    for pkg in fastmcp matplotlib numpy; do
        if $PYTHON_BIN -c "import $pkg" 2>/dev/null; then
            echo "  [OK]   $pkg"
        else
            echo "  [缺少] $pkg"
            missing=1
        fi
    done
    if [ $missing -eq 1 ]; then
        echo "请安装: pip install fastmcp matplotlib numpy"
        exit 1
    fi
    echo "依赖检查完成。"
}

start_service() {
    need_tmux
    check_dependencies
    if session_exists; then
        echo "图表 MCP 服务已在运行中。"
        return 0
    fi
    tmux new-session -d -s "$SESSION_NAME" -n "chart" \
        "cd '$PROJECT_DIR' && $PYTHON_BIN '$MAIN_SCRIPT'"
    sleep 2
    if session_exists; then
        echo "============================================"
        echo "  图表 MCP 服务已启动！"
        echo "  端口: $SERVICE_PORT"
        echo "  端点: http://127.0.0.1:$SERVICE_PORT/mcp"
        echo "============================================"
        echo "Agent 集成:"
        echo '  "chart": {"transport": "http", "url": "http://127.0.0.1:9900/mcp"}'
    else
        echo "启动失败！请检查日志。"
        exit 1
    fi
}

stop_service() {
    need_tmux
    if session_exists; then
        tmux kill-session -t "$SESSION_NAME"
        echo "图表 MCP 服务已停止。"
    else
        echo "图表 MCP 服务未在运行。"
    fi
}

status_service() {
    need_tmux
    if session_exists; then
        echo "图表 MCP 服务: 运行中 | 端口: $SERVICE_PORT | 端点: http://127.0.0.1:$SERVICE_PORT/mcp"
        tmux list-windows -t "$SESSION_NAME"
    else
        echo "图表 MCP 服务未在运行。"
    fi
}

attach_session() {
    need_tmux
    if session_exists; then
        echo "进入会话 (Ctrl+B D 退出)..."
        sleep 1
        tmux attach -t "$SESSION_NAME"
    else
        echo "服务未运行: ./start_chart.sh start"
    fi
}

case "${1:-start}" in
    start)   start_service ;;
    stop)    stop_service ;;
    restart) stop_service; sleep 1; start_service ;;
    status)  status_service ;;
    attach)  attach_session ;;
    *)       echo "用法: $0 [start|stop|restart|status|attach]"; exit 2 ;;
esac
