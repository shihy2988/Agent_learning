"""AI助手 API 客户端 (qyxt 内部 LangGraph 服务).

设计目标:
  1. 提供一个状态化的 `DigitalAssistantClient`, 一次登录 / 一次建线程, 后续多轮复用;
  2. 把模型回复解析成易读的 Markdown 并落盘 (.md), 同时在终端用 rich 渲染;
  3. 等待首个 chunk 期间给用户「正在思考中…」的友好提示;
  4. 既能作为模块被 voice_assistant 的 test.py 复用, 也能独立 `python api.py` 验证。
"""

from __future__ import annotations

import json
import os
import re
import sys
import threading
import time
import traceback
from datetime import datetime
from typing import Callable, Optional

import requests



try:
    from rich.console import Console
    from rich.markdown import Markdown
    from rich.panel import Panel
    _RICH_OK = True
    _console = Console(force_terminal=True)
except ImportError:
    _RICH_OK = False
    _console = None


# =====================================================================
# 工具: 等待首个 chunk 时显示「思考中」动画
# =====================================================================
class ThinkingIndicator:
    """在等待模型首个 chunk 之前显示「思考中」的友好提示。

    策略 (双保险, 确保肉眼可见):
      1. 进入时立即打印一行静态文字 ``⏳ 模型正在思考中...``
         (即使下一行代码 10ms 内就关掉它, 这行也已经落到屏幕上)
      2. 同时启动后台线程, 每 100ms 用 ``\\r`` 在同一行原地刷新转圈字符 + 经过秒数
      3. ``stop()`` 时清整行, 让位给后续的流式打字
    """

    _FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

    def __init__(self, text: str = "模型正在思考中"):
        self.text = text
        self._thread: Optional[threading.Thread] = None
        self._stop = False
        self._started_at: Optional[float] = None

    def __enter__(self):
        self._started_at = time.time()
        sys.stdout.write(f"⏳ {self.text}... ")
        sys.stdout.flush()

        def _spin():
            i = 0
            while not self._stop:
                frame = self._FRAMES[i % len(self._FRAMES)]
                elapsed = time.time() - (self._started_at or time.time())
                line = f"\r⏳ {self.text}... {frame}  ({elapsed:.1f}s)"
                sys.stdout.write(line)
                sys.stdout.flush()
                i += 1
                time.sleep(0.1)

        self._thread = threading.Thread(target=_spin, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        if self._thread is not None:
            self._stop = True
            self._thread.join(timeout=1.0)
            self._thread = None
            sys.stdout.write("\r" + " " * 60 + "\r")
            sys.stdout.flush()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()


# =====================================================================
# 工具: SSE 数据 → 文本 chunk
# =====================================================================
def extract_chunk_text(data) -> str:
    """从一条 SSE 解析后的 JSON 中抽出新增的纯文本。

    兼容的常见格式:
      - LangGraph messages-tuple 流: ``[{"content": "...", "type": "AIMessageChunk"}, {meta}]``
      - ``{"content": "..."}``
      - ``{"messages": [{"role": "assistant", "content": "..."}, ...]}``
      - ``{"custom": {"output": "..."}}``
      - LangGraph custom 流原生事件: ``{"output": "..."}`` (不带 custom 外壳)
      - 纯字符串
    """
    text, _ = parse_chunk_payload(data)
    return text


def parse_chunk_payload(data) -> tuple[str, list[dict]]:
    """完整解析 SSE chunk: 返回 (text_chunk, citations).

    - text_chunk:    本帧需要追加到回答里的纯文本 (没有则空字符串)
    - citations:     本帧携带的引用列表 (混合架构后端的 ``{"type": "citations", "items": [...]}``);
                     非引用帧返回 ``[]``。

    设计兼容:
      - 旧后端只发 ``output`` 文本 → text_chunk 有值, citations 为空
      - 新后端在合成结束后再发一帧 ``{"type": "citations", "items": [...]}``
        → text_chunk 为空, citations 是列表
    """
    if isinstance(data, str):
        return data, []

    if isinstance(data, list):
        if data and isinstance(data[0], dict):
            content = data[0].get("content")
            if isinstance(content, str):
                return content, []
        return "", []

    if not isinstance(data, dict):
        return "", []

    # 引用事件 (两种位置都接: 顶层 / custom 包裹)
    citations = _extract_citations(data)
    if citations:
        return "", citations

    content = data.get("content")
    if isinstance(content, str):
        return content, []

    messages = data.get("messages")
    if isinstance(messages, list):
        buf = []
        for msg in messages:
            if isinstance(msg, dict) and msg.get("role") == "assistant":
                c = msg.get("content")
                if isinstance(c, str):
                    buf.append(c)
        if buf:
            return "".join(buf), []

    custom = data.get("custom")
    if isinstance(custom, dict):
        out = custom.get("output")
        if isinstance(out, str):
            return out, []

    # langgraph 直接走 stream_mode="custom" 时会把 writer 字典原样发上来
    out = data.get("output")
    if isinstance(out, str):
        return out, []

    return "", []


def _extract_citations(data: dict) -> list[dict]:
    """识别 ``{"type": "citations", "items": [...]}``, 兼容裸顶层 / custom 包裹两种位置."""
    for container in (data, data.get("custom") if isinstance(data.get("custom"), dict) else None):
        if not isinstance(container, dict):
            continue
        if container.get("type") == "citations":
            items = container.get("items") or []
            if isinstance(items, list):
                return [it for it in items if isinstance(it, dict)]
    return []


# =====================================================================
# 工具: 把流式拼接出来的文本规范成 Markdown
# =====================================================================
def normalize_markdown(text: str) -> str:
    """让流式拼接产生的文本更规整:
       - 折叠 3+ 个换行为 2 个
       - 压缩行内多空格 (保留前导缩进, 不破坏列表/代码)
    """
    if not text:
        return ""

    text = text.replace("\r\n", "\n").replace("\r", "\n")

    lines = []
    for line in text.split("\n"):
        stripped = line.lstrip(" \t")
        prefix = line[: len(line) - len(stripped)]
        stripped = re.sub(r"[ \t]{2,}", " ", stripped).rstrip()
        lines.append(prefix + stripped)

    text = "\n".join(lines)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip() + "\n"


# =====================================================================
# 默认配置 (可被 DigitalAssistantClient 构造参数覆盖)
# =====================================================================
DEFAULT_BASE_URL = "http://10.11.3.210:2026"
DEFAULT_USERNAME = "634173477@qq.com"
DEFAULT_PASSWORD = "Jw117318.."
DEFAULT_OUTPUT_DIR =  "outputs"

_COMMON_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}


# =====================================================================
# AI助手 API 客户端
# =====================================================================
class DigitalAssistantClient:
    """状态化的AI助手 API 客户端。

    使用方式::

        client = DigitalAssistantClient()
        client.login()
        client.create_thread()
        text = client.stream_chat("风机和压机当前状态？")
        text = client.stream_chat("继续介绍下压机")   # 服务端线程会保留上下文
    """

    def __init__(
        self,
        base_url: str = DEFAULT_BASE_URL,
        username: str = DEFAULT_USERNAME,
        password: str = DEFAULT_PASSWORD,
        *,
        output_dir: Optional[str] = None,
        save_md: bool = True,
        render_markdown: bool = True,
        request_timeout: float = 60.0,
        model_name: str = "gpt-4",
        thinking_enabled: bool = False,
        is_plan_mode: bool = False,
        recursion_limit: int = 100,
    ):
        self.base_url = base_url.rstrip("/")
        self.username = username
        self.password = password
        self.output_dir = output_dir or DEFAULT_OUTPUT_DIR
        self.save_md = save_md
        self.render_markdown = render_markdown
        self.request_timeout = request_timeout

        self.model_name = model_name
        self.thinking_enabled = thinking_enabled
        self.is_plan_mode = is_plan_mode
        self.recursion_limit = recursion_limit

        self.session = requests.Session()
        self.csrf_token: str = ""
        self.thread_id: str = ""

    # ------------------------------------------------------------------
    # 登录
    # ------------------------------------------------------------------
    def login(self) -> bool:
        url = f"{self.base_url}/api/v1/auth/login/local"
        try:
            resp = self.session.post(
                url,
                data={"username": self.username, "password": self.password},
                timeout=10,
            )
            print(f"[登录] 状态码: {resp.status_code}")
            resp.raise_for_status()

            self.csrf_token = self.session.cookies.get("csrf_token") or ""
            access_token = self.session.cookies.get("access_token") or ""

            if self.csrf_token and access_token:
                print(
                    f"[登录] 成功 csrf_token={self.csrf_token[:20]}... "
                    f"access_token={access_token[:20]}..."
                )
                return True
            print("[登录] 失败: 未获取到 token")
            return False
        except Exception as e:
            print(f"[登录] 异常: {e}")
            traceback.print_exc()
            return False

    # ------------------------------------------------------------------
    # 创建对话线程
    # ------------------------------------------------------------------
    def create_thread(self) -> bool:
        if not self.csrf_token:
            print("[线程] 未登录 (缺少 csrf_token), 无法创建")
            return False

        url = f"{self.base_url}/api/langgraph/threads"
        headers = _COMMON_HEADERS.copy()
        headers["Content-Type"] = "application/json"
        headers["X-CSRF-Token"] = self.csrf_token

        resp = None
        try:
            resp = self.session.post(url, headers=headers, json={}, timeout=10)
            print(f"[线程] 创建状态码: {resp.status_code}")
            resp.raise_for_status()

            data = resp.json()
            self.thread_id = data.get("thread_id") or ""
            if not self.thread_id:
                print(f"[线程] 服务端未返回 thread_id, 原始响应: {data}")
                return False
            print(f"[线程] 创建成功 thread_id={self.thread_id}")
            return True
        except Exception as e:
            print(f"[线程] 创建失败: {e}")
            if resp is not None:
                try:
                    print(f"[线程] 错误详情: {resp.text}")
                except Exception:
                    pass
            return False

    def reset_thread(self) -> bool:
        """另起一段新对话 (清空服务端上下文)。"""
        self.thread_id = ""
        return self.create_thread()

    def ensure_ready(self) -> bool:
        """便利方法: 第一次调用时自动登录 + 建线程。"""
        if not self.csrf_token and not self.login():
            return False
        if not self.thread_id and not self.create_thread():
            return False
        return True

    # ------------------------------------------------------------------
    # 流式对话
    # ------------------------------------------------------------------
    def stream_chat(
        self,
        question: str,
        *,
        save_md: Optional[bool] = None,
        render_markdown: Optional[bool] = None,
        on_chunk: Optional[Callable[[str], None]] = None,
        name = 'Name'
    ) -> Optional[str]:
        """向AI助手提问并实时打印回复, 返回整理后的 Markdown 文本 (失败返回 None)。

        参数:
          question         : 用户问题
          save_md          : 是否落盘 .md (默认沿用实例配置)
          render_markdown  : 是否在终端用 rich 渲染 (默认沿用实例配置)
          on_chunk         : 每收到一段文本就回调一次, 方便上层做 TTS 等增量处理
        """
        if not self.ensure_ready():
            print("[对话] 客户端未就绪 (登录/建线程失败), 跳过")
            return None

        save_md = self.save_md if save_md is None else save_md
        render_markdown = self.render_markdown if render_markdown is None else render_markdown

        url = f"{self.base_url}/api/langgraph/threads/{self.thread_id}/runs/stream"
        headers = _COMMON_HEADERS.copy()
        headers["Content-Type"] = "application/json"
        headers["X-CSRF-Token"] = self.csrf_token

        payload = {
            "input": {
                "messages": [{"role": "user", "content": question}],
            },
            "config": {
                "recursion_limit": self.recursion_limit,
                "configurable": {
                    "model_name": self.model_name,
                    "thinking_enabled": self.thinking_enabled,
                    "is_plan_mode": self.is_plan_mode,
                },
            },
            "stream_mode": ["values", "messages-tuple", "custom"],
        }

        response_buf: list[str] = []
        citations: list[dict] = []
        print(f"\n>>> 用户提问: {question}\n")

        thinking = ThinkingIndicator("模型正在思考中")
        thinking.__enter__()
        first_chunk_received = False

        last_id = None
        raw_result_chunks = []
        current_chunk_buf = []

        try:
            with self.session.post(
                url, headers=headers, json=payload,
                stream=True, timeout=self.request_timeout,
            ) as resp:
                resp.raise_for_status()

                for line in resp.iter_lines(decode_unicode=True):
                    if not line or not line.startswith("data:"):
                        continue

                    data_str = line[5:].strip()
                    if not data_str or data_str == "[DONE]":
                        continue
                    try:
                        data = json.loads(data_str)
                    except json.JSONDecodeError:
                        continue

                    # 处理id逻辑：只保存最后一个id的数据，id变更意味着前面的只认为是思考
                    current_id = None
                    if isinstance(data, dict):
                        current_id = data.get("id")
                    elif isinstance(data, list) and data and isinstance(data[0], dict):
                        current_id = data[0].get("id")

                    # 如果id发生变化，说明新一轮推理；重置临时chunk缓存
                    if current_id is not None and current_id != last_id:
                        last_id = current_id
                        current_chunk_buf = []

                    chunk, chunk_citations = parse_chunk_payload(data)
                    if chunk_citations:
                        # 引用事件不进正文, 单独累计, 末尾再渲染
                        citations.extend(chunk_citations)
                        continue
                    if not chunk:
                        continue

                    if not first_chunk_received:
                        thinking.stop()
                        first_chunk_received = True
                        print(">>> 模型回复（流式输出）：")
                        print("-" * 60)
                        
                    print(chunk)
                    current_chunk_buf.append(chunk)
                    # on_chunk 回调始终继续
                    if on_chunk is not None:
                        try:
                            on_chunk(chunk)
                        except Exception as e:
                            print(f"\n[on_chunk] 回调异常: {e}")

                # 只有最后一个id的chunk作为结果
                response_buf = current_chunk_buf

            raw_text = "".join(response_buf)
            md_text = normalize_markdown(raw_text)

           
            print("\n" + "-" * 60)

            if render_markdown and md_text.strip():
                if _RICH_OK:
                    _console.print()
                    _console.print(
                        Panel.fit(
                            Markdown(md_text),
                            title="[bold green]模型回复 (Markdown 渲染)[/bold green]",
                            border_style="green",
                        )
                    )
                else:
                    print("\n===== 模型回复（Markdown 原文） =====")
                    print(md_text)
                    print("=" * 40)

            if save_md and md_text.strip():
                try:
                    os.makedirs(self.output_dir, exist_ok=True)
                    ts = datetime.now().strftime("%Y%m%d")
                    output_dir = os.path.join(self.output_dir, ts)
                    os.makedirs(output_dir, exist_ok=True)
                    md_path = os.path.join(output_dir,f"{name}_{ts}.md")
                    with open(md_path, "w", encoding="utf-8") as f:
                        f.write(f"# 用户提问\n\n{question}\n\n")
                        f.write("# 模型回复\n\n")
                        f.write(md_text)
                    print(f"[已保存] Markdown 文件: {md_path}")
                except Exception as e:
                    print(f"[保存] 写入 .md 失败: {e}")

            return md_text,md_path

        except Exception as e:
            print(f"\n[对话] 失败: {e}")
            traceback.print_exc()
            return None
        finally:
            thinking.stop()


    def delete_thread(self, thread_id=None) -> bool:
        """
        删除指定对话线程 (服务端释放资源)。
        如果传入 thread_id，则删除指定线程，否则删除 self.thread_id。
        """
        if not self.csrf_token:
            print("[删除线程] 未登录 (缺少 csrf_token), 无法删除")
            return False
        delete_id = thread_id if thread_id else getattr(self, "thread_id", None)
        if not delete_id:
            print("[删除线程] 未指定 thread_id, 跳过删除")
            return False

        url = f"{self.base_url}/api/threads/{delete_id}"
        headers = _COMMON_HEADERS.copy()
        headers["Content-Type"] = "application/json"
        headers["X-CSRF-Token"] = self.csrf_token

        resp = None
        try:
            resp = self.session.delete(url, headers=headers, timeout=10)
            print(f"[删除线程] 删除状态码: {resp.status_code}")
            resp.raise_for_status()
            print(f"[删除线程] 成功删除 thread_id={delete_id}")
            if thread_id is None or delete_id == getattr(self, "thread_id", None):
                self.thread_id = ""
            return True
        except Exception as e:
            print(f"[删除线程] 删除失败: {e}")
            if resp is not None:
                try:
                    print(f"[删除线程] 错误详情: {resp.text}")
                except Exception:
                    pass
            return False
        
    def search_threads(self, metadata: dict = None, limit: int = 1000, offset: int = 0, status: str = None) -> list:
        """
        查询对话线程 (search)。

        Args:
            metadata (dict, optional): 过滤元数据. Default is None.
            limit (int, optional): 返回线程数. Default is 100.
            offset (int, optional): 跳过前offset个. Default is 0.
            status (str, optional): 线程状态过滤. Default is None.

        Returns:
            list: 查询到的线程列表
        """
        url = f"{self.base_url}/api/threads/search"
        headers = _COMMON_HEADERS.copy()
        headers["Content-Type"] = "application/json"
        if hasattr(self, "csrf_token") and self.csrf_token:
            headers["X-CSRF-Token"] = self.csrf_token

        payload = {
            "metadata": metadata or {},
            "limit": limit,
            "offset": offset,
            "status": status
        }
        try:
            resp = self.session.post(url, headers=headers, json=payload, timeout=self.request_timeout)
            resp.raise_for_status()
            threads = resp.json()
           
            print(f"[搜索线程] 共获取到 {len(threads)} 条线程记录")
            return threads
        except Exception as e:
            print(f"[搜索线程] 查询失败: {e}")
            if 'resp' in locals():
                try:
                    print(f"[搜索线程] 错误详情: {resp.text}")
                except Exception:
                    pass
            return []
    
# =====================================================================
# CLI 入口 (本地验证用)
# =====================================================================
if __name__ == "__main__":
    print("===== 调用AI助手 API =====")
    client = DigitalAssistantClient()
    if client.ensure_ready():
        # client.stream_chat("现在井下")
        
        threads = client.search_threads()
        print(f"准备删除 {len(threads)} 个线程...")
        for thread in threads:
            thread_id = thread.get("id") or thread.get("thread_id") or thread.get("uuid")
            if not thread_id:
                print(f"未找到线程ID: {thread}")
                continue
            ok = client.delete_thread(thread_id)
            print(f"删除线程 [{thread_id}]: {'成功' if ok else '失败'}")
 


