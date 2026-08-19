import asyncio
import sys
from functools import partial
from langchain.chat_models import init_chat_model
from langchain.messages import HumanMessage, SystemMessage, AnyMessage, ToolMessage, AIMessage
from langgraph.graph import StateGraph, START, END
from typing_extensions import TypedDict, Annotated
import operator
from typing import Literal

# === MCP 集成 ===
from langchain_mcp_adapters.client import MultiServerMCPClient

mcp_client = MultiServerMCPClient({
    "calc": {
        "transport": "http",
        "url": "http://172.31.254.186:8677/mcp",
    }
})

# === 模型配置 ===
model = init_chat_model(
    "openai:qwen3.5-27b",
    base_url="http://10.11.3.210:9702/v1",
    api_key="EMPTY",
    temperature=0.5,
    max_tokens=20480
)


# === State 定义 ===
class MessagesState(TypedDict):
    messages: Annotated[list[AnyMessage], operator.add]
    llm_calls: int
    current_skill: dict  # 👈 新增：接收从上层传入的当前 Skill 字典元数据


# === 异步节点函数 ===
async def llm_call(state: MessagesState, tools):
    current_skill = state.get("current_skill")
    active_tools = tools

    # 👈 核心优化：根据 Skill 声明的工具集动态裁剪全量 MCP 工具，提升大模型准确率
    if current_skill and current_skill.get("tools"):
        active_tools = [t for t in tools if t.name in current_skill["tools"]]

    model_with_tools = model.bind_tools(active_tools)

    current_messages = state["messages"]
    input_messages = []

    # 👈 核心优化：在图内部动态装配 SystemMessage，避免历史上下文被单一提示词污染
    if current_skill:
        base_system = """你是矿山人员智能助手。

规则:
1. 必须遵循已加载skill
2. 优先调用工具
3. 不输出原始JSON
4. 不暴露tool调用细节"""

        system_content = f"""{base_system}

Loaded Skill:
Name: {current_skill.get('name')}
Description: {current_skill.get('description')}

{current_skill.get('content')}"""
        input_messages.append(SystemMessage(content=system_content))
    else:
        input_messages.append(SystemMessage(content="你是矿山人员车辆智能助手，请按需调用工具回答用户问题。"))

    # 合并用户多轮对话上下文
    input_messages.extend(current_messages)

    response = await model_with_tools.ainvoke(input_messages)

    return {
        "messages": [response],
        "llm_calls": state.get('llm_calls', 0) + 1
    }


async def tool_node(state: MessagesState, tools_by_name: dict):
    result = []
    last_message = state["messages"][-1]

    if not hasattr(last_message, 'tool_calls') or not last_message.tool_calls:
        return {"messages": []}

    for tool_call in last_message.tool_calls:
        tool = tools_by_name[tool_call["name"]]
        observation = await tool.ainvoke(tool_call["args"])
        result.append(ToolMessage(
            content=str(observation),
            tool_call_id=tool_call["id"]
        ))
    return {"messages": result}


def should_continue(state: MessagesState) -> Literal["tool_node", END]:
    last_message = state["messages"][-1]
    if hasattr(last_message, 'tool_calls') and last_message.tool_calls:
        return "tool_node"
    return END


def build_agent(tools):
    tools_by_name = {tool.name: tool for tool in tools}
    agent_builder = StateGraph(MessagesState)

    agent_builder.add_node("llm_call", partial(llm_call, tools=tools))
    agent_builder.add_node("tool_node", partial(tool_node, tools_by_name=tools_by_name))

    agent_builder.add_edge(START, "llm_call")
    agent_builder.add_conditional_edges("llm_call", should_continue, {
        "tool_node": "tool_node",
        END: END
    })
    agent_builder.add_edge("tool_node", "llm_call")

    return agent_builder.compile()


# === 流式打印辅助函数 ===
async def print_message(msg, prefix=""):
    if isinstance(msg, HumanMessage):
        print(f"{prefix}👤 用户: {msg.content}", flush=True)
    elif isinstance(msg, AIMessage):
        if msg.content:
            print(f"{prefix}🤖 AI: {msg.content}", flush=True)
        if msg.tool_calls:
            for tc in msg.tool_calls:
                print(f"{prefix}🔧 调用工具: {tc['name']}(args={tc['args']})", flush=True)
    elif isinstance(msg, ToolMessage):
        print(f"{prefix}📥 工具返回: {msg.content}", flush=True)


# === 运行函数 ===
async def main_token_stream():
    tools = await mcp_client.get_tools()
    print(f"✓ Loaded MCP tools: {[t.name for t in tools]}\n")

    agent = build_agent(tools)
    initial_input = {
        "messages": [HumanMessage(content="张三今天在井下的活动轨迹")],
        "llm_calls": 0,
        "current_skill": None
    }

    print("🚀 开始执行 Agent (Token 级流式)...\n" + "=" * 50)
    async_stream = agent.astream(initial_input, stream_mode="messages")
    async for chunk, metadata in async_stream:
        node = metadata.get("langgraph_node", "?")
        if isinstance(chunk, AIMessage):
            if chunk.content:
                print(chunk.content, end="", flush=True)
            if chunk.tool_calls:
                for tc in chunk.tool_calls:
                    print(f"\n[🔧 {node}] 正在调用: {tc['name']}...", flush=True)
        elif isinstance(chunk, ToolMessage):
            print(f"\n[📥 {node}] 观察结果: {chunk.content[:100]}...", flush=True)

    print("\n" + "=" * 50 + "\n✅ 执行完成\n")


if __name__ == "__main__":
    try:
        asyncio.run(main_token_stream())
    except KeyboardInterrupt:
        pass