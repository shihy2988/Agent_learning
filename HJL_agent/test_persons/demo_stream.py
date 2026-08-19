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
        "url": "http://10.11.3.210:10677/mcp",
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
    # Annotated[..., operator.add] 会将新消息追加到旧消息列表后
    messages: Annotated[list[AnyMessage], operator.add]
    llm_calls: int

# === 异步节点函数 ===
async def llm_call(state: MessagesState, tools):
    model_with_tools = model.bind_tools(tools)
    
    # --- 修复逻辑 ---
    # 检查当前消息列表中是否已经存在 SystemMessage
    # 如果没有，则在输入给模型的消息序列最前端添加一个
    current_messages = state["messages"]
    if not any(isinstance(m, SystemMessage) for m in current_messages):
        input_messages = [SystemMessage(content="You are a helpful assistant for api call.")] + current_messages
    else:
        input_messages = current_messages

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
        # 传入工具参数并获取结果
        observation = await tool.ainvoke(tool_call["args"])
        result.append(ToolMessage(
            content=str(observation), 
            tool_call_id=tool_call["id"]
        ))
    return {"messages": result}

def should_continue(state: MessagesState) -> Literal["tool_node", END]:
    last_message = state["messages"][-1]
    # 如果 AI 的最后一条消息包含 tool_calls，则跳转到工具节点
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

# === 运行函数：Token 级流式输出 ===
async def main_token_stream():
    # 1. 获取工具
    tools = await mcp_client.get_tools()
    print(f"✓ Loaded MCP tools: {[t.name for t in tools]}\n")
    
    # 2. 构建图
    agent = build_agent(tools)
    
    # 3. 设置初始消息（直接在此放入 SystemMessage 也是一种好方法）
    initial_input = {
        "messages": [HumanMessage(content="陈玉岭今天在井下的活动轨迹")],
        "llm_calls": 0
    }
    
    print("🚀 开始执行 Agent (Token 级流式)...\n" + "="*50)
    
    async for chunk, metadata in agent.astream(
        initial_input,
        stream_mode="messages",
    ):
        node = metadata.get("langgraph_node", "?")
        
        if isinstance(chunk, AIMessage):
            # 处理内容文本
            if chunk.content:
                # 注意：在 astream(stream_mode="messages") 中，chunk 可能是消息增量
                print(chunk.content, end="", flush=True)
            
            # 处理工具调用
            if chunk.tool_calls:
                for tc in chunk.tool_calls:
                    print(f"\n[🔧 {node}] 正在调用: {tc['name']}...", flush=True)
                    
        elif isinstance(chunk, ToolMessage):
            print(f"\n[📥 {node}] 观察结果: {chunk.content[:100]}...", flush=True)

    print("\n" + "="*50 + "\n✅ 执行完成\n")

if __name__ == "__main__":
    try:
        asyncio.run(main_token_stream())
    except KeyboardInterrupt:
        pass