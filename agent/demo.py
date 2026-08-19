import asyncio
from functools import partial
from langchain.chat_models import init_chat_model
from langchain.messages import HumanMessage, SystemMessage, AnyMessage, ToolMessage
from langgraph.graph import StateGraph, START, END
from typing_extensions import TypedDict, Annotated
import operator
from typing import Literal

# === MCP 集成 ===
from langchain_mcp_adapters.client import MultiServerMCPClient

mcp_client = MultiServerMCPClient({
    "calc": {
        "transport": "http",
        "url": "http://20.24.31.20:8677/mcp",
    }
})

# === 模型配置 ===
model = init_chat_model(
    "openai:AI",
    base_url="http://20.24.31.20:7580/v1",
    api_key="EMPTY",
    temperature=0.5,
    max_tokens=2048
)

# === State 定义 ===
class MessagesState(TypedDict):
    messages: Annotated[list[AnyMessage], operator.add]
    llm_calls: int

# === 异步节点函数 ===
async def llm_call(state: dict, tools):
    model_with_tools = model.bind_tools(tools)
    response = await model_with_tools.ainvoke(
        [SystemMessage(content="You are a helpful assistant for arithmetic.")]
        + state["messages"]
    )
    return {
        "messages": [response],
        "llm_calls": state.get('llm_calls', 0) + 1
    }

async def tool_node(state: dict, tools_by_name: dict):
    result = []
    last_message = state["messages"][-1]
    
    if not getattr(last_message, 'tool_calls', None):
        return {"messages": []}
        
    for tool_call in last_message.tool_calls:
        tool = tools_by_name[tool_call["name"]]
        observation = await tool.ainvoke(tool_call["args"])  # ✅ 异步调用
        result.append(ToolMessage(content=str(observation), tool_call_id=tool_call["id"]))
    return {"messages": result}

def should_continue(state: MessagesState) -> Literal["tool_node", END]:
    last_message = state["messages"][-1]
    if getattr(last_message, 'tool_calls', None):
        return "tool_node"
    return END

def build_agent(tools):
    tools_by_name = {tool.name: tool for tool in tools}
    agent_builder = StateGraph(MessagesState)
    
    agent_builder.add_node("llm_call", partial(llm_call, tools=tools))
    agent_builder.add_node("tool_node", partial(tool_node, tools_by_name=tools_by_name))
    
    agent_builder.add_edge(START, "llm_call")
    agent_builder.add_conditional_edges("llm_call", should_continue, ["tool_node", END])
    agent_builder.add_edge("tool_node", "llm_call")
    
    return agent_builder.compile()

async def main():
    tools = await mcp_client.get_tools()
    print(f"✓ Loaded MCP tools: {[t.name for t in tools]}")
    
    agent = build_agent(tools)
    messages = [HumanMessage(content="请计算 (3+7)/4*2+48/8-7 ")]
    result = await agent.ainvoke({"messages": messages})
    
    print("\n=== 对话历史 ===")
    for m in result["messages"]:
        m.pretty_print()

if __name__ == "__main__":
    asyncio.run(main())