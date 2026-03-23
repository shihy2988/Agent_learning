# -*- coding: utf-8 -*-
'''
@File    : 3.功能测试.py
@IDE     : PyCharm
@Author  : shihongyu
@Date    : 2026/03/02
@Describe: 
'''
from langchain.tools import tool
from langchain.chat_models import init_chat_model
from langgraph.graph import add_messages
from langchain.messages import (
    SystemMessage,
    HumanMessage,
    ToolCall,
)
from langchain_core.messages import BaseMessage
from langgraph.func import entrypoint, task


model = init_chat_model(
    "openai:AI",
    base_url="http://20.24.31.20:7580/v1",
    api_key="EMPTY",
    temperature=0.5,

)

# Define tools
@tool
def multiply(a: int, b: int) -> int:
    """Multiply `a` and `b`.

    Args:
        a: First int
        b: Second int
    """
    return a * b


@tool
def add(a: int, b: int) -> int:
    """Adds `a` and `b`.

    Args:
        a: First int
        b: Second int
    """
    return a + b


@tool
def divide(a: int, b: int) -> float:
    """Divide `a` and `b`.

    Args:
        a: First int
        b: Second int
    """
    return a / b


# Augment the LLM with tools
tools = [add, multiply, divide]
tools_by_name = {tool.name: tool for tool in tools}
model_with_tools = model.bind_tools(tools)


@task
def call_llm(messages: list[BaseMessage]):
    """LLM decides whether to call a tool or not"""
    return model_with_tools.invoke(
        [
            SystemMessage(
                content="You are a helpful assistant tasked with performing arithmetic on a set of inputs."
            )
        ]
        + messages
    )

@task
def final_answer(state: AgentState) -> dict:
    """
    最终回答节点：
    - 不再允许 tool calling
    - 只根据历史 messages 总结最终结果
    """
    messages = [
        SystemMessage(
            content=(
                "You are a helpful assistant.\n"
                "Based on the previous tool results, "
                "provide the final numeric answer.\n"
                "Do NOT call any tools."
            )
        )
    ] + state["messages"]

    response: AIMessage = model.invoke(messages)  # 注意：不用 model_with_tools

    return {"messages": [response]}

@task
def call_tool(tool_call: ToolCall):
    """Performs the tool call"""
    tool = tools_by_name[tool_call["name"]]
    return tool.invoke(tool_call)


@entrypoint()
def agent(messages: list[BaseMessage]):
    model_response = call_llm(messages).result()
    print('model_response----',model_response)
    while True:
        if not model_response.tool_calls:
            break

        # Execute tools
        tool_result_futures = [
            call_tool(tool_call) for tool_call in model_response.tool_calls
        ]
        tool_results = [fut.result() for fut in tool_result_futures]
        messages = add_messages(messages, [model_response, *tool_results])
        model_response = call_llm(messages).result()

    messages = add_messages(messages, model_response)
    return messages

# Invoke
messages = [HumanMessage(content="5加15等于多少. ")]
for chunk in agent.stream(messages, stream_mode="updates"):
    print(chunk)
    print("\n")