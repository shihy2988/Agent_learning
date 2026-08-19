from langchain.chat_models import init_chat_model

local_llm = init_chat_model(
    "openai:AI",
    base_url="http://20.24.31.20:7580/v1",
    api_key="EMPTY",
    temperature=0.9,

)

result = local_llm.invoke("你叫什么名字?  /no_think")
print(result)