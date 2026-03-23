# -*- coding: utf-8 -*-
'''
@File    : database.py
@IDE     : PyCharm
@Author  : shihongyu
@Date    : 2025/11/18
@Describe: 
'''
# database.py
# from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
# from sqlalchemy.orm import DeclarativeBase
#
# DATABASE_URL = "mysql+aiomysql://root:123456@127.0.0.1:3306/ai_system"
#
# engine = create_async_engine(DATABASE_URL, echo=False, future=True)
#
# async_session = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
#
# class Base(DeclarativeBase):
#     pass



from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, DeclarativeBase
from sqlalchemy.pool import QueuePool   # ← 一定要加这行！
DATABASE_URL = "mysql+pymysql://user:123456@10.11.6.15:9774/ai_system?charset=utf8mb4"

engine = create_engine(
    DATABASE_URL,
    poolclass=QueuePool,
    pool_size=5,
    max_overflow=5,
    pool_pre_ping=True,
    echo=False,
    future=True,                    # SQLAlchemy 2.0 风格，必开
)

# 同步会话工厂
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False, autoflush=False)

class Base(DeclarativeBase):
    pass

