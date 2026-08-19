# models.py（补充关系后的完整版本）

from sqlalchemy import (
    Column, Integer, String, ForeignKey, DateTime, Text, func, Table, Float, BigInteger
)
from sqlalchemy.orm import relationship, Mapped, mapped_column
from .base import Base
from datetime import datetime
from typing import List, Optional


class User(Base):
    __tablename__ = "user"

    user_id: Mapped[int] = mapped_column(primary_key=True)
    username: Mapped[str] = mapped_column(String(50), unique=True)
    password: Mapped[str] = mapped_column(String(255))
    role: Mapped[str] = mapped_column(String(10), server_default="user")
    create_time: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    # 一个用户可以启动多个模型任务
    model_run_infos: Mapped[List["ModelRunInfo"]] = relationship(
  "ModelRunInfo", back_populates="user", cascade="all, delete-orphan"
    )


class CameraInfo(Base):
    __tablename__ = "camera_info"

    camera_id: Mapped[int] = mapped_column(primary_key=True)
    area: Mapped[str] = mapped_column(String(64), nullable=True)
    camera_name: Mapped[str] = mapped_column(String(256))
    ip_address: Mapped[str] = mapped_column(String(45), unique=True)
    camera_code: Mapped[str] = mapped_column(String(128), nullable=True, unique=True)
    rtsp_main: Mapped[str] = mapped_column(String(256), nullable=True, unique=True)
    rtsp_sub: Mapped[str] = mapped_column(String(256), nullable=True, unique=True)
    ivs_main: Mapped[str] = mapped_column(String(256), nullable=True, unique=True)
    ivs_sub: Mapped[str] = mapped_column(String(256), nullable=True, unique=True)
    main_resolution: Mapped[str] = mapped_column(String(32), nullable=True)
    sub_resolution: Mapped[str] = mapped_column(String(32), nullable=True)
    fps: Mapped[int] = mapped_column(Integer, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    # 一对多：一个摄像头对应一条运行信息（最新的那条）
    run_info: Mapped["CameraRunInfo"] = relationship(
  "CameraRunInfo", back_populates="camera", uselist=False, cascade="all, delete-orphan"
    )

    # 一对多：一个摄像头可以被多个模型任务使用
    model_run_infos: Mapped[List["ModelRunInfo"]] = relationship(
  "ModelRunInfo", back_populates="camera", cascade="all, delete-orphan"
    )


class CameraRunInfo(Base):
    __tablename__ = "camera_run_info"
    camera_run_id: Mapped[int] = mapped_column(primary_key=True)
    camera_name_id: Mapped[int] = mapped_column(ForeignKey("camera_info.camera_id", ondelete="CASCADE"))
    pull_device: Mapped[str] = mapped_column(String(64), default="CPU")
    pull_type: Mapped[str] =  mapped_column(String(64),default='all')
    pull_status: Mapped[str] = mapped_column(String(64), default="未拉取")
    pull_pid: Mapped[int] = mapped_column(Integer, nullable=True)
    pull_stream: Mapped[str] = mapped_column(String(512), nullable=True)
    pull_stream_type: Mapped[str] = mapped_column(String(64),default='sub')
    pull_resolution: Mapped[str] = mapped_column(String(32), nullable=True)
    device: Mapped[str] = mapped_column(Integer, default=0)
    create_time: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())
    # 反向关系：属于哪个摄像头
    camera: Mapped["CameraInfo"] = relationship(
  "CameraInfo", back_populates="run_info"
    )


class CameraMonitorInfo(Base):
    __tablename__ = "camera_monitor_info"

    monitor_id: Mapped[int] = mapped_column(primary_key=True)
    camera_name_id: Mapped[int] = mapped_column(ForeignKey("camera_info.camera_id", ondelete="CASCADE"))
    ip_address: Mapped[str] = mapped_column(String(45), unique=True)
    ping_avg_ms: Mapped[float] = mapped_column(Float, nullable=True)
    bitrate:Mapped[float]=mapped_column(Float,nullable=True)
    resolution: Mapped[str] = mapped_column(String(32), nullable=True)
    fps: Mapped[float] = mapped_column(Float, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

class StreamStatusInfo(Base):
    __tablename__ = "stream_status"

    id: Mapped[int] = mapped_column(primary_key=True,autoincrement=False)
    url: Mapped[str] = mapped_column(String(512), unique=True)
    resolution: Mapped[str] = mapped_column(String(32), nullable=True)
    fps: Mapped[float] = mapped_column(Float, nullable=True)
    bitrate_kbps:Mapped[float]=mapped_column(Float,nullable=True)
    latency_ms: Mapped[float] = mapped_column(Float, nullable=True)
    last_update: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())




class ModelInfo(Base):
    __tablename__ = "model_info"

    model_id: Mapped[int] = mapped_column(primary_key=True)
    model_type: Mapped[str] = mapped_column(String(64))
    model_name: Mapped[str] = mapped_column(String(128))
    model_version: Mapped[str] = mapped_column(String(32))
    model_run_ID: Mapped[str] = mapped_column(String(64), unique=True)
    dev_status: Mapped[str] = mapped_column(String(16), default="dev")
    created_at: Mapped[datetime] = mapped_column(DateTime, server_default=func.now())

    # 一对多：一个模型可以有多个运行实例（不同摄像头、不同用户）
    run_infos: Mapped[List["ModelRunInfo"]] = relationship(
  "ModelRunInfo", back_populates="model", cascade="all, delete-orphan"
    )


class ModelRunInfo(Base):
    __tablename__ = "model_run_info"

    model_run_id: Mapped[int] = mapped_column(primary_key=True)

    # 外键
    model_name_id: Mapped[int] = mapped_column(ForeignKey("model_info.model_id", ondelete="CASCADE"))
    camera_name_id: Mapped[int] = mapped_column(ForeignKey("camera_info.camera_id", ondelete="CASCADE"))
    user_name_id: Mapped[int] = mapped_column(ForeignKey("user.user_id", ondelete="CASCADE"))

    # 配置 & 运行信息
    config_file: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    run_status: Mapped[str] = mapped_column(String(32), default="未启动")
    run_pid: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    run_time: Mapped[Optional[datetime]] = mapped_column(DateTime, nullable=True)
    log_path: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    warning_count: Mapped[int] = mapped_column(Integer, default=0)
    alarm_count: Mapped[int] = mapped_column(Integer, default=0)

    # === 关系 ===
    # 属于哪个模型
    model: Mapped["ModelInfo"] = relationship(
  "ModelInfo", back_populates="run_infos"
    )

    # 使用了哪个摄像头
    camera: Mapped["CameraInfo"] = relationship(
  "CameraInfo", back_populates="model_run_infos"
    )

    # 由哪个用户启动
    user: Mapped["User"] = relationship(
  "User", back_populates="model_run_infos"
    )


class Camera_cv_status(Base):
    __tablename__ = "camera_cv_status"

    id: Mapped[int] = mapped_column( BigInteger, primary_key=True, autoincrement=True)
    filename: Mapped[str] = mapped_column(  String(255), nullable=False, index=True)
    timestamp_us: Mapped[int] = mapped_column(  BigInteger, nullable=False)
    frame_number: Mapped[int] = mapped_column(  BigInteger, nullable=False)
    time_now: Mapped[Optional[datetime]] = mapped_column(DateTime,nullable=True,  server_default=func.current_timestamp() )
    blur: Mapped[Optional[str]] = mapped_column(  Text, nullable=True)
    block_occlusion: Mapped[Optional[str]] = mapped_column(  Text, nullable=True)
    darkness: Mapped[Optional[str]] = mapped_column(  Text, nullable=True )
    shake: Mapped[Optional[str]] = mapped_column(  Text, nullable=True  )

