# image_helper.py - 自动生成的模块文件
# -*- coding: utf-8 -*-
'''
@File    : get_image.py
@IDE     : PyCharm
@Author  : shihongyu
@Date    : 2025/12/11
@Describe: 
'''
import time
import traceback

import cv2
# import posix_ipc
import mmap
import ctypes
import base64
from PIL import Image
import io
import numpy as np
import os

import requests
import time
from typing import Optional, Dict, Any

base_url = "http://10.11.6.15:8666"  # 改成你实际的服务地址

class SharedFrame(ctypes.Structure):
    _fields_ = [
        ("frame_counter", ctypes.c_uint64),
        ("timestamp_us", ctypes.c_uint64),   # 必须与 C++ 顺序一致
        ("width", ctypes.c_int),
        ("height", ctypes.c_int),
        ("format", ctypes.c_int),
        ("data_size", ctypes.c_uint64),
    ]



HEADER_SIZE = ctypes.sizeof(SharedFrame)


def get_img_from_shm(shm_name):
      # 请确保 C++ 端使用相同的命名规则
    shm = None
    mm = None
    try:
          # 打开只读共享内存
          shm = posix_ipc.SharedMemory(shm_name, posix_ipc.O_RDONLY)
          # 获取实际共享内存大小
          actual_size = shm.size
          if actual_size < HEADER_SIZE:
              dict ={"code": 1, "msg": "共享内存太小，无法读取 header"}
              return None,str(dict)
          # 映射整个共享内存
          mm = mmap.mmap(shm.fd, actual_size, prot=mmap.PROT_READ)
          # 读取 header
          header_bytes = mm[:HEADER_SIZE]
          shared = SharedFrame.from_buffer_copy(header_bytes)

          # 基本校验
          if shared.width <= 0 or shared.height <= 0 or shared.frame_counter == 0:

              return {"code": 1, "msg": "共享内存未初始化或无有效帧"}

          expected_data_size = (shared.width * shared.height * 3) // 2

          if HEADER_SIZE + expected_data_size > actual_size:
              dict = {"code": 1, "msg": "共享内存空间不足，无法容纳完整帧数据"}
              return None, str(dict)


          # 提取最新帧的 RGB 数据
          raw_data = mm[HEADER_SIZE: HEADER_SIZE + expected_data_size]

          # 使用 Pillow 将 RGB 数据转为 JPEG
          yuv = np.frombuffer(raw_data, dtype=np.uint8).reshape((shared.height * 3 // 2, shared.width))
          img = cv2.cvtColor(yuv, cv2.COLOR_YUV2BGR_I420)
          ts_sec = shared.timestamp_us / 1_000_000.0

    except posix_ipc.ExistentialError:
        error = f"共享内存 {shm_name} 不存在，请检查拉流程序是否运行"
        return None, error
    except Exception as e:
        error = f"获取图片失败: {str(e)} {traceback.format_exc()}"
        return None, error
    finally:
        if mm:
            mm.close()
        if shm:
            shm.close_fd()

    return img, ts_sec

def get_image(ip,w,h):
    shm_name = f"/rtsp_{ip}"
    if os.path.exists(f'/dev/shm/{shm_name}'):
        return get_img_from_shm(shm_name)
    else:
        return get_rtsp_image(ip)

def get_main_image(ip):
    try:
        shm_name_main = f"/shm_scan_{ip}"
        img_main, ts_main = get_img_from_shm(shm_name_main)
    except Exception as e:
        img_main = None
        ts_main = f"获取主码流图片失败: {str(e)} {traceback.format_exc()} {ts_main}"
    return img_main,ts_main


def base64_to_bgr(base64_string: str) -> np.ndarray | None:
    """
    将 base64 字符串转换为 OpenCV 的 BGR numpy 数组
    返回 None 表示失败
    """
    try:
        # 去掉可能的前缀 "data:image/jpeg;base64,"
        if base64_string.startswith("data:"):
            base64_string = base64_string.split(",", 1)[1]

        # 解码 base64 → bytes
        img_data = base64.b64decode(base64_string)

        # bytes → numpy 数组
        nparr = np.frombuffer(img_data, np.uint8)

        # 解码为 BGR 格式（cv2 默认就是 BGR）
        img_bgr = cv2.imdecode(nparr, cv2.IMREAD_COLOR)

        if img_bgr is None:
            print("警告：cv2.imdecode 失败，数据可能不是有效图像")
            return None

        return img_bgr

    except Exception as e:
        print(f"base64 转 BGR 失败: {str(e)}")
        return None


def get_rtsp_image(ip: str):
    """
    获取 RTSP 最新帧的 base64 图片
    """
    url = f"{base_url.rstrip('/')}/get-rtsp-image/"
    params = {"ip": ip}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("code") == 0:
            return base64_to_bgr(data["data"]["base64"]),data["data"]["timestamp"]

        else:
            return base64_to_bgr(data["data"]["base64"]),data.get("msg", "接口返回失败")

    except requests.Timeout:
        return None, "请求超时"
    except requests.RequestException as e:
        return None, f"请求失败: {str(e)}"


def get_shm_scan_image(ip: str):
    """
    获取 shm_scan 最新帧的 base64 图片
    """
    url = f"{base_url.rstrip('/')}/get-shm-scan-image/"
    params = {"ip": ip}

    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        if data.get("code") == 0:
            return base64_to_bgr(data["data"]["base64"]), data["data"]["timestamp"]

        else:
            return base64_to_bgr(data["data"]["base64"]), data.get("msg", "接口返回失败")

    except requests.Timeout:
        return None, "请求超时"
    except requests.RequestException as e:
        return None, f"请求失败: {str(e)}"


# 使用示例
if __name__ == "__main__":
    camera_ip = "10.11.4.22"
    print("正在获取 RTSP 图像...")
    rtsp_result = get_rtsp_image(camera_ip)
    print(time.time()-rtsp_result[1])

    shm_result = get_shm_scan_image(camera_ip)
    print(time.time() - shm_result[1])
    cv2.imshow('2', rtsp_result[0])
    cv2.imshow('1', shm_result[0])
    cv2.waitKey(0)

