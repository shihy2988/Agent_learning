# -*- coding: utf-8 -*-
'''
@File    : 4.duimei_monitoring.py
@IDE     : PyCharm
@Author  : shihongyu
@Date    : 2025/10/14
@Describe:
'''
import sys
import traceback

# -*- coding: utf-8 -*-
'''
@File    : base_process.py
@IDE     : PyCharm
@Author  : shihongyu
@Date    : 2025/10/11
@Describe:  安装该base进行编译

'''
import base64
import numpy as np
import cv2
from datetime import datetime
from queue import Queue
from threading import Thread
import psutil
from shapely.geometry import Polygon, LineString
from interface import query_camera_geofences, authenticate
from tools import crop_roi, is_roi_all_white, light_change_detect, compare_images, montage_roi, put_chinese_text, \
    judge_intersection, draw_hud_panel_cn
import time
import requests
import json
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

import sys
import os


class BigCoalProcessor:
    """基础处理器，初始化时加载设备特定数据，处理帧时复用缓存"""

    def __init__(self, ip_logger, alert_config, om_model):
        """
        初始化
        :param ip_logger: 日志记录器
        :param alert_config: 告警配置
        :param om_model: 模型
        :param model_record_dict: 记录模型处理时间  用于重启
        :param model_error_dict: 记录模型是否报错   用于提示用户
        """
        ip_logger.info("Processor init")
        ip_logger.info(f"alert_config={alert_config}")

        self.ip_logger = ip_logger
        self.alert_config = alert_config
        self.om_model = om_model
        self.error = False

        # 提取默认配置
        self.ip = alert_config.get('ip')
        if self.ip == 'unknown_ip':
            self.ip = '10.11.6.76'
        self.cameraCode = alert_config.get('cameraCode')
        self.addr = '1'
        self.base_dir = alert_config.get('base_dir', '/AI_warnings')
        self.log_dir = alert_config.get('log_dir', '/AIlogs')
        self.model_id = alert_config.get('model_id')
        self.tip = alert_config.get('tip', (1, 1))
        self.tip1 = self.tip

        self.worker_id = alert_config.get('work_id')

        self.log_dir = self.log_dir
        self.base_dir = self.base_dir + '/shy/大块煤矸石尺寸测量'

        self.warning_count = 0  # 告警数量
        self.alarm_count = 0  # 预警数量

        # 提取配置
        self.stop_warning = alert_config.get('stop_warning', False)
        self.yj_lim = alert_config.get('yj_lim', 30)
        self.belt_refresh_interval = alert_config.get('belt_refresh_interval', 60)
        self.cameraCode = alert_config.get('cameraCode')
        self.save_interval = alert_config.get('save_interval', 1)
        self.threshold_roi = alert_config.get('threshold_roi', 1500)
        self.npy_file = f"model_configs/shy/{self.ip}.npy"
        self.conf_threshold = alert_config.get('conf_threshold', 0.6)
        self.iou_threshold = alert_config.get('iou_threshold', 0.5)
        self.imgsz = alert_config.get('imgsz', [640, 640])
        self.logg_interval = alert_config.get('logg_interval', 10)
        self.log_start_time = 0
        self.roi_refresh_interval = alert_config.get('roi_refresh_interval', 300)  # 300s自动刷新ROI
        self.t_f = 0
        if '57' in self.ip:
            self.conf_threshold = 0.5
        # 加载全局棋盘数据
        try:
            self.global_data = np.load(self.npy_file, allow_pickle=True).item()
            self.ip_logger.info(f"{self.ip} 已加载全局棋盘数据 {self.npy_file}")
        except FileNotFoundError:
            self.global_data = None
            self.ip_logger.error(f"未找到 {self.npy_file} 文件")

        if '162' in self.ip:
            self.length_161 = 80
        elif '57' in self.ip:
            self.length_161 = 120
        else:
            self.length_161 = 80

        # # 卡块数据初始化
        # self.push_interval = alert_config.get('push_interval', 120)
        # self.push_time = 0
        # self.frame_buffer = deque(maxlen=120)
        self.push_first = True
        # 定义静态变量
        self.static_vars = {
            'first_frame_saved': alert_config.get('save_first_frame', True),
            'last_save_time': 0,
            'last_bj_time': 0,
            'last_yj_time': 0,
            'last_bj_bigcoal': 0,
            'pre_save_dir': None,
            'last_roi_query_time': 0,
            'belt_query_time': 0
        }

        # 皮带运行状态查询
        self.query_url = 'https://10.11.22.81:28701/apiaccess/api/getDeviceTagValueFromYK_v3'
        tag76 = 'zhuYunShu.conveyorBelt.No4_RUN'
        tag56 = 'zhuYunShu.conveyorBelt.No1ZC51204_STATUS'
        tag179 = 'zhuYunShu.conveyorBelt.No2JJ43202_RUN'
        tag180 = 'zhuYunShu.conveyorBelt.No2JJ43202_RUN'
        tag161 = 'zhuYunShu.conveyorBelt.No1ZC51204_RUN'
        tag223 = 'zhuYunShu.conveyorBelt.No1ZC43204_STATUS'

        if '76' in self.ip:
            tag = tag76
        elif '56' in self.ip:
            tag = tag56
        elif '179' in self.ip:
            tag = tag179
        elif '180' in self.ip:
            tag = tag180
        elif '161' in self.ip:
            tag = tag161
        elif '223' in self.ip:
            tag = tag223
        elif '57' in self.ip:
            tag = tag56
        else:
            tag = tag76
        self.body_move = {
            "tags": [
                tag
            ]
        }

        # 如果需要认证，可以在 headers 或 auth 里加上
        self.move_headers = {
            "Content-Type": "application/json"
        }
        self.belt_move = False  # self.get_belt_move()

        # 初始化 ROI
        self._initialize_rois()

        # 初始化检测结果队列和消费者线程（用于非阻塞处理）
        self.process_queue = Queue(maxsize=5)
        self.consumer_thread = Thread(target=self._consume_model_results)
        self.consumer_thread.daemon = True
        self.consumer_thread.start()

    def line_intersection(self, p1, p2, p3, p4):
        """计算两条线段(p1,p2)和(p3,p4)的交点，如果无交点则返回None"""
        x1, y1 = p1
        x2, y2 = p2
        x3, y3 = p3
        x4, y4 = p4

        denom = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4)
        if denom == 0:
            return None  # 平行或重合

        px = ((x1 * y2 - y1 * x2) * (x3 - x4) - (x1 - x2) * (x3 * y4 - y3 * x4)) / denom
        py = ((x1 * y2 - y1 * x2) * (y3 - y4) - (y1 - y2) * (x3 * y4 - y3 * x4)) / denom

        # 检查交点是否在线段范围内
        return (px, py)

    def roi_horizontal_intersections(self, roi, y1, y2, img_width):
        """
        计算 y=y1 (上边) 和 y=y2 (下边) 与 ROI 左右边的交点
        :param roi: np.array([[x0,y0], [x1,y1], [x2,y2], [x3,y3]])，四边形
        :param y1, y2: 上下水平线的 y 坐标
        :param img_width: 整张图的宽度（水平线x范围为0~img_width）
        """
        roi_left = (tuple(roi[0]), tuple(roi[3]))  # 左边
        roi_right = (tuple(roi[1]), tuple(roi[2]))  # 右边

        top_line = ((0, y1), (img_width, y1))
        bottom_line = ((0, y2), (img_width, y2))

        intersections = {"top": [], "bottom": []}

        for side_name, y_line in [("top", top_line), ("bottom", bottom_line)]:
            for roi_side in [roi_left, roi_right]:
                pt = self.line_intersection(y_line[0], y_line[1], roi_side[0], roi_side[1])
                if pt is not None:
                    intersections[side_name].append(pt)

        return intersections

    def _process_width(self, width, true_width_top, true_width_bottom):
        """处理单帧图像宽度，根据输入宽度和真实宽度调整输出宽度。"""
        # 计算顶部和底部真实宽度的最大值，作为基准宽度
        true_width = (true_width_top + true_width_bottom) / 2
        # 计算输入宽度与真实宽度的差值
        delta = width - true_width

        # 如果差值小于等于10，直接返回输入宽度
        if abs(delta) <= 10:
            return width
        # 如果差值大于等于30，返回真实宽度
        if abs(delta) >= 20:
            return true_width
        # 如果差值在15到30之间，返回真实宽度加上差值的45%
        if delta >= 10:
            return true_width + 0.50 * delta
        if delta <= -10:
            return true_width
        # 其他情况（差值在10到15之间），返回真实宽度加上差值的20%
        return 0.1 * delta + true_width

    def get_roi_corners(self, points):
        """
        按左下、右下、右上、左上排序点。
        """
        points = sorted(points, key=lambda p: (p[1]))
        return sorted(points[2:], key=lambda p: (p[0])) + sorted(points[:2], key=lambda p: (-p[0]))

    def get_belt_move(self):
        try:
            self.ip_logger.info(f"{self.ip}       开始查询皮带状态")
            response = requests.post(self.query_url, headers=self.move_headers, data=json.dumps(self.body_move),
                                     verify=False)
            res_json = response.json()
            # 取 value（假设只返回一个）
            if "data" in res_json and res_json["data"]:
                value = res_json["data"][0]["value"]
            else:
                value = "False"
            if value == "1":
                value = "True"
            elif value == "0":
                value = "False"
            self.ip_logger.info(f"{self.ip}       belt_move={value}")
            self.static_vars['belt_query_time'] = time.time()
        except Exception as e:
            self.ip_logger.error(f"推送异常: {str(e)}")
            value = "False"
            self.static_vars['belt_query_time'] = time.time()
        return value

    def _initialize_rois(self):
        """初始化或刷新 ROI"""
        self.rois = []
        self.length_threshold = 80
        self.ip_logger.info(f"{self.ip} 开始初始化 ROI")
        try:
            acc_rois = query_camera_geofences(self.cameraCode)
            acc_rois = [[(x * self.tip1[0], y * self.tip1[1]) for x, y in points[:-2]] + points[-2:] for points in
                        acc_rois]
            self.ip_logger.info(f"{self.ip} 已获取到 {len(acc_rois)} 个 ROI roi={acc_rois}")
            for acc_roi in acc_rois:
                model_id = acc_roi[-1]
                if model_id in self.model_id:
                    self.length_threshold = int(acc_roi[-2]) * 2
                    self.rois.append(np.array(acc_roi[:-2], dtype=np.int32))

            if not self.rois:
                self.rois = [[[0, 0], [640, 0], [640, 480], [0, 480]]]
            self.ip_logger.info(f"{self.ip} rois={self.rois} ")
            self.static_vars['last_roi_query_time'] = time.time()
        except Exception as e:
            self.ip_logger.error(f"{self.ip} ROI初始化失败: {str(e)}")
            self.error = True

    def find_overlapping_squares(self, points):
        """
        找到与输入多边形有交集但不完全包含在其中的小格，并返回交集面积作为权重，
        同时标记与最下面或最上面一条边相交的小格。
        """
        input_points = np.array(points, dtype=np.float32)
        input_polygon = Polygon(input_points)

        sorted_points = sorted(points, key=lambda p: p[1], reverse=True)
        bottom_edge = LineString([sorted_points[0], sorted_points[1]])
        top_edge = LineString([sorted_points[2], sorted_points[3]])

        overlapping_squares = []
        for square in self.global_data.get('squares', []):
            corners = np.array(square['corners']).reshape(4, 2)
            square_polygon = Polygon(corners)
            intersection = input_polygon.intersection(square_polygon)
            intersection_area = intersection.area
            is_contained = input_polygon.contains(square_polygon)

            if intersection_area > 0 and not is_contained:
                is_bottom_edge = bottom_edge.intersects(square_polygon)
                is_top_edge = top_edge.intersects(square_polygon)
                overlapping_squares.append((square, intersection_area, is_bottom_edge, is_top_edge))

        return overlapping_squares

    def calculate_physical_dimensions_with_multiple_squares(self, points):
        """
        根据输入的四个像素点和覆盖的多个小格参数计算实际物理宽高（单位：厘米）。
        """
        if self.global_data is None:
            roi = self.get_roi_corners(self.rois[0])
            (x1, y1), (x2, y2), (x3, y3), (x4, y4) = roi
            pixel_length = self.length_161 / ((x2 - x1) / self.tip[0])
            width = (points[1][0] - points[0][0]) * pixel_length
            height = (points[3][1] - points[0][1]) * pixel_length
            return width, height

        if not self.global_data.get('squares', []):
            self.ip_logger.error("全局棋盘参数中没有小格数据")
            return None, None

        overlapping_squares = self.find_overlapping_squares(points)
        if not overlapping_squares:
            return None, None

        total_area = sum(area for _, area, _, _ in overlapping_squares)
        weights = [area / total_area for _, area, _, _ in overlapping_squares]
        weighted_matrix = np.zeros((3, 3), dtype=np.float32)

        wrd, wrp = [], []
        for (square, _, is_bottom_edge, is_top_edge), weight in zip(overlapping_squares, weights):
            transform_matrix = np.array(square['transform_matrix']).reshape(3, 3)
            weighted_matrix += weight * transform_matrix
            if is_bottom_edge:
                wrd.append(square['transform_matrix'][1])
            if is_top_edge:
                wrp.append(square['transform_matrix'][1])

        wrd = np.mean(wrd) if wrd else np.nan
        wrp = np.mean(sorted(wrp)[:4]) if wrp else np.nan
        wr = wrd if not np.isnan(wrd) else wrp
        if np.isnan(wr):
            return None, None

        points = np.array(points, dtype=np.float32)
        pixel_area = cv2.contourArea(points)
        physical_area = pixel_area * weighted_matrix[0, 0] * weighted_matrix[1, 1]
        width = (points[2][0] - points[3][0]) * wr
        height = physical_area / width if width != 0 else None

        return width, height

    def process_frame(self, frame, frame_time):
        """处理单帧数据并返回检测结果（只到模型推理，避免阻塞）"""
        try:
            # 保存帧逻辑
            timestamp = datetime.fromtimestamp(frame_time).strftime("%Y-%m-%d-%H-%M-%S-%f")

            current_date = datetime.now().strftime("%Y-%m-%d")
            save_dir = os.path.join(self.base_dir, self.ip.replace('.', '_'), current_date)
            if self.static_vars['pre_save_dir'] != save_dir:
                os.makedirs(save_dir, exist_ok=True)
                self.static_vars['pre_save_dir'] = save_dir
                first_frame_path = os.path.join(save_dir, f"{self.ip.replace('.', '_')}_{timestamp}_first_frame.jpg")
                cv2.imwrite(first_frame_path, frame)
                self.ip_logger.info(f"首帧保存至 {first_frame_path}")
            if not self.static_vars['first_frame_saved']:
                self.static_vars['first_frame_saved'] = True
                first_frame_path = os.path.join(save_dir, f"{self.ip.replace('.', '_')}_{timestamp}_first_frame.jpg")
                cv2.imwrite(first_frame_path, frame)
                self.ip_logger.info(f"首帧保存至 {first_frame_path}")

            # 模型推理（如果模型存在）
            model_results = []
            if self.om_model:
                try:
                    t1 = time.time()
                    model_results = self.om_model(frame, conf_threshold=self.conf_threshold,
                                                  iou_threshold=self.iou_threshold)
                    self.tt = time.time() - t1

                except Exception as e:
                    self.ip_logger.error(f"{self.ip} 推理失败: {str(e)}")
                    self.error = True

                    return []

            # 放入前判断
            if self.process_queue.full():
                _ = self.process_queue.get_nowait()  # 丢弃旧帧
            self.process_queue.put((model_results, frame.copy(), frame_time, save_dir, timestamp))
            self.error = True
            return model_results
        except Exception as e:
            self.ip_logger.error(f"处理帧失败: {str(e)}\n{traceback.format_exc()}")
            self.error = False
            return []

    def _process_model_results(self, model_results, frame, frame_time, save_dir, timestamp):

        """处理检测结果的具体逻辑（原process_frame的后半部分）"""
        # 定期刷新 ROI（300s）
        if time.time() - self.static_vars['last_roi_query_time'] >= self.roi_refresh_interval:
            self.ip_logger.info(f"{self.ip} {self.roi_refresh_interval}已到，重新查询ROI")
            self._initialize_rois()
        # 定时查询皮带状态（60s）
        if time.time() - self.static_vars['belt_query_time'] >= self.belt_refresh_interval:
            self.ip_logger.info(f"{self.ip} {self.belt_refresh_interval} 已到，重新查询皮带状态")
            self.belt_move = self.get_belt_move()

        if self.log_start_time + self.logg_interval < time.time():
            self.log_start_time = time.time()

            self.ip_logger.info(
                f"{self.ip} 皮带运行状态 {self.belt_move} 参考阈值{self.length_threshold} 检测到 {len(model_results)} 个目标, 耗时 {self.tt:.3f} 内存使用率: {psutil.virtual_memory().percent}")

        h, w = frame.shape[:2]
        wtip, htip = w / 640.0, h / 360.0
        current_date = datetime.now().strftime("%Y-%m-%d")

        if '76' in self.ip:
            actu_width = 80
        elif '180' in self.ip:
            actu_width = 110
        elif '179' in self.ip:
            actu_width = 110
        elif '162' in self.ip:
            actu_width = 80
        elif '57' in self.ip:
            actu_width = 110
        else:
            actu_width = 100

        true_width_top = 0
        true_width_bottom = 0
        true_height_top = 0
        true_height_bottom = 0

        framenew = frame.copy()
        image_path_ori = os.path.join(save_dir, f"{self.ip.replace('.', '_')}_{timestamp}_ori.jpg")
        image_path_warning = os.path.join(save_dir, f"{self.ip.replace('.', '_')}_{timestamp}_warning.jpg")

        ori_write = False
        warn_push_yj = False
        warn_push_BJ = False

        width_belt = 0
        labels = []
        for det in model_results:
            [(x1, y1), (x2, y1), (x2, y2), (x1, y2)] = det['bbox']
            if det['label'] == 2:
                labels.append(det['label'])
                width_belt = x2 - x1
                break

        if 1 in labels or 3 in labels:
            return []

        max_w, max_h = 0, 0
        max_target = None
        roi_areanew = [(20, h - 30), (620, h - 30), (620, h - 20), (20, h - 20)]

        for det in model_results:
            [(x1, y1), (x2, y1), (x2, y2), (x1, y2)] = det['bbox']
            x1, y1, x2, y2 = int(x1), int(y1), int(x2), int(y2)
            conf = det['conf']
            label = det['label']
            if label != 0:
                continue
            if conf < self.conf_threshold:
                continue
            if not ori_write and time.time() - self.t_f > 30:
                cv2.imencode('.jpg', frame)[1].tofile(image_path_ori)
                self.t_f = time.time()
                ori_write = True

            corners_box = [(x1, y1), (x2, y1), (x2, y2), (x1, y2)]

            area = cv2.contourArea(np.array(corners_box))
            roi_area = 0
            in_roi = False
            if judge_intersection(corners_box, roi_areanew) > 0.1:
                continue
            for roi in self.rois:
                framenew = cv2.polylines(framenew, [roi], True, (0, 255, 0), 2)
                roi_area = max(cv2.contourArea(np.array(roi)), roi_area)
                if judge_intersection(corners_box, roi) > 0.5:
                    in_roi = True

            result = self.roi_horizontal_intersections(self.rois[0], y1, y2, 2048)
            if len(result["top"]) == 2:
                left, right = result["top"]
                pix_width = actu_width / abs(left[0] - right[0])
                true_width_top = (x2 - x1) * pix_width
                true_height_top = (y2 - y1) * pix_width
            if len(result["bottom"]) == 2:
                left, right = result["bottom"]
                pix_width = actu_width / abs(left[0] - right[0])
                true_width_bottom = (x2 - x1) * pix_width
                true_height_bottom = (y2 - y1) * pix_width
            per_area = area / roi_area
            # 大煤块监测实现逻辑
            # width_cond = 0.2 * width_belt < (x2 - x1) < 0.65 * width_belt
            # height_cond = 0.2 * width_belt < (y2 - y1) < 0.65 * width_belt
            # if area > 0.15 * roi_area and in_roi and (width_cond or height_cond):

            if area > 0.15 * roi_area and in_roi:
                input_points = [(x1 / self.tip[0], y1 / self.tip[1]), (x2 / self.tip[0], y1 / self.tip[1]),
                                (x2 / self.tip[0], y2 / self.tip[1]), (x1 / self.tip[0], y2 / self.tip[1])]
                width, height = self.calculate_physical_dimensions_with_multiple_squares(input_points)
                width, height = width * 0.95, height * 0.95
                if '76' in self.ip:
                    width, height = width * 0.82, height * 0.75
                if '180' in self.ip:
                    width, height = width / 0.57, height
                if '179' in self.ip:
                    width, height = width / 0.57, height
                if width and height:
                    width = self._process_width(width, true_width_top, true_width_bottom)
                    height = self._process_width(height, true_height_top, true_height_bottom)
                    img_crop = frame[y1:y2, x1:x2]
                    img_crop = cv2.cvtColor(img_crop, cv2.COLOR_BGR2GRAY)
                    if cv2.mean(img_crop)[0] > 200:
                        continue

                    # self.ip_logger.info(
                    #     f"{self.ip} 检测到大块煤矸：{timestamp}, 面积: {area}, 物理尺寸: {width:.2f}cm, {height:.2f}cm")

                    if ((
                            self.length_threshold >= width > self.yj_lim and 0.75 * self.yj_lim < height < self.length_threshold) or
                            (
                                    self.length_threshold >= height > self.yj_lim and 0.75 * self.yj_lim < width < self.length_threshold)):
                        warn_push_yj = True
                        cv2.rectangle(framenew, (x1, y1), (x2, y2), (0, 0, 255), 2)
                        cv2.putText(framenew, f"YJ cfg:{conf:.2f} pc:{per_area:.2f}", (x1, y1 - 10),
                                    cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0, 0, 255), 1)
                        if max_target is None or width > max_target[0]:
                            max_target = (width, height)

                    if ((
                            self.length_threshold + 10 > width > self.length_threshold and 200 > height > 0.6 * self.yj_lim and per_area > 0.26) or
                            (self.length_threshold + 10 < width < 200 and per_area > 0.25) or
                            (
                                    self.length_threshold + 10 > height > self.length_threshold and 200 > width > self.yj_lim and per_area > 0.26) or
                            (self.length_threshold + 10 < height < 200 and per_area > 0.3)
                    ):
                        warn_push_BJ = True
                        cv2.rectangle(framenew, (x1, y1), (x2, y2), (0, 0, 255), 2)
                        cv2.putText(framenew, f"BJ cfg:{conf:.2f} pc:{per_area:.2f}", (x1, y1 - 10),
                                    cv2.FONT_HERSHEY_SIMPLEX, 0.6, (0, 0, 255), 1)
                        if max_target is None or width > max_target[0]:
                            max_target = (width, height)

            # over_limit = (x2 - x1) > 0.32 * width_belt or (y2 - y1) > 0.32 * width_belt
            # if area > 0.15 * roi_area and in_roi and over_limit and not warn_push_yj:
            #     input_points = [(x1 / self.tip[0], y1 / self.tip[1]), (x2 / self.tip[0], y1 / self.tip[1]),
            #                     (x2 / self.tip[0], y2 / self.tip[1]), (x1 / self.tip[0], y2 / self.tip[1])]
            #
            #     width, height = self.calculate_physical_dimensions_with_multiple_squares(input_points)
            #     width, height = width * 0.95, height * 0.95
            #     if '76' in self.ip:
            #         width, height = width * 0.88, height * 0.95
            #     if '180' in self.ip:
            #         width, height = width / 0.57, height
            #     if '179' in self.ip:
            #         width, height = width / 0.57, height
            #
            #     if width and height:
            #         width = self._process_width(width, true_width_top, true_width_bottom)
            #         self.ip_logger.info(
            #             f"{self.ip} 检测到大块煤矸：{timestamp}, 面积: {area}, 物理尺寸: {width:.2f}cm, {height:.2f}cm")
            #         img_crop = frame[y1:y2, x1:x2]
            #         img_crop = cv2.cvtColor(img_crop, cv2.COLOR_BGR2GRAY)
            #         if cv2.mean(img_crop)[0] > 180:
            #             continue
            #
            #         if (1000 > width > self.length_threshold ) or  (1000>height > self.length_threshold) :
            #             warn_push_BJ = True
            #             cv2.rectangle(framenew, (x1, y1), (x2, y2), (0, 0, 255), 2)
            #             cv2.putText(framenew, f"{per_area:.2f}", (x1, y1 - 10), cv2.FONT_HERSHEY_SIMPLEX, 0.5,
            #                         (0, 0, 255), 1)
            #             max_w, max_h = max(max_w, width), max(max_h, height)

        # if '57' in self.ip :
        #     if warn_push_BJ:
        #         warn_push_BJ = False
        #         warn_push_yj = True

        if warn_push_yj or warn_push_BJ:
            if max_target:
                max_w, max_h = max_target
            solve_txt = '报警'
            if not warn_push_BJ and warn_push_yj:
                solve_txt = '预警'
            info_dict = {
                "监控日期": {"text": current_date, "color": (255, 255, 0)},
                "处理状态": {"text": f"检测到大块煤矸{solve_txt}",
                             "color": (0, 0, 255) if solve_txt == '报警' else (0, 255, 255)},
                "尺寸大小": {"text": f"宽={max_w:.2f}cm,长={max_h:.2f}cm", "color": (0, 30, 200)},
                "皮带状态": {"text": f"{'运行中' if self.belt_move == 'True' else '停止'}",
                             "color": (0, 255, 0) if self.belt_move == 'True' else (0, 0, 255)},

            }

            framenew = draw_hud_panel_cn(
                framenew, info_dict,
                title="大块煤矸智能尺寸监测",
                title_color=(0, 0, 255),
                panel_color=(128, 128, 128),
                panel_width=0.45,
                value_color=(255, 255, 255),
                font_size=16 * wtip,
                offset=(20, 20),
            )

            self.ip_logger.info(
                f"{self.ip} 检测到大块煤矸：{timestamp}, 物理尺寸: {max_w:.2f}cm, {max_h:.2f}cm")
            # if self.stop_warning and '76' in self.ip:
            #     self.stop_belt('76')
            #     self.Broadcast('76')

        if warn_push_BJ and time.time() - self.static_vars[
            'last_bj_time'] >= self.save_interval and self.belt_move == 'True':
            self.static_vars['last_bj_time'] = time.time()
            try:
                token = authenticate(self.alert_config, self.ip_logger)

                if token:
                    body = {
                        "eventInfo": '大煤块识别',
                        "alarmContent": self.alert_config.get('alarmContent', '大煤块检测告警'),
                        "alarmLevel": 'BJ',
                        "addr": self.addr,
                        "modelid": self.alert_config['model_id'],
                    }
                    frame_time, timenow = self.process_and_push_alert(framenew, frame.copy(), self.ip_logger,
                                                                      self.alert_config,
                                                                      frame_time, body, token)
                    self.ip_logger.info(f"大煤块告警，推送告警成功, 告警时间: {frame_time}, 当前时间: {timenow}")
                    cv2.imencode('.jpg', framenew)[1].tofile(image_path_warning)
                    cv2.imencode('.jpg', frame)[1].tofile(image_path_ori)
                    self.warning_count += 1
                else:
                    self.ip_logger.error("推送告警失败，token为空")
            except Exception as e:
                self.ip_logger.error(f"推送告警失败: {str(e)}")

        if not warn_push_BJ and warn_push_yj and time.time() - self.static_vars[
            'last_yj_time'] >= self.save_interval and self.belt_move == 'True':
            self.static_vars['last_yj_time'] = time.time()
            try:
                token = authenticate(self.alert_config, self.ip_logger)

                if token:
                    body = {
                        "eventInfo": '大煤块识别',
                        "alarmContent": self.alert_config.get('alarmContent', '大煤块检测告警'),
                        "alarmLevel": 'YJ',
                        "addr": self.addr,
                        "modelid": self.alert_config['model_id'],
                    }
                    frame_time, timenow = self.process_and_push_alert(framenew, frame.copy(), self.ip_logger,
                                                                      self.alert_config,
                                                                      frame_time, body, token)
                    self.ip_logger.info(f"大煤块预警，推送告警成功, 告警时间: {frame_time}, 当前时间: {timenow}")
                    # cv2.imencode('.jpg', framenew)[1].tofile(image_path_warning)
                    # cv2.imencode('.jpg', frame)[1].tofile(image_path_ori)
                    self.alarm_count += 1
                else:
                    self.ip_logger.error("推送告警失败，token为空")
            except Exception as e:
                self.ip_logger.error(f"推送告警失败: {str(e)}")

        if self.push_first:
            self.push_first = False
            try:
                framenew = frame.copy()
                for det in model_results:
                    label, conf, xyxyxyxy = det['label'], det['conf'], det['bbox']
                    cv2.polylines(framenew, [np.array(xyxyxyxy, dtype=np.int32)], True, (0, 255, 255), 1)
                cv2.polylines(framenew, [np.array(self.rois[0], dtype=np.int32)], True, (0, 255, 0), 2)
                cv2.polylines(framenew, [np.array(roi_areanew)], True, (0, 255, 255), 1)
                info_dict = {
                    "监控日期": {"text": current_date, "color": (255, 255, 0)},
                    "处理状态": {"text": "第一次进行预警", "color": (0, 255, 0)},
                    "皮带状态": {"text": f"{'运行中' if self.belt_move == 'True' else '停止'}",
                                 "color": (0, 255, 0) if self.belt_move == 'True' else (0, 0, 255)},

                }

                framenew = draw_hud_panel_cn(
                    framenew, info_dict,
                    title="大块煤矸智能尺寸监测",
                    title_color=(0, 0, 255),
                    panel_color=(128, 128, 128),
                    panel_width=0.45,
                    value_color=(255, 255, 255),
                    font_size=16 * wtip,
                    offset=(20, 20),
                )

                image_path_ori = os.path.join(save_dir, f"{self.ip.replace('.', '_')}_{timestamp}_ka_ori.jpg")
                image_path_warning = os.path.join(save_dir, f"{self.ip.replace('.', '_')}_{timestamp}_ka_warning.jpg")
                token = authenticate(self.alert_config, self.ip_logger)

                if token:
                    body = {
                        "eventInfo": '大煤块告警',
                        "alarmContent": self.alert_config.get('alarmContent', '大煤块卡堵告警'),
                        "alarmLevel": 'YJ',
                        "addr": self.addr,
                        "modelid": self.alert_config['model_id'],
                    }
                    frame_time, timenow = self.process_and_push_alert(framenew, framenew, self.ip_logger,
                                                                      self.alert_config,
                                                                      frame_time, body, token)
                    self.ip_logger.info(f"大煤块尺寸测量, 预警时间: {frame_time}, 当前时间: {timenow}")
                    cv2.imencode('.jpg', framenew)[1].tofile(image_path_warning)
                    cv2.imencode('.jpg', frame)[1].tofile(image_path_ori)

                else:
                    self.ip_logger.error("推送告警失败，token为空")

            except Exception as e:
                self.ip_logger.error(f"推送告警失败: {str(e)}")

        return None

    def _consume_model_results(self):
        """消费者线程：从队列中取出结果并处理（非阻塞主流程）"""
        while True:
            try:
                item = self.process_queue.get(timeout=1)
                if item is None or len(item) != 5:
                    continue
                model_results, frame, frame_time, save_dir, timestamp = item
                if model_results is not None and model_results != []:
                    self._process_model_results(model_results, frame, frame_time, save_dir, timestamp)
                    self.error = False

            except Exception as e:
                error_trace = traceback.format_exc()
                self.ip_logger.error(f"{self.ip} 消费者线程处理失败: {str(e)} {error_trace}")

                self.error = True

    def close(self):
        """释放设备特定资源"""
        self.rois = []
        self.global_data = None
        self.ip_logger.info(f"进程 {self.ip} 设备特定资源已释放")
        # 关闭队列（可选：如果需要优雅停止消费者线程）
        self.process_queue.put(None)  # 发送停止信号
        self.consumer_thread.join()

    def process_and_push_alert(self, framenew, frame_o, ip_logger, alert_config, frame_time, body, token):
        """
        处理图像编码和告警推送。

        Args:
            framenew: 告警图像（numpy array）
            frame_o: 原始图像（numpy array）
            ip_logger: IP对应的日志器
            alert_config: 告警配置字典，包含 ip, url, cameraCode, modelid
            frame_time: 帧时间
            body: 告警推送的body字典，允许自定义
            token: 认证 token

        Returns:
            float: 最后保存时间（frame_time），或None（如果推送失败或token无效）
        """
        # 验证token
        if not token or not isinstance(token, str) or not token.startswith("eyJhbGciOiJ"):
            ip_logger.warning(f"无效的 Token：{str(token)[:50]}...")
            return None

        # 编码告警图像为base64
        _, buffer = cv2.imencode('.jpg', framenew)
        image_bytes = buffer.tobytes()
        encoded_image = base64.b64encode(image_bytes).decode('utf-8')
        alert_img_base64 = "data:image/jpeg;base64," + encoded_image

        # 编码原始图像为base64
        _, buffer = cv2.imencode('.jpg', frame_o)
        image_bytes = buffer.tobytes()
        encoded_image = base64.b64encode(image_bytes).decode('utf-8')
        frame_img_base64 = "data:image/jpeg;base64," + encoded_image

        # 设置请求头
        headers = {
            "Content-Type": "application/json",
            "gc-authentication": token
        }
        ip = alert_config['ip']
        ip_logger.info(f"进程 {ip} 告警推送进程初始化成功")
        timenow = time.strftime("%Y-%m-%d %H:%M:%S")
        # 更新body中的动态字段
        body.update({
            "time": timenow,
            "cameraCode": alert_config['cameraCode'],
            "markedPicture": alert_img_base64,
            "picture": frame_img_base64,
        })

        # 推送告警
        session = requests.Session()
        try:
            # response = session.post(
            #     alert_config['url'],
            #     json=body,
            #     headers=headers,
            #     verify=False,
            #     timeout=(3.05, 15)
            # )
            # ip_logger.info(
            #     f"告警推送成功，响应状态码：{response.status_code} 响应内容: {response.text} 帧时间: {frame_time} 目前时间: {timenow}")
            ip_logger.warning(f"告警成功：{str(token)[:5]}...")
        except requests.RequestException as e:
            ip_logger.error(f"告警推送失败：{str(e)}")
            return None, None

        return frame_time, timenow

    def stop_belt(self, belt_name: str, stop_reason: str = "AI检测触发自动停机", operation_type: int = 1):
        """
        向皮带停机接口发送请求

        参数:
            belt_name (str): 皮带名称，例如 "1号主运皮带"
            stop_reason (str): 停机原因描述，默认 "AI检测触发自动停机"
            operation_type (int): 触发类型，0=远控, 1=AI触发(默认)

        返回:
            dict: 接口响应结果
        """

        # 皮带名称到 beltNumber 的映射字典
        belt_dict = {
            "1号主运皮带": "1",
            "2号主运皮带": "2",
            "3号主运皮带": "3",
            "76": "4",
            "43202掘进1号皮带": "43202-1",
            "43202掘进2号皮带": "43202-2",
            "51204": "51204-1",
            "43204": "43204-1",
            "43204综采2号皮带": "43204-2",
        }

        # 查找皮带编号
        belt_number = belt_dict.get(belt_name)
        if not belt_number:
            raise ValueError(f"未知的皮带名称: {belt_name}")

        # 请求地址
        url = "https://10.11.22.80:38443/metaworks/vapp/mw-xt-ai-server/operations/belt-stop"

        # 请求体
        payload = {
            "beltNumber": belt_number,
            "operationType": operation_type,
            "stopReason": stop_reason
        }

        headers = {"Content-Type": "application/json"}

        try:
            response = requests.post(url, headers=headers, data=json.dumps(payload), verify=False, timeout=10)
            return {"status": response.status_code, "response": response.text}
        except requests.exceptions.RequestException as e:
            return {"error": str(e)}

    def Broadcast(self, belt_name: str):
        """
        传递广播

        参数:
            belt_name (str): 皮带名称，例如 "1号主运皮带"
        返回:
            dict: 接口响应结果
        """

        # 皮带名称到 beltNumber 的映射字典
        belt_dict = {
            "1号主运皮带": "1",
            "2号主运皮带": "2",
            "3号主运皮带": "3",
            "76": "4",
            "43202掘进1号皮带": "43202-1",
            "43202掘进2号皮带": "43202-2",
            "51204": "51204-1",
            "43204": 7002,
            "43204综采2号皮带": 7041,
        }

        # 查找皮带编号
        belt_number = belt_dict.get(belt_name)
        if belt_name == '76':
            # 请求地址
            url1 = 'https://10.11.22.81:28701/apiaccess/api/getDeviceTagValueFromYK_v3'
            query_url = 'https://10.11.22.81:28701/apiaccess/api/setDeviceTagValueToYK_v3'
            # 请求体
            payload = {
                "tags": [
                    "zhuYunShu.conveyorBelt.PILECOAL_ALARM_No4"
                ]
            }
            payload1 = {
                "tags": [
                    {
                        "name": "zhuYunShu.conveyorBelt.PILECOAL_ALARM_No4",
                        "value": "true"
                    }
                ]
            }
            payload2 = {
                "tags": [
                    {
                        "name": "zhuYunShu.conveyorBelt.PILECOAL_ALARM_No4",
                        "value": "false"
                    }
                ]
            }
            headers = {"Content-Type": "application/json"}

            try:
                response = requests.post(url1, headers=headers, data=json.dumps(payload), verify=False, timeout=10)
                res_json = response.json()
                if "data" in res_json and res_json["data"]:
                    value = res_json["data"][0]["value"]
                if value == 'True':
                    print('如果 True 先置false 关闭广播')
                    response = requests.post(query_url, headers=headers,
                                             data=json.dumps(payload2), verify=False)
                    print('再置false 开启广播')
                    response = requests.post(query_url, headers=headers,
                                             data=json.dumps(payload1), verify=False)
                    time.sleep(2)
                    print('睡2s 置false 关闭广播')
                    response = requests.post(query_url, headers=headers,
                                             data=json.dumps(payload2), verify=False)
                else:
                    print('如果 false 先置true 开启广播')
                    response = requests.post(query_url, headers=headers,
                                             data=json.dumps(payload1), verify=False)
                    time.sleep(2)
                    print('睡2s 置false 关闭广播')
                    response = requests.post(query_url, headers=headers,
                                             data=json.dumps(payload2), verify=False)
                return {"status": response.status_code, "response": response.text}
            except requests.exceptions.RequestException as e:
                return {"error": str(e)}
        else:
            # 请求地址
            url1 = "https://10.11.22.81:28701/apiaccess/api/getGisSpeakVoice"
            # 请求体
            payload = {"content": f"{belt_name} 1号皮带皮带发现大块煤矸，请注意！", "frequency": "3",
                       "terminalNo": [belt_number]}
            headers = {"Content-Type": "application/json"}

            try:
                response = requests.post(url1, headers=headers, data=json.dumps(payload), verify=False, timeout=10)
                return {"status": response.status_code, "response": response.text}
            except requests.exceptions.RequestException as e:
                return {"error": str(e)}
