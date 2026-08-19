import json
from fastmcp import FastMCP
from sqlalchemy.orm import Session
from sqlalchemy import or_, func
from database import SessionLocal
from models.models import CameraInfo, CameraMonitorInfo, CameraRunInfo, ModelRunInfo, ModelInfo
from utils.image_helper import get_rtsp_image

def register_camera_tools(mcp: FastMCP):

    @mcp.tool()
    def query_camera_info(detailed: bool = False, query_str: str = None) -> str:
        """
        查询摄像头基础信息和监控配置数据。
        - 场景1 (简要查询所有): detailed=False, query_str=None
        - 场景2 (详细查询所有): detailed=True, query_str=None
        - 场景3 (查询单个/特定摄像头详细信息): 将名称或IP传入 query_str (此时会自动返回详细数据)
        """
        db: Session = SessionLocal()
        try:
            query = db.query(CameraInfo)
            
            # 单个/模糊查询 (需求3)
            if query_str:
                query = query.filter(
                    or_(CameraInfo.camera_name.like(f"%{query_str}%"),
                        CameraInfo.ip_address.like(f"%{query_str}%"))
                )
                detailed = True # 明确指定个别查询时强制返回详细信息

            cameras = query.all()
            result = []

            for cam in cameras:
                cam_data = {
                    "camera_id": cam.camera_id,
                    "name": cam.camera_name,
                    "ip": cam.ip_address,
                    "area": cam.area,
                }
                # 详细信息查询 (需求2, 3)
                if detailed:
                    monitor = db.query(CameraMonitorInfo).filter_by(camera_name_id=cam.camera_id).order_by(CameraMonitorInfo.created_at.desc()).first()
                    cam_data["monitor"] = {
                        "ping_avg_ms": monitor.ping_avg_ms if monitor else None,
                        "bitrate": monitor.bitrate if monitor else None,
                        "fps": monitor.fps if monitor else None,
                        "resolution": monitor.resolution if monitor else None
                    }
                else:
                    cam_data["status"] = "Basic Info Only"
                    
                result.append(cam_data)
            
            # 必须转为 str 解决 LangChain 校验报错
            return json.dumps({"count": len(result), "data": result}, ensure_ascii=False, default=str)
        finally:
            db.close()

    @mcp.tool()
    def query_camera_status(query_str: str = None) -> str:
        """
        查询摄像头的运行状态（在线/离线、帧率、码率）并获取实时图片截图。
        - 场景4 (查询所有摄像头运行状态统计): query_str 留空。返回在线离线数、低帧率低码率名单。
        - 场景5 (查询单个摄像头状态及图片): 将名称或IP传入 query_str。返回拉流状态、设备及实时图片。
        """
        db: Session = SessionLocal()
        try:
            if not query_str:
                # 需求4：所有摄像头状态统计
                all_cams = db.query(CameraInfo).all()
                stats = {"online": 0, "offline": 0, "low_fps": [], "low_bitrate": []}
                
                for cam in all_cams:
                    run_info = db.query(CameraRunInfo).filter_by(camera_name_id=cam.camera_id).first()
                    monitor = db.query(CameraMonitorInfo).filter_by(camera_name_id=cam.camera_id).order_by(CameraMonitorInfo.created_at.desc()).first()
                    
                    is_online = run_info.pull_status != "未拉取" if run_info else False
                    if is_online:
                        stats["online"] += 1
                    else:
                        stats["offline"] += 1
                    
                    if monitor:
                        if monitor.fps and monitor.fps < 15.0:  
                            stats["low_fps"].append({"name": cam.camera_name, "fps": monitor.fps})
                        if monitor.bitrate and monitor.bitrate < 1024: 
                            stats["low_bitrate"].append({"name": cam.camera_name, "bitrate": monitor.bitrate})
                
                return json.dumps({"type": "global_statistics", "data": stats}, ensure_ascii=False, default=str)

            else:
                # 需求5：单个或多个模糊匹配摄像头状态 + 图片
                cams = db.query(CameraInfo).filter(
                    or_(CameraInfo.camera_name.like(f"%{query_str}%"), CameraInfo.ip_address.like(f"%{query_str}%"))
                ).all()

                if not cams:
                    return json.dumps({"error": f"未找到匹配 '{query_str}' 的摄像头"}, ensure_ascii=False)

                result = []
                for cam in cams:
                    run_info = db.query(CameraRunInfo).filter_by(camera_name_id=cam.camera_id).first()
                    image_url = get_rtsp_image(cam.ip_address)
                    
                    result.append({
                        "camera_name": cam.camera_name,
                        "ip": cam.ip_address,
                        "pull_status": run_info.pull_status if run_info else "Unknown",
                        "pull_device": run_info.pull_device if run_info else "Unknown",
                        "latest_image": image_url
                    })

                return json.dumps({"type": "target_status", "data": result}, ensure_ascii=False, default=str)
        finally:
            db.close()

    @mcp.tool()
    def query_model_running_info(query_str: str = None) -> str:
        """
        查询摄像头下的 AI 模型运行情况（告警数、预警数、部署时间等）。
        - 场景6 (查询所有摄像头模型情况): query_str 留空。返回哪些摄像头运行了模型及总告警数。
        - 场景7 (查询单个摄像头模型情况): 将名称或IP传入 query_str。返回该摄像头具体运行的模型及告警数。
        """
        db: Session = SessionLocal()
        try:
            base_query = db.query(ModelRunInfo, CameraInfo, ModelInfo).join(
                CameraInfo, ModelRunInfo.camera_name_id == CameraInfo.camera_id
            ).join(
                ModelInfo, ModelRunInfo.model_name_id == ModelInfo.model_id
            )

            if query_str:
                # 需求7：单个摄像头模型运行状态
                runs = base_query.filter(
                    or_(CameraInfo.camera_name.like(f"%{query_str}%"), CameraInfo.ip_address.like(f"%{query_str}%"))
                ).all()
                
                result = []
                for run, cam, mod in runs:
                    result.append({
                        "camera_name": cam.camera_name,
                        "model_name": mod.model_name,
                        "run_status": run.run_status,
                        "deploy_time": run.run_time.isoformat() if run.run_time else None,
                        "alarms": run.alarm_count,
                        "warnings": run.warning_count
                    })
                return json.dumps({"type": "single_camera_models", "data": result}, ensure_ascii=False, default=str)

            else:
                # 需求6：所有摄像头模型运行情况
                runs = base_query.all()
                total_alarms = 0
                total_warnings = 0
                camera_models_map = {}

                for run, cam, mod in runs:
                    total_alarms += run.alarm_count
                    total_warnings += run.warning_count
                    
                    if cam.camera_name not in camera_models_map:
                        camera_models_map[cam.camera_name] = []
                        
                    camera_models_map[cam.camera_name].append({
                        "model_name": mod.model_name,
                        "status": run.run_status,
                        "deploy_time": run.run_time.isoformat() if run.run_time else None,
                        "camera_alarms": run.alarm_count
                    })

                return json.dumps({
                    "type": "global_model_stats",
                    "total_alarms": total_alarms,
                    "total_warnings": total_warnings,
                    "camera_deployments": camera_models_map
                }, ensure_ascii=False, default=str)
        finally:
            db.close()