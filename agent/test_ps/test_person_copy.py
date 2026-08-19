import traceback
import clickhouse_connect
from datetime import datetime, time
import traceback
from datetime import datetime
from typing import List, Dict, Optional
from pprint import pprint

class PersonnelService:
    def __init__(self):
        self.client = clickhouse_connect.get_client(
            host="10.11.22.80",
            port=9120,
            username="nethouse",
            password="CGC%EVXr.ET10Y_N",
            secure=True,
            verify=False,
        )

    def get_today_persons(self) -> list:
        """
        获取今日所有在站头的人员ID列表
        修正常见 ClickHouse SQL 错误，推荐表结构为 `PS.REAL_TIME_STATION_HEAD_INFO`（不要双重 schema），
        并确保 UPDATE_TIME 为 Date 或 DateTime。
        """
        try:
            # 请根据实际表名调整
            query = """
            SELECT
                "STATIONHEADID",
                "STATIONHEADPLACE",
                "STATIONHEADTYPE"
            FROM PS.REAL_TIME_STATION_HEAD_INFO
            WHERE toDate(UPDATE_TIME) = today()
            """
            result = self.client.query(query)
            # 返回字典: STATIONHEADID 作为 key，其他两个作为 value 的元组
            return {
                row[0]: (row[1], row[2])
                for row in result.result_rows
            }
     
        except Exception as e:
            # 建议日志记录实际异常
            print(f"Error in get_today_persons: {e}")
            return []

    def print_area_info_fields(self):
        """
        打印 PS.REAL_TIME_AREA_INFO 表的字段
        """
        try:
            query = "DESCRIBE TABLE PS.REAL_TIME_AREA_INFO"
            result = self.client.query(query)
            fields = [row[0] for row in result.result_rows]
            print("PS.REAL_TIME_AREA_INFO 字段列表：")
            for field in fields:
                print(field)
        except Exception as e:
            print(f"Error in print_area_info_fields: {e}")
    
    def print_car_location_fields(self):
        """
        打印 PS.REAL_TIME_CAR_LOCATION 表的字段
        """
        try:
            query = "DESCRIBE TABLE PS.SYG_RYDW_CAR_LOCATION"
            result = self.client.query(query)
            fields = [row[0] for row in result.result_rows]
            print("PS.REAL_TIME_CAR_LOCATION 字段列表：")
            for field in fields:
                print(field)
        except Exception as e:
            print(f"Error in print_car_location_fields: {e}")
       
            
    def get_area_name_to_limit_dict(self):
        """
        获取 PS.REAL_TIME_AREA_INFO 中 AREANAME 到 AREALIMIT 的字典
        :return: dict {AREANAME: AREALIMIT}
        """
        try:
            query = """
            SELECT
                "AREANAME",
                "AREALIMIT"
            FROM PS.REAL_TIME_AREA_INFO
            """
            result = self.client.query(query)
            # result.result_rows 为数据行，每行 [AREANAME, AREALIMIT]
            area_dict = {row[0]: row[1] for row in result.result_rows}
            return area_dict
        except Exception as e:
            print(f"Error in get_area_name_to_limit_dict: {e}")
            return {}

    def get_today_cars(self):
        """
        获取当天所有最新的、去重的车辆信息，包含: 
        CARCODE,CARNAME, DEPARTMENT, CARTYPENAME, ELECTRICITY, MAINSTATIONID, MAINSTATIONTIME, MAINSTATIONDISTANCE, ENTERTIME, UPDATE_TIME
        """
        query = """
        SELECT
            CARCODE,
            CARNAME,
            DEPARTMENT,
            CARTYPENAME,
            ELECTRICITY,
            MAINSTATIONID,
            MAINSTATIONTIME,
            MAINSTATIONDISTANCE,
            ENTERTIME,
            UPDATE_TIME
        FROM (
            SELECT *,
                row_number() OVER (PARTITION BY CARNAME ORDER BY UPDATE_TIME DESC) AS rn
            FROM PS.REAL_TIME_CAR_LOCATION
            WHERE UPDATE_TIME >= today()
              AND UPDATE_TIME < today() + 1
        )
        WHERE rn = 1
        """
        return {
            row[1]: {
                "CARCODE": row[0],
                "DEPARTMENT": row[2],
                "CARTYPENAME": row[3],
                "ELECTRICITY": row[4],
                "MAINSTATIONID": row[5],
                "MAINSTATIONTIME": row[6],
                "MAINSTATIONDISTANCE": row[7],
                "ENTERTIME": row[8],
            }
       
            for row in self.client.query(query).result_rows
        }
    
    def close(self):
        self.client.close()


if __name__ == "__main__":
  
    service = PersonnelService()

    # result = service.get_today_persons()
    # print(f"人数: {len(result)}")
    # print(result)
    
    
   
    result = service.print_car_location_fields()
    pprint(result)
    service.close()
    

   