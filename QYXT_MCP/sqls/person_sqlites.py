# -*- coding: utf-8 -*-
'''
@File    : person_sqlites.py.py
@IDE     : PyCharm
@Author  : shihongyu
@Date    : 2026/05/26
@Describe: 
'''
# file: cache/data_cache_manager.py
import sqlite3
import json
import hashlib
import logging
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Callable, Union
from contextlib import contextmanager
from enum import Enum



class CacheType(Enum):
    """缓存数据类型枚举"""
    PERSON_TRAJECTORY = "person_trajectory"  # 人员轨迹
    CAR_TRAJECTORY = "car_trajectory"  # 车辆轨迹
    PERSONNEL_LIST = "personnel_list"  # 人员名单
    CARS_LIST = "cars_list"  # 车辆名单

    @classmethod
    def all_types(cls) -> List[str]:
        return [t.value for t in cls]


class DataCacheManager:
    """
    通用数据缓存管理器
    支持多类型数据缓存，通过 cache_type + unique_key 实现数据隔离
    """

    # 不同类型的数据主键字段配置
    KEY_CONFIG = {
        CacheType.PERSON_TRAJECTORY: {"id_field": "name", "time_fields": ("start_time", "end_time")},
        CacheType.CAR_TRAJECTORY: {"id_field": "cardID", "time_fields": ("start_time", "end_time")},
        CacheType.PERSONNEL_LIST: {"id_field": "filter_hash", "time_fields": ("start_time", "end_time")},
        CacheType.CARS_LIST: {"id_field": "filter_hash", "time_fields": ("start_time", "end_time")},
    }

    def __init__(self, db_path: str = "data/person_car_cache.db",
                 default_keep_days: int = 365,logger = None):
        """
        初始化通用缓存管理器

        Args:
            db_path: SQLite 数据库路径
            default_keep_days: 默认缓存保留天数
        """
        self.logger = logger
        self.db_path = db_path
        self.default_keep_days = default_keep_days
        self._ensure_db_dir()
        self._init_database()

        self.logger.info(f"通用缓存管理器初始化: {db_path}")

    def _ensure_db_dir(self):
        """确保数据库目录存在"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)

    @contextmanager
    def _get_connection(self):
        """数据库连接上下文管理器"""
        conn = sqlite3.connect(self.db_path, timeout=15.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")  # 提升并发性能
        conn.execute("PRAGMA synchronous=NORMAL")
        try:
            yield conn
        finally:
            conn.close()

    def _init_database(self):
        """初始化统一缓存表"""
        with self._get_connection() as conn:
            cursor = conn.cursor()

            # 统一缓存表：所有类型数据共用
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS Person_data_cache (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,

                    -- 核心区分字段
                    cache_type TEXT NOT NULL,                    -- 数据类型: person_trajectory, car_trajectory...
                    unique_key TEXT NOT NULL,                    -- 业务唯一键: name, cardID, 或filter_hash
                    time_range TEXT NOT NULL,                    -- 时间范围: "2026-05-20 00:00:00|2026-05-26 23:59:59"

                    -- 缓存内容
                    data_json TEXT NOT NULL,                     -- 完整业务数据（JSON格式）
                    data_signature TEXT,                         -- 数据指纹（用于快速比对变更）

                    -- 元数据
                    meta_info TEXT,                              -- 额外元数据（部门、工种等，便于快速筛选）
                    query_params TEXT,                           -- 原始查询参数（便于调试和复用）

                    -- 生命周期
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    expire_at TEXT,                              -- 可选：显式过期时间
                    access_count INTEGER DEFAULT 0,              -- 访问次数（用于热点识别）

                    -- 唯一约束：同类型+同唯一键+同时间范围只存一份
                    UNIQUE(cache_type, unique_key, time_range)
                )
            ''')

            # 高频查询索引
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_type_key_time 
                ON Person_data_cache(cache_type, unique_key, time_range)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_updated 
                ON Person_data_cache(updated_at DESC)
            ''')
            cursor.execute('''
                CREATE INDEX IF NOT EXISTS idx_expire 
                ON Person_data_cache(expire_at) WHERE expire_at IS NOT NULL
            ''')

            # 可选：创建统计视图
            cursor.execute('''
                CREATE VIEW IF NOT EXISTS v_cache_stats AS
                SELECT 
                    cache_type,
                    COUNT(*) as record_count,
                    COUNT(DISTINCT unique_key) as unique_keys,
                    MIN(updated_at) as oldest_update,
                    MAX(updated_at) as latest_update,
                    ROUND(SUM(length(data_json)) * 1.0 / 1024 / 1024, 2) as size_mb
                FROM Person_data_cache
                GROUP BY cache_type
            ''')

            conn.commit()
            self.logger.debug("统一缓存表初始化完成")

    # ==================== 核心工具方法 ====================

    @staticmethod
    def _generate_unique_key(cache_type: CacheType, **kwargs) -> str:
        """
        根据类型和参数生成业务唯一键

        策略：
        - 轨迹类：直接用 name/cardID
        - 列表类：对多条件筛选参数计算哈希
        """
        config = DataCacheManager.KEY_CONFIG.get(cache_type)
        if not config:
            raise ValueError(f"未知缓存类型: {cache_type}")

        id_field = config["id_field"]

        # 轨迹类：直接用标识字段
        if id_field in ("name", "cardID"):
            value = kwargs.get(id_field)
            if not value:
                raise ValueError(f"缺少必要参数: {id_field}")
            return str(value).strip()

        # 列表类：多条件组合哈希
        elif id_field == "filter_hash":
            # 提取所有筛选参数（排除时间）
            filter_params = {
                k: v for k, v in kwargs.items()
                if k not in ("start_time", "end_time") and v is not None
            }
            # 排序后序列化保证哈希稳定
            param_str = json.dumps(filter_params, sort_keys=True, default=str)
            return hashlib.md5(param_str.encode('utf-8')).hexdigest()[:16]

        return str(kwargs.get(id_field, ""))

    @staticmethod
    def _format_time_range(start: str, end: str) -> str:
        """格式化时间范围为统一字符串"""
        return f"{start.strip()}|{end.strip()}"

    @staticmethod
    def _parse_time_range(time_range: str) -> tuple:
        """解析时间范围字符串"""
        parts = time_range.split("|")
        return parts[0].strip(), parts[1].strip() if len(parts) > 1 else ""

    @staticmethod
    def _calculate_data_signature(data: Dict) -> str:
        """计算数据指纹（用于快速判断数据是否变更）"""
        # 只哈希核心业务数据，排除时间戳等易变字段
        core_data = {k: v for k, v in data.items()
                     if k not in ("query_date", "update_time", "_cache_hit")}
        content = json.dumps(core_data, sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(content.encode('utf-8')).hexdigest()[:16]

    # ==================== 缓存查询：智能合并 ====================

    def query_with_merge(self,
                         cache_type: CacheType,
                         unique_key: str,
                         req_start: str,
                         req_end: str,
                         fetch_func: Callable[[str, str, str], Dict],
                         merge_func: Optional[Callable[[List[Dict], Dict, str, str], Dict]] = None,
                         **fetch_kwargs) -> Dict[str, Any]:
        """
        通用智能查询：缓存优先 + 增量获取 + 合并返回

        Args:
            cache_type: 缓存类型枚举
            unique_key: 业务唯一标识（姓名/卡号/筛选哈希）
            req_start/end: 请求时间范围
            fetch_func: 缺失数据获取回调，签名: func(key, start, end, **kwargs) -> Dict
            merge_func: 可选，自定义合并逻辑；默认按时间裁剪+去重
            **fetch_kwargs: 透传给 fetch_func 的额外参数

        Returns:
            合并后的完整业务数据
        """
        self.logger.debug(f"[{cache_type.value}] 缓存查询: {unique_key} [{req_start} ~ {req_end}]")

        # 1. 查询缓存覆盖情况
        cache_info = self._find_cache_coverage(cache_type, unique_key, req_start, req_end)

        # 2. 收集所有相关数据片段
        all_data_fragments = []
        base_meta = {}

        # 2.1 添加缓存中的片段
        for cache_rec in cache_info['cached_records']:
            fragment = self._parse_cached_data(cache_rec)
            all_data_fragments.append(fragment)
            if not base_meta:
                base_meta = self._extract_meta(cache_rec)

        # 2.2 查询并添加缺失时间段的数据
        if cache_info['missing_ranges']:
            self.logger.info(f"[{cache_type.value}] 缺失 {len(cache_info['missing_ranges'])} 段，增量查询")
            for miss_start, miss_end in cache_info['missing_ranges']:
                try:
                    partial = fetch_func(unique_key, miss_start, miss_end, **fetch_kwargs)
                    if partial:
                        all_data_fragments.append(partial)
                        if not base_meta:
                            base_meta = {k: v for k, v in partial.items()
                                         if k not in ('segments', 'records', 'cars', 'persons', 'total')}
                        self.logger.debug(f"获取缺失: [{miss_start} ~ {miss_end}]")
                except Exception as e:
                    self.logger.warning(f"增量查询失败 [{miss_start}~{miss_end}]: {e}")
                    continue

        # 3. 合并数据
        if merge_func:
            # 使用自定义合并逻辑
            merged_result = merge_func(all_data_fragments, base_meta, req_start, req_end)
        else:
            # 默认合并：轨迹类按时间裁剪，列表类直接聚合
            if cache_type in (CacheType.PERSON_TRAJECTORY, CacheType.CAR_TRAJECTORY):
                merged_result = self._merge_trajectory_fragments(
                    all_data_fragments, base_meta, req_start, req_end, unique_key
                )
            else:  # 列表类
                merged_result = self._merge_list_fragments(
                    all_data_fragments, base_meta, cache_type
                )

        # 4. 补充通用字段
        merged_result['_cache_meta'] = {
            'type': cache_type.value,
            'key': unique_key,
            'hit': cache_info['full_coverage'],
            'missing_ranges': cache_info['missing_ranges']
        }

        # 5. 异步保存最新数据
        self._async_save(cache_type, unique_key, req_start, req_end, merged_result)

        return merged_result

    def _find_cache_coverage(self, cache_type: CacheType, unique_key: str,
                             req_start: str, req_end: str) -> Dict:
        """查找缓存覆盖情况"""
        req_range = self._format_time_range(req_start, req_end)

        with self._get_connection() as conn:
            cursor = conn.cursor()
            # 查询所有时间有重叠的缓存记录
            cursor.execute('''
                SELECT * FROM Person_data_cache 
                WHERE cache_type = ? 
                AND unique_key = ?
                AND time_range NOT LIKE ?  -- 排除完全不相交的
                ORDER BY time_range
            ''', (cache_type.value, unique_key, f"%|{req_start}")) 

            # 更精确的查询：解析 time_range 比较
            cursor.execute('''
                SELECT * FROM Person_data_cache 
                WHERE cache_type = ? AND unique_key = ?
            ''', (cache_type.value, unique_key))

            rows = cursor.fetchall()

        if not rows:
            return {
                'cached_records': [],
                'missing_ranges': [(req_start, req_end)],
                'full_coverage': False
            }

        # 解析并筛选有重叠的记录
        cached_records = []
        covered_ranges = []

        for row in rows:
            cache_start, cache_end = self._parse_time_range(row['time_range'])
            # 判断时间重叠: [A,B] 与 [C,D] 重叠条件: A<=D and C<=B
            if cache_start <= req_end and req_start <= cache_end:
                cached_records.append(dict(row))
                covered_ranges.append((cache_start, cache_end))

        # 计算缺失区间
        missing = self._calculate_gaps(req_start, req_end, covered_ranges)

        return {
            'cached_records': cached_records,
            'missing_ranges': missing,
            'full_coverage': len(missing) == 0
        }

    def _calculate_gaps(self, req_start: str, req_end: str,
                        covered: List[tuple]) -> List[tuple]:
        """计算未覆盖的时间区间（同前）"""
        if not covered:
            return [(req_start, req_end)]

        # 合并重叠区间
        sorted_cov = sorted(covered, key=lambda x: x[0])
        merged = [sorted_cov[0]]
        for s, e in sorted_cov[1:]:
            last_s, last_e = merged[-1]
            if s <= last_e:
                merged[-1] = (last_s, max(last_e, e))
            else:
                merged.append((s, e))

        # 找空缺
        gaps = []
        cursor = req_start
        for cov_s, cov_e in merged:
            if cov_s > cursor:
                gaps.append((cursor, min(cov_s, req_end)))
            cursor = max(cursor, cov_e)
            if cursor >= req_end:
                break
        if cursor < req_end:
            gaps.append((cursor, req_end))

        return gaps

    def _parse_cached_data(self, cache_rec: Dict) -> Dict:
        """解析缓存记录中的数据"""
        data_json = cache_rec.get('data_json', '{}')
        return json.loads(data_json) if isinstance(data_json, str) else data_json

    def _extract_meta(self, cache_rec: Dict) -> Dict:
        """提取元数据"""
        meta_str = cache_rec.get('meta_info', '{}')
        return json.loads(meta_str) if isinstance(meta_str, str) else {}

    # ==================== 默认合并逻辑 ====================

    def _merge_trajectory_fragments(self, fragments: List[Dict], base_meta: Dict,
                                    req_start: str, req_end: str, unique_key: str) -> Dict:
        """轨迹类数据合并：按时间裁剪 + segments 去重"""
        all_segments = []

        for frag in fragments:
            segs = frag.get('segments', [])
            if isinstance(segs, list):
                all_segments.extend(segs)

        # 去重 + 裁剪
        merged_segs = self._deduplicate_and_trim_segments(all_segments, req_start, req_end)

        # 构建结果
        result = {
            **base_meta,
            'start': req_start,
            'end': req_end,
            'name' if 'name' in base_meta else 'vehicle_name': unique_key,
            'segments': merged_segs,
            'total_segments': len(merged_segs)
        }
        return result

    def _merge_list_fragments(self, fragments: List[Dict], base_meta: Dict,
                              cache_type: CacheType) -> Dict:
        """列表类数据合并：聚合人员/车辆字典"""
        merged_items = {}  # name/cardID -> info

        for frag in fragments:
            # 根据类型提取人员/车辆字典
            if cache_type == CacheType.PERSONNEL_LIST:
                items = frag.get('persons', {})
                id_field = 'cardid'
            else:  # CARS_LIST
                items = frag.get('cars', {})
                id_field = 'cardId'

            for item_id, info in items.items():
                if item_id not in merged_items:
                    merged_items[item_id] = info.copy()
                else:
                    # 合并 records（避免重复）
                    existing_records = merged_items[item_id].get('records', [])
                    new_records = info.get('records', [])
                    # 简单去重：按 inTime 判断
                    existing_times = {r.get('inTime') for r in existing_records}
                    for rec in new_records:
                        if rec.get('inTime') not in existing_times:
                            existing_records.append(rec)
                    merged_items[item_id]['records'] = existing_records

        # 构建结果
        result = {
            **base_meta,
            ('persons' if cache_type == CacheType.PERSONNEL_LIST else 'cars'): merged_items,
            'total': sum(len(info.get('records', [])) for info in merged_items.values()),
            'update_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        return result

    def _deduplicate_and_trim_segments(self, segments: List[Dict],
                                       req_start: str, req_end: str) -> List[Dict]:
        """segments 去重 + 时间裁剪"""
        if not segments:
            return []

        # 排序
        segments.sort(key=lambda x: x.get('segmentStartTime', '') or '')

        # 去重：(start_time, station) 为键
        seen = {}
        for seg in segments:
            key = (seg.get('segmentStartTime'), seg.get('mainStationId') or seg.get('area'))
            if key not in seen or len(json.dumps(seg)) > len(json.dumps(seen[key])):
                seen[key] = seg.copy()

        # 裁剪时间范围
        result = []
        for seg in seen.values():
            s_start = seg.get('segmentStartTime', '')
            s_end = seg.get('segmentEndTime', '')
            if s_end < req_start or s_start > req_end:
                continue
            new_seg = seg.copy()
            if s_start < req_start:
                new_seg['segmentStartTime'] = req_start
            if s_end > req_end:
                new_seg['segmentEndTime'] = req_end
            result.append(new_seg)

        return result

    # ==================== 缓存保存 ====================

    def _async_save(self, cache_type: CacheType, unique_key: str,
                    start: str, end: str, data: Dict):
        """异步保存（生产环境建议用线程池）"""
        try:
            self.save(cache_type, unique_key, start, end, data)
        except Exception as e:
            self.logger.warning(f"[{cache_type.value}] 缓存保存失败: {e}")

    def save(self, cache_type: CacheType, unique_key: str,
             start: str, end: str, data: Dict,
             expire_days: Optional[int] = None,
             meta_override: Optional[Dict] = None) -> bool:
        """
        同步保存数据到缓存

        Args:
            expire_days: 显式设置过期天数（覆盖默认值）
            meta_override: 覆盖自动提取的元数据
        """
        # 提取核心数据（移除调试字段）
        data_to_cache = {k: v for k, v in data.items() if not k.startswith('_')}

        # 计算实际数据时间范围（从 segments/records 推断）
        actual_start, actual_end = start, end
        if 'segments' in data_to_cache and data_to_cache['segments']:
            times = [s.get('segmentStartTime') for s in data_to_cache['segments'] if s.get('segmentStartTime')]
            times += [s.get('segmentEndTime') for s in data_to_cache['segments'] if s.get('segmentEndTime')]
            if times:
                times = [t for t in times if t]
                times.sort()
                actual_start = min(times[0], start)
                actual_end = max(times[-1], end)

        time_range = self._format_time_range(actual_start, actual_end)
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        expire_at = (datetime.now() + timedelta(days=expire_days or self.default_keep_days)).strftime(
            '%Y-%m-%d %H:%M:%S') if expire_days or self.default_keep_days else None

        # 元数据
        meta = meta_override or {
            k: v for k, v in data_to_cache.items()
            if k in ('department', 'workType', 'carType', 'leave')
        }

        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO Person_data_cache 
                    (cache_type, unique_key, time_range, data_json, data_signature,
                     meta_info, query_params, created_at, updated_at, expire_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    cache_type.value,
                    unique_key,
                    time_range,
                    json.dumps(data_to_cache, ensure_ascii=False),
                    self._calculate_data_signature(data_to_cache),
                    json.dumps(meta, ensure_ascii=False),
                    json.dumps({'start': start, 'end': end}, ensure_ascii=False),
                    now, now, expire_at
                ))
                conn.commit()
                self.logger.debug(f"[{cache_type.value}] 缓存保存: {unique_key} [{time_range}]")
                return True
        except Exception as e:
            self.logger.error(f"缓存保存异常: {e}")
            return False

    # ==================== 维护方法 ====================

    def cleanup(self, keep_days: Optional[int] = None,
                cache_types: Optional[List[CacheType]] = None) -> int:
        """清理过期缓存"""
        cutoff = (datetime.now() - timedelta(days=keep_days or self.default_keep_days)).strftime('%Y-%m-%d %H:%M:%S')

        with self._get_connection() as conn:
            cursor = conn.cursor()
            if cache_types:
                placeholders = ','.join('?' * len(cache_types))
                cursor.execute(f'''
                    DELETE FROM Person_data_cache 
                    WHERE updated_at < ? AND cache_type IN ({placeholders})
                ''', [cutoff] + [t.value for t in cache_types])
            else:
                cursor.execute('''
                    DELETE FROM Person_data_cache WHERE updated_at < ?
                ''', (cutoff,))

            deleted = cursor.rowcount
            conn.commit()

        if deleted > 0:
            self.logger.info(f"缓存清理: 删除 {deleted} 条记录")
            self._vacuum()
        return deleted

    def _vacuum(self):
        """整理数据库碎片"""
        try:
            with self._get_connection() as conn:
                conn.execute('VACUUM')
        except Exception as e:
            self.logger.warning(f"VACUUM失败: {e}")

    def get_stats(self, cache_type: Optional[CacheType] = None) -> Dict:
        """获取缓存统计"""
        with self._get_connection() as conn:
            cursor = conn.cursor()
            if cache_type:
                cursor.execute('SELECT * FROM v_cache_stats WHERE cache_type = ?', (cache_type.value,))
            else:
                cursor.execute('SELECT * FROM v_cache_stats')

            rows = cursor.fetchall()
            return {row['cache_type']: dict(row) for row in rows}

    def clear_type(self, cache_type: CacheType, confirm: bool = False) -> bool:
        """清空指定类型的缓存"""
        if not confirm:
            return False
        with self._get_connection() as conn:
            conn.execute('DELETE FROM Person_data_cache WHERE cache_type = ?', (cache_type.value,))
            conn.commit()
        self._vacuum()
        self.logger.warning(f"已清空 [{cache_type.value}] 类型缓存")
        return True