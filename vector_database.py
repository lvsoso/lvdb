import logging as logger
from enum import Enum
import numpy as np
from typing import Dict, Any, List

from persistence import Persistence
from scalar_storage import ScalarStorage
from indexes.index_factory import IndexFactory
from indexes.faiss_index import FaissIndex
from indexes.hnsw_index import HNSWIndex

from schemas import SearchRequest
from constants import IndexType, Operation


class VectorDatabase:
    def __init__(self, index_factory: IndexFactory, db_path: str, wal_path: str, 
                        version: str):
        """
        初始化向量数据库
        :param index_factory: 索引工厂
        :param db_path: 数据库路径
        :param wal_path: WAL日志路径
        :param version: 版本号
        """
        self.scalar_storage = ScalarStorage(db_path)
        self.index_factory = index_factory
        self.version = version
        self.persistence = Persistence()
        self.persistence.init(wal_path)

    def reload_database(self) -> None:
        """重新加载数据库"""
        logger.info("Entering VectorDatabase::reload_database()")
        
        while True:
            # 读取下一条WAL日志
            result = self.persistence.read_next_wal_log()
            if result is None:
                break
                
            operation_type, json_data = result
            logger.info(f"Operation Type: {operation_type}")
            logger.info(f"Read Line: {json_data}")

            if operation_type == "upsert":
                try:
                    id = json_data["id"]
                    index_type = self._get_index_type_from_request(json_data)
                    
                    # 调用upsert接口重建数据
                    self.upsert(id, json_data, index_type)
                except Exception as e:
                    logger.error(f"Error processing WAL log entry: {str(e)}")
                    continue

    def write_wal_log(self, operation_type: str, json_data: Dict[str, Any]) -> None:
        """
        写入WAL日志
        :param operation_type: 操作类型
        :param json_data: JSON数据
        """
        self.persistence.write_wal_log(operation_type, json_data, self.version)

    def _get_index_type_from_request(self, json_request: Dict[str, Any]) -> IndexType:
        """
        从请求中获取索引类型
        :param json_request: 请求数据
        :return: 索引类型
        """
        if "index_type" in json_request:
            index_type_str = json_request["index_type"]
            if index_type_str == IndexType.FLAT.value:
                return IndexType.FLAT
            elif index_type_str == IndexType.HNSW.value:
                return IndexType.HNSW
        return IndexType.UNKNOWN

    def upsert(self, id: int, data: Dict[str, Any], index_type: IndexType) -> None:
        """
        更新或插入向量
        :param id: 向量ID
        :param data: 包含向量数据的字典
        :param index_type: 索引类型
        """
        # 检查是否存在现有向量
        try:
            existing_data = self.scalar_storage.get_scalar(id)
        except Exception:
            existing_data = {}

        # 如果存在现有向量，从索引中删除
        if existing_data:
            existing_vector = np.array(existing_data.get("vectors", []), dtype=np.float32)
            
            index = self.index_factory.get_index(index_type)
            if index_type == IndexType.FLAT:
                faiss_index = index
                faiss_index.remove_vectors([id])
            elif index_type == IndexType.HNSW:
                hnsw_index = index
                # HNSW 目前不支持删除操作
                pass

        # 插入新向量
        new_vector = np.array(data["vectors"], dtype=np.float32)
        index = self.index_factory.get_index(index_type)
        # TODO: 检查index是否为空
        index.insert_vectors(new_vector, id)

        # 支持过滤索引
        filter_index = self.index_factory.get_index(IndexType.FILTER)
        if filter_index:
            for field_name, value in data.items():
                if isinstance(value, int) and field_name != "id":

                    # 获取旧值（如果存在）
                    old_value = None
                    if existing_data and field_name in existing_data:
                        old_value = existing_data[field_name]

                    # 更新过滤器
                    filter_index.update_int_field_filter(
                        field_name=field_name,
                        old_value=old_value,
                        new_value=value,
                        id=id
                    )
                    logger.error(f"id: {id}, field_name: {field_name}, value : {value}")
    
        # 更新标量存储
        self.scalar_storage.insert_scalar(id, data)

    def query(self, id: int) -> Dict[str, Any]:
        """
        查询向量
        :param id: 向量ID
        :return: 向量数据字典
        """
        return self.scalar_storage.get_scalar(id)

    def search(self, json_request: SearchRequest) -> tuple[list[int], list[float]]:
        """
        搜索向量
        :param json_request: 包含搜索参数的字典
        :return: (ids, distances) 元组
        """
        # 从请求中获取查询参数
        query = np.array(json_request.vectors, dtype=np.float32)
        k = json_request.k

        # 获取索引类型
        index_type = IndexType.UNKNOWN
        if json_request.index_type:
            index_type_str = json_request.index_type
            if index_type_str == IndexType.FLAT.value:
                index_type = IndexType.FLAT
            elif index_type_str == IndexType.HNSW.value:
                index_type = IndexType.HNSW

        # 处理过滤条件
        filter_bitmap = None
        if json_request.filter:
            filter_data = json_request.filter
            field_name = filter_data.fieldName
            op_str = filter_data.op
            value = filter_data.value

            # 转换操作符
            op = Operation.EQUAL if op_str == "=" else Operation.NOT_EQUAL

            logger.error(f"op: {op}, field_name: {field_name}, value : {value}")

            # 获取过滤索引并创建位图
            filter_index = self.index_factory.get_index(IndexType.FILTER)
            if filter_index:
                filter_bitmap = filter_index.get_int_field_filter_bitmap(
                    field_name, op, value
                )

        # 获取向量索引
        index = self.index_factory.get_index(index_type)
        if not index:
            raise ValueError(f"Index type {index_type} not initialized")

        # 执行搜索
        match index_type:
            case IndexType.FLAT:
                results = index.search_vectors(query, k, filter_bitmap)
            case IndexType.HNSW:
                results = index.search_vectors(query, k, filter_bitmap)
            case _:
                raise ValueError(f"Unsupported index type: {index_type}")

        return results
