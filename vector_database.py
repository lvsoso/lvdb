from enum import Enum
import numpy as np
from typing import Dict, Any, List
from scalar_storage import ScalarStorage
from constants import IndexType
from indexes.index_factory import IndexFactory
from indexes.faiss_index import FaissIndex
from indexes.hnsw_index import HNSWIndex

class VectorDatabase:
    def __init__(self, index_factory: IndexFactory, db_path: str):
        """
        初始化向量数据库
        :param index_factory: 索引工厂
        :param db_path: 数据库路径
        """
        self.scalar_storage = ScalarStorage(db_path)
        self.index_factory = index_factory

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
        # 更新标量存储
        self.scalar_storage.insert_scalar(id, data)

    def query(self, id: int) -> Dict[str, Any]:
        """
        查询向量
        :param id: 向量ID
        :return: 向量数据字典
        """
        return self.scalar_storage.get_scalar(id)
