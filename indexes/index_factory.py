import os
import logging as logger
from typing import Dict, Optional, Union
from constants import IndexType, MetricType
from indexes.faiss_index import FaissIndex
from indexes.hnsw_index import HNSWIndex
from indexes.filter_index import FilterIndex


class IndexFactory:
    def __init__(self):
        self.index_map: Dict[IndexType, Union[FaissIndex, HNSWIndex]] = {}

    def init(self, type_: IndexType, dim: int = 1, num_data: int = 0, metric: MetricType = MetricType.L2):
        """
        初始化索引
        :param type_: 索引类型
        :param dim: 向量维度
        :param num_data: 数据量
        :param metric: 距离度量类型
        """
        match type_:
            case IndexType.FLAT:
                self.index_map[type_] = FaissIndex(dim, metric)
            case IndexType.HNSW:
                self.index_map[type_] = HNSWIndex(dim, num_data, metric, 32, 200)
            case IndexType.FILTER:
                self.index_map[type_] = FilterIndex()

    def get_index(self, type_: IndexType) -> Optional[FaissIndex]:
        return self.index_map.get(type_)

    def save_index(self, folder_path: str, scalar_storage) -> None:
        """
        保存所有索引到指定文件夹
        :param folder_path: 保存文件夹路径
        :param scalar_storage: 标量存储对象
        """
        os.makedirs(folder_path, exist_ok=True)

        for index_type, index in self.index_map.items():
            file_path = os.path.join(folder_path, f"{index_type.value}.index")            
            match index_type:
                case IndexType.FLAT:
                    index.save_index(file_path)
                case IndexType.HNSW:
                    index.save_index(file_path)
                case IndexType.FILTER:
                    index.save_index(scalar_storage, file_path)

    def load_index(self, folder_path: str, scalar_storage) -> None:
        """
        从指定文件夹加载所有索引
        :param folder_path: 索引文件夹路径
        :param scalar_storage: 标量存储对象
        """
        if not os.path.exists(folder_path):
            logger.warning(f"Folder not found: {folder_path}")
            return

        for index_type, index in self.index_map.items():
            file_path = os.path.join(folder_path, f"{index_type.value}.index")
            
            match index_type:
                case IndexType.FLAT:
                    index.load_index(file_path)
                case IndexType.HNSW:
                    index.load_index(file_path)
                case IndexType.FILTER:
                    index.load_index(scalar_storage, file_path)