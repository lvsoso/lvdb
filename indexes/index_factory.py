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