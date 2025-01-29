from typing import Dict, Optional, Union
from constants import IndexType, MetricType
from indexes.faiss_index import FaissIndex
from indexes.hnsw_index import HNSWIndex


class IndexFactory:
    def __init__(self):
        self.index_map: Dict[IndexType, Union[FaissIndex, HNSWIndex]] = {}

    def init(self, type_: IndexType, dim: int, num_data: int = 0, metric: MetricType = MetricType.L2):
        if type_ == IndexType.FLAT:
            self.index_map[type_] = FaissIndex(dim, metric)
        elif type_ == IndexType.HNSW:
            self.index_map[type_] = HNSWIndex(dim, num_data, metric, 16, 200)

    def get_index(self, type_: IndexType) -> Optional[FaissIndex]:
        return self.index_map.get(type_)