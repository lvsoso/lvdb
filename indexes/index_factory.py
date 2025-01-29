from typing import Dict, Optional
from constants import IndexType, MetricType
from indexes.faiss_index import FaissIndex

class IndexFactory:
    def __init__(self):
        self.index_map: Dict[IndexType, FaissIndex] = {}

    def init(self, type_: IndexType, dim: int, metric: MetricType = MetricType.L2):
        if type_ == IndexType.FLAT:
            self.index_map[type_] = FaissIndex(dim, metric)

    def get_index(self, type_: IndexType) -> Optional[FaissIndex]:
        return self.index_map.get(type_)