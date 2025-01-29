import faiss
import numpy as np
from constants import MetricType


class FaissIndex:

    def __init__(self, dim: int, metric_type: MetricType = MetricType.L2):
        if metric_type == MetricType.L2:
            self.index = faiss.IndexFlatL2(dim)
        else:
            self.index = faiss.IndexFlatIP(dim)
        self.id_map = {}
        self.reverse_id_map = {}

    def insert_vectors(self, vectors: list, label: int):
        vector = np.array(vectors).reshape(1, -1).astype('float32')
        self.index.add(vector)
        internal_id = self.index.ntotal - 1
        self.id_map[internal_id] = label
        self.reverse_id_map[label] = internal_id

    def search_vectors(self, query: list, k: int):
        query = np.array(query).reshape(1, -1).astype('float32')
        distances, indices = self.index.search(query, k)
        result_ids = [self.id_map.get(idx, -1) for idx in indices[0]]
        return result_ids, distances[0].tolist()

    def remove_vectors(self, ids: list):
        """
        删除指定ID的向量
        :param ids: 要删除的向量ID列表
        """
        if not hasattr(self.index, 'remove_ids'):
            raise RuntimeError("当前索引类型不支持删除操作")

        # 将外部ID转换为内部ID
        internal_ids = []
        for label in ids:
            if label in self.reverse_id_map:
                internal_id = self.reverse_id_map[label]
                internal_ids.append(internal_id)
                del self.id_map[internal_id]
                del self.reverse_id_map[label]

        if internal_ids:
            selector = faiss.IDSelectorBatch(len(internal_ids), np.array(internal_ids, dtype='int64'))
            self.index.remove_ids(selector)