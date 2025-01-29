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

    def insert_vectors(self, vectors: list, label: int):
        vector = np.array(vectors).reshape(1, -1).astype('float32')
        self.index.add(vector)
        self.id_map[self.index.ntotal - 1] = label

    def search_vectors(self, query: list, k: int):
        query = np.array(query).reshape(1, -1).astype('float32')
        distances, indices = self.index.search(query, k)
        result_ids = [self.id_map.get(idx, -1) for idx in indices[0]]
        return result_ids, distances[0].tolist()