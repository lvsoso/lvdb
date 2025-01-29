import os
import pickle
import logging as logger
import faiss
import numpy as np
from constants import MetricType


class RoaringBitmapIDSelector:
    def __init__(self, bitmap):
        self.bitmap = bitmap

    def is_member(self, id: int) -> bool:
        return id in self.bitmap if self.bitmap else True


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

    def search_vectors_(self, query: list, k: int, bitmap=None) -> tuple[list[int], list[float]]:
        """
        搜索向量
        :param query: 查询向量
        :param k: 返回的最近邻数量
        :param bitmap: 可选的位图过滤器
        :return: (ids, distances) 元组
        """
        query = np.array(query).reshape(1, -1).astype('float32')
        
        # 创建搜索参数
        params = None
        if bitmap is not None:
            logger.error(" bitmap ")
            selector = RoaringBitmapIDSelector(bitmap)
            params = faiss.SearchParameters(sel = selector)

        distances, indices = self.index.search(query, k, params=params)

        result_ids = [self.id_map.get(idx, -1) for idx in indices[0]]
        return result_ids, distances[0].tolist()

    def search_vectors(self, query: list, k: int, bitmap=None) -> tuple[list[int], list[float]]:
        """
        搜索向量
        :param query: 查询向量
        :param k: 返回的最近邻数量
        :param bitmap: 可选的位图过滤器
        :return: (ids, distances) 元组
        """
        query = np.array(query).reshape(1, -1).astype('float32')
        
        # 如果有位图过滤器，获取更多候选项以应对过滤
        search_k = k * 2 if bitmap is not None else k
        distances, indices = self.index.search(query, search_k)

        # 应用过滤器
        filtered_results = []
        for idx, dist in zip(indices[0], distances[0]):
            label = self.id_map.get(idx, None)
            if label and (bitmap is None or label in bitmap):
                filtered_results.append((label, dist))
                if len(filtered_results) >= k:
                    break

        while len(filtered_results) < k:
            filtered_results.append((-1, 0))

        result_ids, distances = zip(*filtered_results)
        return list(result_ids), list(distances)

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

    def save_index(self, file_path: str) -> None:
        """
        保存索引到文件
        :param file_path: 保存路径
        """
        try:
            faiss.write_index(self.index, file_path)
            with open(f"{file_path}.map", "wb") as f:
                pickle.dump({
                    "id_map": self.id_map,
                    "reverse_id_map": self.reverse_id_map
                }, f)
        except Exception as e:
            logger.error(f"Failed to save index: {str(e)}")
            raise

    def load_index(self, file_path: str) -> None:
        """
        从文件加载索引
        :param file_path: 索引文件路径
        """
        try:
            if os.path.exists(file_path):
                self.index = faiss.read_index(file_path)
                if os.path.exists(f"{file_path}.map"):
                    import pickle
                    with open(f"{file_path}.map", "rb") as f:
                        mapping = pickle.load(f)
                        self.id_map = mapping["id_map"]
                        self.reverse_id_map = mapping["reverse_id_map"]
            else:
                logger.warning(f"File not found: {file_path}. Skipping loading index.")
        except Exception as e:
            logger.error(f"Failed to load index: {str(e)}")
            raise