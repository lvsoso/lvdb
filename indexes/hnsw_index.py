import os
import logging as logger
import numpy as np
import hnswlib
from typing import Optional, Set

from constants import MetricType



class RoaringBitmapIDFilter:
    def __init__(self, bitmap: Optional[Set[int]] = None):
        self.bitmap = bitmap if bitmap is not None else set()

    def __call__(self, label: int) -> bool:
        """
        检查标签是否在位图中
        :param label: 要检查的标签
        :return: 如果bitmap为空返回True，否则检查标签是否在bitmap中
        """
        if not self.bitmap:
            return True
        return label in self.bitmap


class HNSWIndex:

    def __init__(self, dim: int, num_data: int, metric: MetricType, M: int = 32, ef_construction: int = 200):
        """
        初始化 HNSW 索引
        :param dim: 向量维度
        :param num_data: 预计插入的数据量
        :param metric: 距离度量类型
        :param M: 每个节点的最大邻居数
        :param ef_construction: 构建索引时的搜索深度
        """
        self.dim = dim
        space = metric.value.lower()
        
        # 创建索引
        self.index = hnswlib.Index(space=space, dim=dim)
        
        # 初始化索引
        self.index.init_index(
            max_elements=num_data,
            ef_construction=ef_construction,
            M=M
        )

    def insert_vectors(self, vectors: list, label: int):
        """
        插入向量
        :param vectors: 向量数据，一维列表
        :param label: 向量标签，整数
        """
        # 将输入列表转换为 numpy 数组并重塑维度
        vector = np.array(vectors).reshape(1, -1).astype('float32')
        labels = np.array([label])
        self.index.add_items(vector, labels)

    def search_vectors(self, query: list, k: int, bitmap=None, ef_search: int = 50):
        """
        查询向量
        :param query: 查询向量，一维列表
        :param k: 返回最近邻的数量
        :param bitmap: 可选的位图过滤器
        :param ef_search: 搜索时的搜索深度
        :return: (labels, distances) 元组，包含最近邻的标签和距离
        """
        query = np.array(query).reshape(1, -1).astype('float32')
        self.index.set_ef(ef_search)

        # 创建过滤器
        id_filter = RoaringBitmapIDFilter(bitmap)
        
        # 执行搜索，获取更多的候选项以应对过滤
        labels, distances = self.index.knn_query(query, k=k, num_threads=1, filter=id_filter)

        return labels[0].tolist(), distances[0].tolist()

    def save_index(self, file_path: str) -> None:
        """
        保存索引到文件
        :param file_path: 保存路径
        """
        try:
            self.index.save_index(file_path)
            logger.info(f"Successfully saved index to {file_path}")
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
                self.index.load_index(
                    file_path,
                    max_elements=self.index.max_elements
                )
                logger.info(f"Successfully loaded index from {file_path}")
            else:
                logger.warning(f"File not found: {file_path}. Skipping loading index.")
        except Exception as e:
            logger.error(f"Failed to load index: {str(e)}")
            raise