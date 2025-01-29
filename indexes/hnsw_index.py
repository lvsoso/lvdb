import numpy as np
import hnswlib
from constants import MetricType

class HNSWIndex:

    def __init__(self, dim: int, num_data: int, metric: MetricType, M: int = 16, ef_construction: int = 200):
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

    def search_vectors(self, query: list, k: int, ef_search: int = 50):
        """
        查询向量
        :param query: 查询向量，一维列表
        :param k: 返回最近邻的数量
        :param ef_search: 搜索时的搜索深度
        :return: (labels, distances) 元组，包含最近邻的标签和距离
        """
        query = np.array(query).reshape(1, -1).astype('float32')
        # 设置 ef_search 参数
        self.index.set_ef(ef_search)
        # 执行搜索
        labels, distances = self.index.knn_query(query, k=k)
        return labels[0].tolist(), distances[0].tolist()