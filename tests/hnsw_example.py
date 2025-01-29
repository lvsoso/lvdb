import numpy as np
import hnswlib


if __name__ == '__main__':
    # 创建索引
    dim = 1
    num_vectors = 1000

    space = 'l2'
    M = 16
    ef_construction = 200
    index = hnswlib.Index(space=space, dim=dim)
    
    # 初始化索引
    index.init_index(
        max_elements=num_vectors,
        ef_construction=ef_construction,
        M=M
    )

    
    data = np.random.random((100, dim)).astype('float32')
    labels = np.arange(100)
    index.add_items(data, labels)

    query = np.random.random(dim).astype('float32')

    index.set_ef(50)
    labels, distances = index.knn_query(query, k=5)
    
    print(f"最近邻标签: {labels[0]}")
    print(f"对应距离: {distances[0]}")