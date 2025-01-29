import faiss
import numpy as np

if __name__ == '__main__':
    import faiss
    d = 64                           # 向量维度
    nb = 100000                      # 数据库大小（基础向量数量）
    nq = 10000                       # 查询向量数量
    np.random.seed(1234)             # 设置随机种子，确保每次运行结果一致

    # 生成 100000 个 64 维的随机向量
    xb = np.random.random((nb, d)).astype('float32')
    # 为每个向量的第一个维度添加一个递增的小值
    xb[:, 0] += np.arange(nb) / 1000.
    # 生成 10000 个 64 维的随机向量作为查询集
    xq = np.random.random((nq, d)).astype('float32')
    # 同样为查询向量的第一个维度添加递增值
    xq[:, 0] += np.arange(nq) / 1000.
    # 欧式距离
    index = faiss.IndexFlatL2(d)
    # 是否还需训练
    print(index.is_trained)
    index.add(xb)
    # 包含的总向量数
    print(index.ntotal)

    # topK的K值
    k = 4
    # xq为待检索向量，返回的I为每个待检索query最相似TopK的索引list，D为其对应的距离
    D, I = index.search(xq, k)
    print(I[:5])
    print(D[-5:])