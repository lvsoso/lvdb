import logging as logger
import traceback
import numpy as np
from fastapi import FastAPI, HTTPException


from constants import IndexType, MetricType, DIM, NUM_DATA, BD_PATH, WAL_PATH, VERSION
from schemas import SearchRequest, SearchResponse, InsertRequest, InsertResponse \
    , UpsertRequest, UpsertResponse, QueryRequest, QueryResponse
from indexes.index_factory import IndexFactory
from vector_database import VectorDatabase

app = FastAPI(debug=True)

"""
初始化索引
"""

index_factory = IndexFactory()
index_factory.init(IndexType.FLAT, DIM)
index_factory.init(IndexType.HNSW, DIM, NUM_DATA)
index_factory.init(IndexType.FILTER)

# 初始化数据库和WAL日志
vector_database = VectorDatabase(index_factory, BD_PATH, WAL_PATH, VERSION)
vector_database.reload_database()

"""
注册接口
"""

@app.post("/search", response_model=SearchResponse)
async def search(request: SearchRequest):
    try:
        match request.index_type:
            case IndexType.FLAT.value:
                index_type = IndexType.FLAT
            case IndexType.HNSW.value:
                index_type = IndexType.HNSW
            case _:
                raise HTTPException(status_code=400, detail="Invalid index type")


        ids, distances = vector_database.search(request)
        # index = index_factory.get_index(index_type)
        # if not index:
        #     raise HTTPException(status_code=400, detail="Index not initialized")

        # ids, distances = index.search_vectors(request.vectors, request.k)
        
        valid_results = [(i, d) for i, d in zip(ids, distances) if i != -1]
        if not valid_results:
            return SearchResponse(vectors=[], distances=[])
            
        result_ids, result_distances = zip(*valid_results)
        return SearchResponse(vectors=list(result_ids), distances=list(result_distances))

    except Exception as e:
        print(traceback.format_exc())
        return SearchResponse(retcode=1, error_msg=str(e))


@app.post("/insert", response_model=InsertResponse)
async def insert(request: InsertRequest):
    try:
        match request.index_type:
            case IndexType.FLAT.value:
                index_type = IndexType.FLAT
            case IndexType.HNSW.value:
                index_type = IndexType.HNSW
            case _:
                raise HTTPException(status_code=400, detail="Invalid index type")

        index = index_factory.get_index(index_type)
        if not index:
            raise HTTPException(status_code=400, detail="Index not initialized")

        index.insert_vectors(request.vectors, request.id)
        return InsertResponse()

    except Exception as e:
        return InsertResponse(retcode=1, error_msg=str(e))


@app.post("/upsert", response_model=UpsertResponse)
async def upsert(request: UpsertRequest):
    """更新或插入向量"""
    try:
        # 获取索引类型
        match request.index_type:
            case IndexType.FLAT.value:
                index_type = IndexType.FLAT
            case IndexType.HNSW.value:
                index_type = IndexType.HNSW
            case _:
                raise HTTPException(status_code=400, detail="Invalid index type")

        vector_database.write_wal_log("upsert", request.dict())
        # 执行更新插入
        vector_database.upsert(request.id, request.dict(), index_type)
        return UpsertResponse()

    except Exception as e:
        print(traceback.format_exc())
        return UpsertResponse(retcode=1, error_msg=str(e))


@app.post("/query", response_model=QueryResponse)
async def query(request: QueryRequest):
    """查询向量数据"""    
    try:
        # 执行查询
        result = vector_database.query(request.id)
        if not result:
            return QueryResponse(data={})
            
        return QueryResponse(data=result)

    except Exception as e:
        return QueryResponse(retcode=1, error_msg=str(e))