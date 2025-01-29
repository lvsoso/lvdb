import numpy as np
from fastapi import FastAPI, HTTPException


from constants import IndexType, MetricType, DIM, NUM_DATA
from schemas import SearchRequest, SearchResponse, InsertRequest, InsertResponse
from indexes.index_factory import IndexFactory

app = FastAPI(debug=True)

"""
初始化索引
"""

index_factory = IndexFactory()
index_factory.init(IndexType.FLAT, DIM)
index_factory.init(IndexType.HNSW, DIM, NUM_DATA)


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

        index = index_factory.get_index(index_type)
        if not index:
            raise HTTPException(status_code=400, detail="Index not initialized")

        ids, distances = index.search_vectors(request.vectors, request.k)
        
        valid_results = [(i, d) for i, d in zip(ids, distances) if i != -1]
        if not valid_results:
            return SearchResponse(vectors=[], distances=[])
            
        result_ids, result_distances = zip(*valid_results)
        return SearchResponse(vectors=list(result_ids), distances=list(result_distances))

    except Exception as e:
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