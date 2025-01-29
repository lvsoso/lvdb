import numpy as np
from fastapi import FastAPI, HTTPException


from constants import IndexType, MetricType, Dim
from schemas import SearchRequest, SearchResponse, InsertRequest, InsertResponse
from indexes.index_factory import IndexFactory

app = FastAPI()

index_factory = IndexFactory()
index_factory.init(IndexType.FLAT, Dim)

@app.post("/search", response_model=SearchResponse)
async def search(request: SearchRequest):
    try:
        index_type = IndexType.FLAT if request.index_type == "FLAT" else IndexType.UNKNOWN
        if index_type == IndexType.UNKNOWN:
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
        index_type = IndexType.FLAT if request.index_type == "FLAT" else IndexType.UNKNOWN
        if index_type == IndexType.UNKNOWN:
            raise HTTPException(status_code=400, detail="Invalid index type")

        index = index_factory.get_index(index_type)
        if not index:
            raise HTTPException(status_code=400, detail="Index not initialized")

        index.insert_vectors(request.vectors, request.id)
        return InsertResponse()

    except Exception as e:
        return InsertResponse(retcode=1, error_msg=str(e))