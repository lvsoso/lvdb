from pydantic import BaseModel
from typing import List, Optional
from constants import IndexType


class SearchRequest(BaseModel):
    vectors: List[float]
    k: int
    index_type: str = IndexType.FLAT


class InsertRequest(BaseModel):
    vectors: List[float]
    id: int
    index_type: str = IndexType.FLAT


class SearchResponse(BaseModel):
    retcode: int = 0
    vectors: Optional[List[int]] = None
    distances: Optional[List[float]] = None
    error_msg: Optional[str] = None


class InsertResponse(BaseModel):
    retcode: int = 0
    error_msg: Optional[str] = None