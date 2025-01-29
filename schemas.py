from pydantic import BaseModel
from typing import List, Optional


class SearchRequest(BaseModel):
    vectors: List[float]
    k: int
    index_type: str = "FLAT"


class InsertRequest(BaseModel):
    vectors: List[float]
    id: int
    index_type: str = "FLAT"


class SearchResponse(BaseModel):
    retcode: int = 0
    vectors: Optional[List[int]] = None
    distances: Optional[List[float]] = None
    error_msg: Optional[str] = None


class InsertResponse(BaseModel):
    retcode: int = 0
    error_msg: Optional[str] = None