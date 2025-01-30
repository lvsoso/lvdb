from typing import List, Dict, Optional, Union
from dataclasses import dataclass
from enum import Enum

import requests


class IndexType(Enum):
    FLAT = "FLAT"
    HNSW = "HNSW"


@dataclass
class VectorSearchResult:
    vectors: List[int]
    distances: List[float]
    retcode: int = 0
    error_msg: str = ""


@dataclass
class VectorUpsertResult:
    retcode: int = 0
    error_msg: str = ""


@dataclass
class VectorQueryResult:
    data: Dict
    retcode: int = 0
    error_msg: str = ""


class VectorDBClient:
    def __init__(self, base_url: str = "http://localhost:8000"):
        self.base_url = base_url.rstrip('/')

    def search(self, 
              vectors: List[float], 
              k: int, 
              index_type: IndexType = IndexType.FLAT,
              filter_condition: Optional[Dict] = None) -> VectorSearchResult:
        url = f"{self.base_url}/search"
        payload = {
            "vectors": vectors,
            "k": k,
            "index_type": index_type.value
        }
        if filter_condition:
            payload["filter"] = filter_condition

        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            result = response.json()
            return VectorSearchResult(**result)
        except requests.exceptions.RequestException as e:
            return VectorSearchResult(
                vectors=[],
                distances=[],
                retcode=1,
                error_msg=str(e)
            )

    def upsert(self, 
               vectors: List[float], 
               id: int, 
               index_type: IndexType = IndexType.FLAT,
               extra_fields: Optional[Dict] = None) -> VectorUpsertResult:
        url = f"{self.base_url}/upsert"
        payload = {
            "vectors": vectors,
            "id": id,
            "index_type": index_type.value
        }
        if extra_fields:
            payload.update(extra_fields)

        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            result = response.json()
            return VectorUpsertResult(**result)
        except requests.exceptions.RequestException as e:
            return VectorUpsertResult(retcode=1, error_msg=str(e))

    def query(self, id: int) -> VectorQueryResult:
        """
        查询向量数据
        :param id: 向量ID
        :return: 查询结果
        """
        url = f"{self.base_url}/query"
        payload = {"id": id}

        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            result = response.json()
            return VectorQueryResult(**result)
        except requests.exceptions.RequestException as e:
            return VectorQueryResult(
                data={},
                retcode=1,
                error_msg=str(e)
            )