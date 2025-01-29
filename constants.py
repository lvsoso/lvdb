from enum import Enum


BD_PATH = '.lvdb'
DIM = 1
NUM_DATA = 1000

class IndexType(Enum):
    FLAT = "FLAT"
    HNSW = "HNSW"
    FILTER = "FILTER"
    UNKNOWN = "UNKNOWN"


class MetricType(Enum):
    L2 = "L2"
    IP = "IP"
    COSINE = 'COSINE'


class Operation(Enum):
    EQUAL = "eq"
    NOT_EQUAL = "ne"
