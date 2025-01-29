from enum import Enum

DIM = 1
NUM_DATA = 1000

class IndexType(Enum):
    FLAT = "FLAT"
    HNSW = "HNSW"
    UNKNOWN = "UNKNOWN"


class MetricType(Enum):
    L2 = "L2"
    IP = "IP"
    COSINE = 'COSINE'