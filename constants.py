from enum import Enum


VERSION = "1.0"
BD_PATH = ".lvdb"
WAL_PATH = "wal.log"
SNAPSHOT_FOLDER_PATH = ".snapshots"
SNAPSHOTS_MAX_LOG_ID = "snapshots_max_log_id"


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
