from enum import Enum

Dim = 1

class IndexType(Enum):
    FLAT = "FLAT"
    UNKNOWN = "UNKNOWN"


class MetricType(Enum):
    L2 = "L2"
    IP = "IP"