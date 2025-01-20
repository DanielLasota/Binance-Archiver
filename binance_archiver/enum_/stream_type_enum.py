from enum import Enum, auto


class StreamType(Enum):
    DIFFERENCE_DEPTH_STREAM = auto()
    TRADE_STREAM = auto()
    DEPTH_SNAPSHOT = auto()
    ...
