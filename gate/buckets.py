# gate/buckets.py
from dataclasses import dataclass
from enum import Enum


class Dir(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"


@dataclass
class BucketSignal:
    bucket: str          # "A", "B", "C"
    direction: Dir
    strength: float      # 0.0 .. 1.0
    reason: str
    active: bool = True   # False = bucket cannot act (e.g. C with no usable levels)
    extra: dict | None = None   # structured levels for replay/audit (e.g. wall distances)