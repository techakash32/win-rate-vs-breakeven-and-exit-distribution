from dataclasses import dataclass, field
from enum import Enum


class Dir(Enum):
    LONG = "LONG"
    SHORT = "SHORT"
    NEUTRAL = "NEUTRAL"


@dataclass
class BucketSignal:
    bucket: str
    direction: Dir
    strength: float
    reason: str
    disabled: bool = False
    extra: dict = field(default_factory=dict)

    @property
    def active(self) -> bool:
        return self.direction != Dir.NEUTRAL
