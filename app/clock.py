import time
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...


class SystemClock:
    def now_ms(self) -> int:
        return int(time.time() * 1000)


class FakeClock:
    """Deterministic time for tests: nothing moves unless the test says so."""

    def __init__(self, start_ms: int = 1_800_000_000_000) -> None:
        self.t = start_ms

    def now_ms(self) -> int:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += int(seconds * 1000)
