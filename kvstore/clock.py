"""时钟抽象。

为什么单独抽出来：TTL 完全依赖"现在几点"，而老师要求实验里必须能注入
"系统时间突然倒退一分钟"这种故障。把时钟做成可注入的对象，
实验时就能随时拨动时间，不用真的去改操作系统。
"""
import time


class Clock:
    """原始墙钟。offset 用于实验里人为拨动时间。"""

    def __init__(self):
        self._offset = 0.0

    def now(self) -> float:
        return time.time() + self._offset

    def skew(self, delta: float) -> None:
        """拨动时钟：delta 为负表示时间倒退。"""
        self._offset += delta


class RollbackSafeClock(Clock):
    """防倒退时钟：忽略任何导致时间变小的回拨（钳位到观测到的最大值）。

    取舍（写进 PPT）：系统时间倒退 60 秒时，TTL 会跟着"暂停"60 秒，
    但绝不会把已经过期的 key 复活。
    如果直接用原始墙钟，倒退会让已过期的 key 重新变活，等于读到脏数据。
    """

    def __init__(self):
        super().__init__()
        self._last = 0.0

    def now(self) -> float:
        t = super().now()
        if t < self._last:
            return self._last
        self._last = t
        return t
