"""A 负责的模块：带 TTL 的单机 KV 节点。

这是全组的地基，所以接口必须稳定，其他四个人都依赖它：
    node.get(key)                     -> Entry | None
    node.set(key, value, ttl=None)    -> version (int)
    node.delete(key)                  -> version (int)
    node.apply_replica(key, entry)    -> bool     给 C（异步复制）用
    node.snapshot() / load_snapshot() -> 给 C/E（迁移、重启实验）用
    node.stats                        -> 统计计数，给 E（验证）用

B（LRU/LFU）通过构造参数 store 挂进来，不需要改本文件；
只需要实现 on_access(key) 和 on_write(key) -> list[str] 两个方法。
"""
import threading
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from .clock import RollbackSafeClock


@dataclass
class Entry:
    value: Any
    version: int                          # 每个 key 单调递增，用来拒绝陈旧/重复消息
    expire_at: Optional[float] = None     # 绝对过期时刻（墙钟秒），None = 永不过期
    deleted: bool = False                 # 墓碑标记，保证显式 delete 在复制后不被旧值覆盖

    def copy(self) -> "Entry":
        return Entry(self.value, self.version, self.expire_at, self.deleted)

    def is_expired(self, now: float) -> bool:
        return self.expire_at is not None and now >= self.expire_at


class KVNode:
    def __init__(self, name: str = "node", clock=None, store=None, sweep_interval: float = 0.1):
        self.name = name
        self.clock = clock or RollbackSafeClock()
        self._data: Dict[str, Entry] = {}
        self._lock = threading.RLock()
        self._store = store                 # B 的淘汰策略，可为 None
        self._sweep_interval = sweep_interval
        self._stop = threading.Event()
        self._sweeper: Optional[threading.Thread] = None
        self.stats = {
            "get": 0, "hit": 0, "miss": 0, "set": 0, "delete": 0,
            "expire_lazy": 0, "expire_sweep": 0, "evict": 0, "stale_write": 0,
        }

    # ---------------- 生命周期 ----------------

    def start(self) -> "KVNode":
        """启动主动过期线程（老师说的 '内存受限' 要求过期数据真的被释放）。"""
        if self._sweep_interval is None or self._sweeper is not None:
            return self
        self._sweeper = threading.Thread(
            target=self._sweep_loop, name=f"{self.name}-sweeper", daemon=True
        )
        self._sweeper.start()
        return self

    def stop(self) -> None:
        self._stop.set()

    def _sweep_loop(self) -> None:
        while not self._stop.wait(self._sweep_interval):
            self.sweep_once()

    # ---------------- 读写 ----------------

    def get(self, key: str) -> Optional[Entry]:
        with self._lock:
            self.stats["get"] += 1
            e = self._data.get(key)
            if e is None or e.deleted:
                self.stats["miss"] += 1
                return None
            if e.is_expired(self.clock.now()):
                self._drop(key, "expire_lazy")      # 懒过期：访问时顺手删
                self.stats["miss"] += 1
                return None
            self.stats["hit"] += 1
            if self._store is not None:
                self._store.on_access(key)
            return e.copy()

    def set(self, key: str, value: Any, ttl: Optional[float] = None) -> int:
        with self._lock:
            old = self._data.get(key)
            version = (old.version + 1) if old else 1
            expire_at = None if ttl is None else self.clock.now() + ttl
            self._data[key] = Entry(value, version, expire_at, deleted=False)
            self.stats["set"] += 1
            if self._store is not None:
                for k in (self._store.on_write(key) or []):
                    self._data.pop(k, None)
                    self.stats["evict"] += 1
            return version

    def delete(self, key: str) -> int:
        """显式删除要留墓碑：否则复制到从节点时，被删掉的旧值会"复活"。"""
        with self._lock:
            old = self._data.get(key)
            version = (old.version + 1) if old else 1
            self._data[key] = Entry(None, version, None, deleted=True)
            self.stats["delete"] += 1
            return version

    # ---------------- 给 C（异步复制）和 E（实验）用 ----------------

    def apply_replica(self, key: str, entry: Entry) -> bool:
        """应用远端复制过来的写。

        返回 False 表示被拒绝：说明这条消息比本地状态旧（重复发送 / 延迟乱序）。
        这一行版本比较就是 E 的"消息重复三次""延迟 10 秒乱序"实验能通过的原因。
        """
        with self._lock:
            cur = self._data.get(key)
            if cur is not None and cur.version >= entry.version:
                self.stats["stale_write"] += 1
                return False
            self._data[key] = entry.copy()
            return True

    def snapshot(self) -> Dict[str, Entry]:
        """导出快照（跳过已过期和墓碑），用于新节点迁移 / 重启恢复。"""
        with self._lock:
            now = self.clock.now()
            return {
                k: e.copy() for k, e in self._data.items()
                if not e.deleted and not e.is_expired(now)
            }

    def load_snapshot(self, data: Dict[str, Entry]) -> None:
        with self._lock:
            self._data = {k: e.copy() for k, e in data.items()}

    # ---------------- 辅助 ----------------

    def sweep_once(self) -> int:
        """主动过期：整表扫描一遍，返回清掉的条数。"""
        with self._lock:
            now = self.clock.now()
            dead = [k for k, e in self._data.items() if not e.deleted and e.is_expired(now)]
            for k in dead:
                self._drop(k, "expire_sweep")
            return len(dead)

    def _drop(self, key: str, reason: str) -> None:
        self._data.pop(key, None)
        self.stats[reason] += 1

    def size(self) -> int:
        with self._lock:
            return len(self._data)

    def raw_entry(self, key: str) -> Optional[Entry]:
        """不加过期判断地看内部状态，仅实验/调试用。"""
        with self._lock:
            e = self._data.get(key)
            return e.copy() if e else None

    def __repr__(self) -> str:
        return f"<KVNode {self.name} size={self.size()}>"
