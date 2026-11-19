"""A 的单元测试。不需要 pytest，直接 python tests/test_a.py 就能跑。

每组机制一个测试，方便演示时逐条说"我知道它为什么对"。
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kvstore import Clock, Entry, KVNode, RollbackSafeClock


def test_basic_get_set():
    n = KVNode(sweep_interval=None)
    assert n.get("none") is None
    assert n.set("k", "v1") == 1
    assert n.set("k", "v2") == 2
    assert n.get("k").value == "v2"
    print("  ok  test_basic_get_set")


def test_ttl_lazy_expire():
    n = KVNode(sweep_interval=None)
    n.set("k", "v", ttl=0.1)
    assert n.get("k").value == "v"
    time.sleep(0.15)
    assert n.get("k") is None
    assert n.size() == 0          # 懒过期顺带把 key 删掉了
    assert n.stats["expire_lazy"] == 1
    print("  ok  test_ttl_lazy_expire")


def test_no_ttl_never_expires():
    n = KVNode(sweep_interval=None)
    n.set("k", "v")
    time.sleep(0.15)
    assert n.get("k").value == "v"
    print("  ok  test_no_ttl_never_expires")


def test_sweep_reclaims_memory():
    n = KVNode(sweep_interval=None)
    for i in range(200):
        n.set(f"k{i}", i, ttl=0.05)
    time.sleep(0.1)
    assert n.size() == 200        # 没访问过，懒过期不会触发
    removed = n.sweep_once()
    assert removed == 200
    assert n.size() == 0
    assert n.stats["expire_sweep"] == 200
    print("  ok  test_sweep_reclaims_memory")


def test_delete_tombstone():
    n = KVNode(sweep_interval=None)
    n.set("k", "v")
    ver = n.delete("k")
    assert ver == 2
    assert n.get("k") is None
    assert n.raw_entry("k").deleted is True    # 墓碑还在，复制时能覆盖旧值
    print("  ok  test_delete_tombstone")


def test_replica_rejects_stale_and_duplicate():
    n = KVNode(sweep_interval=None)
    n.set("k", "v1")
    assert n.apply_replica("k", Entry("v2", version=2)) is True
    assert n.get("k").value == "v2"
    assert n.apply_replica("k", Entry("old", version=1)) is False   # 陈旧
    assert n.apply_replica("k", Entry("v2", version=2)) is False    # 重复
    assert n.get("k").value == "v2"
    assert n.stats["stale_write"] == 2
    print("  ok  test_replica_rejects_stale_and_duplicate")


def test_rollback_safe_clock_blocks_resurrection():
    raw_clock, safe_clock = Clock(), RollbackSafeClock()
    raw = KVNode("raw", clock=raw_clock, sweep_interval=None)
    safe = KVNode("safe", clock=safe_clock, sweep_interval=None)
    raw.set("k", "v", ttl=0.1)
    safe.set("k", "v", ttl=0.1)
    time.sleep(0.15)
    raw_clock.now(); safe_clock.now()
    raw_clock.skew(-10)
    safe_clock.skew(-10)
    assert raw.get("k") is not None     # 裸墙钟：过期数据复活
    assert safe.get("k") is None        # 防倒退：仍然过期
    print("  ok  test_rollback_safe_clock_blocks_resurrection")


def test_snapshot_skips_expired():
    n = KVNode(sweep_interval=None)
    n.set("alive", 1)
    n.set("dead", 2, ttl=0.01)
    n.set("gone", 3)
    n.delete("gone")
    time.sleep(0.02)
    snap = n.snapshot()
    assert set(snap.keys()) == {"alive"}
    print("  ok  test_snapshot_skips_expired")


def test_reboot_loses_memory():
    n = KVNode(sweep_interval=None)
    for i in range(10):
        n.set(f"k{i}", i)
    rebooted = KVNode(sweep_interval=None)
    assert rebooted.size() == 0
    assert rebooted.get("k0") is None
    print("  ok  test_reboot_loses_memory")


TESTS = [v for k, v in sorted(globals().items()) if k.startswith("test_")]


if __name__ == "__main__":
    failed = 0
    for t in TESTS:
        try:
            t()
        except AssertionError as e:
            failed += 1
            print(f"  FAIL {t.__name__}: {e}")
    print(f"\n{len(TESTS) - failed}/{len(TESTS)} passed")
    sys.exit(1 if failed else 0)
