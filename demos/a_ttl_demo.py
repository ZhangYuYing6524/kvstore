"""A 的演示脚本 —— 也是视频里属于 A 的那一段素材。

运行： python demos/a_ttl_demo.py
每个实验都按老师的四步走：预测 -> 注入 -> 实际 -> 解释。
"""
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kvstore import Clock, KVNode, RollbackSafeClock


def title(n, text):
    print("\n" + "=" * 60)
    print(f"实验 {n}：{text}")
    print("=" * 60)


def main():
    # ---------- 1. 基本读写 + 版本号 ----------
    title(1, "基本读写与版本号单调递增")
    n = KVNode("A1", sweep_interval=None)
    v1 = n.set("user:1", {"name": "ada"})
    v2 = n.set("user:1", {"name": "ada", "age": 18})
    print(f"  写入两次，version = {v1} -> {v2}（预测：1 -> 2）")
    print(f"  读到 = {n.get('user:1').value}")
    print("  解释：version 由本节点按 key 自增，是后面 C 做复制排序、E 拒绝陈旧消息的依据。")

    # ---------- 2. TTL 懒过期 ----------
    title(2, "TTL 过期（懒过期）")
    n = KVNode("A2", sweep_interval=None)
    n.set("session:9", "token-xyz", ttl=0.5)
    print(f"  写入后立刻读 = {n.get('session:9').value}（预测：能读到）")
    time.sleep(0.6)
    got = n.get("session:9")
    print(f"  等 0.6s 再读 = {got.value if got else None}（预测：None）")
    print(f"  stats: expire_lazy = {n.stats['expire_lazy']}")
    print("  解释：过期数据不会立刻消失，而是在被访问时发现并删除，这叫懒过期。"
          "缺点是没人访问就一直占内存。")

    # ---------- 3. 主动过期：内存真的被释放 ----------
    title(3, "主动过期（后台清扫线程）")
    n = KVNode("A3", sweep_interval=0.05)
    for i in range(1000):
        n.set(f"k{i}", i, ttl=0.2)
    n.start()
    print(f"  写入 1000 个 0.2s 过期的 key，size = {n.size()}（预测：1000）")
    time.sleep(0.5)
    print(f"  0.5s 后 size = {n.size()}（预测：接近 0）")
    print(f"  stats: expire_sweep = {n.stats['expire_sweep']}")
    n.stop()
    print("  解释：懒过期只保证'读不到'，主动过期才真正回收内存，"
          "满足题目'内存受限'的要求。代价是每秒全表扫一遍，key 多时有 CPU 开销。")

    # ---------- 4. 系统时间倒退一分钟 ----------
    title(4, "系统时间突然倒退（-5s 快速复现）")
    raw_clock, safe_clock = Clock(), RollbackSafeClock()
    raw = KVNode("裸墙钟", clock=raw_clock, sweep_interval=None)
    safe = KVNode("防倒退", clock=safe_clock, sweep_interval=None)
    raw.set("config", "v1", ttl=1.0)
    safe.set("config", "v1", ttl=1.0)
    time.sleep(1.3)                       # 真实时间已经超过 TTL
    raw_clock.now(); safe_clock.now()     # 模拟期间其他请求/清扫线程一直在读时钟
    print("  预测：裸墙钟会读到已死的数据，防倒退时钟读到 None")
    raw_clock.skew(-5)
    safe_clock.skew(-5)
    print(f"  实际：裸墙钟 -> {raw.get('config') and 'v1（复活！）'}，"
          f"防倒退 -> {safe.get('config')}")
    print("  解释：TTL 用绝对时间戳，时间倒退会让 expire_at 看起来还没到，"
          "脏数据复活。RollbackSafeClock 记录已观测到的最大时间并拒绝回拨，"
          "取舍是这 5 秒内 TTL 暂停，但绝不返回过期数据。")

    # ---------- 5. 节点重启丢失内存状态 ----------
    title(5, "节点重启后丢失内存状态")
    n = KVNode("A5", sweep_interval=None)
    for i in range(100):
        n.set(f"k{i}", i)
    before = n.size()
    n2 = KVNode("A5-reboot", sweep_interval=None)   # 新进程 = 空字典
    print(f"  重启前 size = {before}，重启后 size = {n2.size()}（预测：100 -> 0）")
    print(f"  重启后读 k0 = {n2.get('k0')}（预测：None）")
    print("  解释：内存缓存必然丢数据，这正是题目说'允许部分丢失'的原因；"
          "要少丢只能靠 C 的复制或 E 的迁移实验来验证。")

    # ---------- 6. 旧消息延迟到达被拒绝 ----------
    title(6, "延迟/重复的消息到达时被版本号拒绝")
    n = KVNode("A6", sweep_interval=None)
    from kvstore import Entry
    n.set("hot", "new-value")
    v_now = n.get("hot").version
    stale = Entry("old-value", version=v_now - 1)
    ok = n.apply_replica("hot", stale)
    print(f"  当前值 = new-value，收到 version={stale.version} 的旧消息 -> 接受? {ok}")
    print(f"  读到的仍是 = {n.get('hot').value}")
    dup = Entry("new-value", version=v_now)
    print(f"  重复收到同版本消息 -> 接受? {n.apply_replica('hot', dup)}")
    print(f"  stats: stale_write = {n.stats['stale_write']}（预测：2）")
    print("  解释：用每 key 的 version 做 last-write-wins 判重，"
          "重复发送和乱序到达都不会破坏数据。")

    print("\n全部实验结束。")


if __name__ == "__main__":
    main()
