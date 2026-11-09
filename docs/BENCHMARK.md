# 性能测量

日期：2026-11-09，当前完整功能版本，Release 构建。
环境及挂载点见 [environment.txt](results/environment.txt)，数据在 WSL2 的 ext4，
不是 /tmp 的 tmpfs。

## 复现

~~~bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j 2
python3 tools/benchmark.py ./build-release/raftkv --operations 50 --rounds 3
~~~

## 口径

- 一台 WSL 中三个真实节点进程，固定三成员。
- 每轮全新目录，先完成选举，再向已知 Leader 顺序发送 50 个 PUT。
- 每个 value 为 64 字节，使用不同 key；每个请求新建 TCP 连接。
- 包含请求收发、日志复制、多数派持久化、commit 同步、状态机应用与触发的快照。
- snapshot-every=32。
- 不包含启动/选举、测量后的全部节点突然退出、重启及逐项 GET 校验。
- 计时阶段不隐藏重试；收到非 OK 结果会中止基准。
- 延迟为客户端 RPC 的端到端耗时，百分位使用排序后向下取整的索引估计。
  样本较小，不能用于推断生产长尾。

## 实际结果

| 轮次 | 50 次写入耗时 | writes/s | p50 | p95 | p99 | 重启校验 |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | 1.934436 s | 25.85 | 38.72 ms | 40.74 ms | 45.12 ms | 50/50 |
| 2 | 1.942296 s | 25.74 | 39.24 ms | 44.63 ms | 48.24 ms | 50/50 |
| 3 | 1.933326 s | 25.86 | 39.00 ms | 43.28 ms | 43.75 ms | 50/50 |

吞吐量中位数：25.85 writes/s。
原始数据：[benchmark-50x3.json](results/benchmark-50x3.json)。

本项目优先可理解性和正确性。每次状态变化完整重写文件并同步、逐条请求、
短连接、锁内磁盘同步等都有限制。未测量 batching、TCP_NODELAY、追加 WAL、
后台快照等优化的收益，也未测量读密集、高并发、大数据量和跨机网络表现。
不要据此写“高性能分布式存储”。
