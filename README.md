# RaftKV

C++20 实现的固定三节点 Raft KV 服务：三个独立进程，通过 TCP 选举、
复制日志、形成多数派提交，将命令应用到各自的 KV 状态机。

支持 PUT / GET / DELETE / SCAN、持久化恢复、快照与 InstallSnapshot、
写请求去重，以及基于已提交日志屏障的强一致读取。

这是可运行的学习项目，不宣称生产可用。KV 状态机使用 std::map；
持久化由自行实现的 Raft 日志和快照提供，没有使用 RocksDB、LevelDB、SQLite。
原 MiniKV 的 LSM 引擎没有接入，两个项目相互独立。

## 编译

环境：Linux / WSL Ubuntu，C++20 编译器、CMake >= 3.16、Python 3。
不依赖第三方 C++ 库。

~~~bash
git clone https://github.com/beast621/raft-kv.git
cd raft-kv
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j 2
~~~

## 启动三个节点

在仓库根目录，三个终端分别执行：

~~~bash
# 终端 1
./build/raftkv node --id 1 --dir ./data/node1
~~~

~~~bash
# 终端 2
./build/raftkv node --id 2 --dir ./data/node2
~~~

~~~bash
# 终端 3
./build/raftkv node --id 3 --dir ./data/node3
~~~

默认 Raft 端口为 9101 / 9102 / 9103，客户端端口为 9201 / 9202 / 9203，
均绑定 127.0.0.1。必须使用不同数据目录。
约 0.9～1.5 秒选举超时后，日志输出 LEADER。普通运行不模拟节点故障。

## 使用客户端

~~~bash
./build/raftkv client put a one
./build/raftkv client get a
./build/raftkv client put a two
./build/raftkv client delete a
./build/raftkv client get a
./build/raftkv client scan a z
./build/raftkv client status
~~~

输出为 JSON。客户端自动尝试配置的三个节点，并根据 NOT_LEADER 回复转向 Leader。
OK / NOT_FOUND 的退出码为 0，服务层错误为 2，参数或启动错误为 1。
status 是本地诊断，可返回任意节点的本地状态，不提供强一致数据读取语义。

PUT / DELETE 通过 client-id + seq 去重。默认每次启动客户端生成新身份；
跨进程重试必须显式保留相同身份、序号和命令：

~~~bash
./build/raftkv client --client-id demo --seq 1 put a one
# 重复执行上条，只确认原请求，不再次覆盖后来写入的新值。
./build/raftkv client --client-id demo --seq 1 put a one
./build/raftkv client --client-id demo --seq 2 delete a
~~~

每个身份只允许一个逻辑请求在途，序号从 1 连续递增。
重用同一序号发送不同命令返回 ID_REUSE；跳号返回 SEQUENCE_GAP；
早于该会话最近请求的序号返回 STALE_SEQUENCE。
业务层失败 LIMIT / NOT_FOUND 也可能是已提交结果，并推进会话序号。
超时或 RETRY 不代表回滚；操作可能稍后提交，应使用相同身份重试。

SCAN 使用字符串字节字典序的 [start, end) 区间，过滤删除项。
单次最多 100 行、约 64 KiB 的 JSON 行数据；more=true 表示有剩余数据。
GET / SCAN 会复制一个空操作屏障后才读取，网络分区中的少数派无法完成这种读取。

## 多机地址与诊断

所有节点必须提供完全相同、顺序按节点 1 / 2 / 3 排列的地址配置：

~~~bash
./build/raftkv node --id 1 --dir ./data/node1 \
  --peers 192.168.1.11:9101,192.168.1.12:9102,192.168.1.13:9103 \
  --clients 192.168.1.11:9201,192.168.1.12:9202,192.168.1.13:9203
~~~

对应节点会绑定配置中的自身 IPv4 地址。客户端使用相应的 --servers 三地址列表。
没有 TLS、身份认证或访问控制，只适用于可信网络。
实际测试在同一台 WSL 内的三个进程执行，未做跨机器部署验证。

~~~bash
# --once 禁止自动重试，可观察指定节点返回的 NOT_LEADER。
./build/raftkv client --servers 127.0.0.1:9202,127.0.0.1:9201,127.0.0.1:9203 --once status
~~~

数据目录绑定节点编号和完整地址配置。修改这些内容后，旧目录会拒绝启动；
不能通过直接改地址列表实现成员变更，也不能复制一个节点目录给另一个编号使用。
--snapshot-every 默认 64，可设为 4～1024。

## 验证故障恢复

先成功写入数据，再终止 Leader 所在进程，例如在对应终端 Ctrl+C。
其余两个节点应重新选举，客户端仍能读取已确认数据。
恢复节点：使用完全相同的 node 命令和原数据目录启动。

自动测试使用 SIGKILL 验证突然退出。自动测试只在独立临时目录启动自己的进程，
不会终止手动启动的节点。

~~~bash
ctest --test-dir build --output-on-failure
python3 tools/benchmark.py ./build/raftkv --operations 50 --rounds 3
~~~

## 限制与持久化语义

- 固定 3 个投票成员、单 Raft 组；没有动态成员变更、分片、事务、SQL。
- key 非空、最多 256 字节；value 可空、最多 4096 字节；均要求有效 UTF-8。
  命令行用引号传递空格。比较按 UTF-8 字节，不按本地语言排序。
- KV 总 key/value 内容最多 1 MiB；最多 1024 个客户端会话，当前不自动回收。
- 客户端日志积压上限 4096，超过返回 BUSY；内部日志硬上限 8192。
- 网络帧最多 16 MiB。IPv4、每连接一请求一响应，有绝对 I/O 超时和有界队列。
- 每次持久化重写 state.bin，不是高性能追加 WAL；选择简单的原子发布保证易理解。
- 写入成功须完成：本节点日志同步、至少一个 follower 日志同步、提交位置同步、
  本地状态机应用。文件 write 成功本身不表示持久化。
- state.tmp 完整写入、fsync、close、rename、父目录 fsync 后才允许相关确认。
  同步失败时节点停止服务，不在不确定状态下继续确认请求。
- 当前快照复制为单帧；数据与会话容量受限，尚无分块传输、后台压缩或磁盘增量存储。
- 不支持拜占庭故障，不做生产级性能或可用性保证。进程崩溃测试不等同于真实断电实验。

详细说明：[架构与文件格式](docs/DESIGN.md)、[测试与实际结果](docs/TESTING.md)、
[性能测量](docs/BENCHMARK.md)、[学习顺序](docs/LEARNING.md)。

Raft 规则参考 [Ongaro / Ousterhout 原论文](https://raft.github.io/raft.pdf)，重点为图 2、
5.4 节当前任期提交规则与第 7 节快照。
