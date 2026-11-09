# 架构与文件格式

## 文件职责

- codec.hpp：大端整数、长度前缀字符串、UTF-8 与输入边界检查、JSON 转义。
- transport.cpp：非阻塞 socket、poll、连接截止时间、分次收发、帧大小限制。
- storage.cpp：KV 状态机、写会话去重、状态序列化、完整性检查、安全文件发布、目录锁。
- raft.cpp：选举、日志复制、提交、快照、强一致读取和工作线程调度。
- main.cpp：节点/客户端参数与重定向。
- tests：独立数据目录里的组件与真实三进程测试。
- tools/benchmark.py：顺序写入基准，并在突然终止全部进程后逐项校验。

## 线程与锁

每节点有一个 Raft ticker、两个 peer RPC worker、四个 client worker。
主线程用 poll 监听两个端口，为不同用途分配独立的有界队列（每队列 64 个连接）。
客户端等待提交时释放状态锁，peer worker 可以继续接收投票和日志。

协议状态、KV、会话、等待结果由一把 mutex 保护。
文件同步发生在锁内，保持状态转换顺序；跨节点 RPC 在锁外，避免网络等待锁死协议。
收到回复后重新核对当前任期和角色，忽略过期竞选或复制结果。
没有用“拿锁就永久保证 Leader 身份”的假设。

## 选举

- 超时随机 900～1500 ms；心跳目标间隔 100 ms；对一个 peer 的 RPC 截止时间 300 ms。
- 自增任期、自投票先持久化，再请求其他成员。
- 投票比较最后一条日志的任期，再比较序号；快照边界也参与比较。
- 每任期只能投给一个成员，重复请求可以再次确认同一票。
- 当选后追加当前任期的空操作；多数副本只直接提交当前任期日志。
- 比自己高的任期立即同步到磁盘并退回 Follower。
- 不含 PreVote、CheckQuorum 和 Leader lease。失去多数派的旧 Leader 可能暂时保留角色，
  但无法提交新日志，因而不能确认新的写入或读取屏障。

## 复制、提交、读取

nextIndex / matchIndex 按绝对日志序号记录，每次至多发送 16 条。
Follower 校验 previous index/term；不匹配时回复回退提示。
仅在发现冲突时截掉未提交尾部，禁止覆盖已经提交的记录。
空心跳也要经过索引匹配，只能提交本次已经验证的范围。

写入命令先在多数成员持久化，Leader 持久化 commit 再顺序应用，最后才返回结果。
GET / SCAN 也追加一个当前任期屏障，提交并应用后在锁内读状态机。
这是为教学选择的简单强一致读方案：每次读也有复制、写盘和快照成本。
没有声称只向 Leader 发一个普通 GET 就自动具备线性一致性。

## 会话语义

会话只保存最后一个请求的 seq、原命令签名和执行结果。
同 seq/同命令返回原结果，不重复应用；同 seq/不同命令拒绝。
较旧序号和跳号均拒绝。每个会话须串行发起逻辑请求。
会话信息纳入快照，重启及 InstallSnapshot 后仍然有效。
会话不会自动过期；达到 1024 个后，新身份的写操作返回 LIMIT。

一次请求没有收到成功，不代表状态已回滚。恢复后可能发现该命令已提交。
返回 RETRY 后应使用完全相同的 client-id、seq、命令重试。

## 快照

applied - snapshotIndex 达到阈值后，复制当前 KV 和会话为新快照，
保留未提交后缀，更新 snapshotIndex / snapshotTerm。
快照与后缀日志通过同一次 state.bin 发布切换，不跨文件分别覆盖。
发送方发现 nextIndex 已落到快照边界之前，改发 InstallSnapshot。
接收方只向前安装快照；只有边界任期匹配时保留本地后缀，随后正常复制剩余日志。

## state.bin 格式 V2

所有整数都是无符号 64 位大端。text 为 u64 长度 + 原始字节。

外层：
1. text("RAFTKV02")
2. u64 FNV-1a-64(body)
3. text(body)

body：
1. u64 nodeId + text(固定成员/地址配置)
2. u64 currentTerm、votedFor、commitIndex
3. u64 snapshotIndex、snapshotTerm
4. snapshot machine
5. u64 后缀日志条数
6. 每条：u64 entryTerm + command

machine：
- u64 KV 条数；每条 text(key)、text(value)。
- u64 会话条数；每条 text(clientId)、u64 seq、text(signature)、text(result)。

command：
u64 op、text(clientId)、u64 seq、text(key)、text(value)。
signature：u64 op、text(key)、text(value)。

启动时验证格式、长度、校验、任期顺序、commit 边界、节点及配置。
FNV 检查用于发现内容损坏，不防恶意篡改，不保证识别所有损坏。

持久化流程：
state.tmp 写全 → fsync(file) → close → rename(state.bin) → fsync(data directory)。
旧 state.bin 在 rename 前保留；重启只读取发布文件，不猜测临时文件是否“看起来更新”。
创建数据目录时同步相关祖先目录。进程持有 LOCK 的独占 flock。

## TCP 协议

每条消息：4 字节无符号大端 payload 长度 + payload，长度 1～16 MiB。
一次连接只传一个请求和一个响应，关闭后下次重新连接。
header 和 body 分开或分多次到达均可；读到半帧后断连视为请求失败。
send 使用 MSG_NOSIGNAL，所有网络等待受绝对截止时间约束。

Peer 请求公共前缀：
u64 type、text(cluster config)、u64 senderId、u64 term。
- type=1：lastIndex、lastTerm。
- type=2：prevIndex、prevTerm、leaderCommit、count、count 个 Entry。
- type=3：snapshotIndex、snapshotTerm、machine。
Peer 响应：term、success(0/1)、hint。

Client 请求为 command（op 1 Put / 2 Delete / 3 Get / 4 Scan / 5 Status）。
Client 响应：text(status)、text(leader client endpoint)、text(JSON body)。

配置校验和成员编号校验不是身份认证。协议只适用于可信节点与可信网络。
