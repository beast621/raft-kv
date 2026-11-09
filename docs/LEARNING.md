# 阅读顺序

建议先完整跑通 README 的演示，再按函数逐步阅读。

1. main.cpp 的 node_main：为什么一个进程只保存一个节点？
2. transport.cpp 的 transfer：为什么一次 recv 不一定读到完整消息？
3. storage.cpp 的 save/load：write、fsync、rename、目录 fsync 分别解决什么？
4. raft.cpp 的 campaign/peer_request：如何防止同一任期投给两个人？为何要比较日志？
5. replicate/advance_commit：为什么不能直接提交多数节点上的旧任期日志？
6. client_request/apply_committed：什么时候向客户端报告成功？读取为什么也有日志屏障？
7. compact/InstallSnapshot：落后副本需要的日志已被删除时，如何追赶？
8. Machine::apply：重复请求为何不能重新覆盖后来写入的值？
9. tests/integration.py：未提交写入为何既可能消失，也可能后来提交？

这份代码是独立 Raft KV 版本。MiniKV 的 MemTable/SSTable/Manifest 没有接入。
后续如要复用 LSM，需要定义 apply(index, command) 的持久化边界，
让应用进度、去重会话和 KV 状态在重启后保持一致，不能只在 RPC 里直接调用 Put。

简历应描述实际阅读、验证并能解释的功能。不要把“代码已生成”当作“已掌握全部机制”。
