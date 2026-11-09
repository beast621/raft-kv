# 测试与实际结果

验证日期：2026-11-09（Asia/Shanghai）。
Linux / WSL2、GCC 15.2、CMake 4.2.3、Python 3.14.4，数据临时目录在 WSL ext4。
具体环境见 [environment.txt](results/environment.txt)。

## 运行

~~~bash
cmake -S . -B build-release -DCMAKE_BUILD_TYPE=Release
cmake --build build-release -j 2
ctest --test-dir build-release --output-on-failure
~~~

内存与未定义行为检查：

~~~bash
cmake -S . -B build-asan -DCMAKE_BUILD_TYPE=Debug -DRAFTKV_SANITIZE=ON
cmake --build build-asan -j 2
ASAN_OPTIONS=detect_leaks=1:abort_on_error=1 \
UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 \
ctest --test-dir build-asan --output-on-failure
~~~

两种配置均通过 2 个 CTest 入口。入口内包含多个场景，不将入口数冒充独立测试数量。

| 配置 | 结果 | 总耗时 |
| --- | --- | --- |
| Release | 2/2 通过 | 19.50 s |
| Debug + ASan/UBSan | 2/2 通过 | 21.27 s |

原始日志：[Release](results/release-checks.txt)、[Release 详情](results/release-detail.txt)、
[ASan/UBSan](results/asan-checks.txt)、[ASan/UBSan 详情](results/asan-detail.txt)。

## unit_tests

使用实际编码器、状态机、存储器和 Node::peer_request：
- 整数/含 NUL 字符串编码，截断与越界长度拒绝，严格 UTF-8 校验。
- 投票按最后日志任期、再按序号比较。
- 覆盖更新、删除、重复请求不重新执行、序号重用/跳号/过期处理。
- KV 1 MiB 和会话 1024 个的容量上限。
- 快照保存去重信息。
- 数据目录独占、节点编号/地址配置不符拒绝启动。
- 持久化往返、校验失败拒绝恢复。
- fork + _exit(99) 注入 rename 前后崩溃，验证恢复旧版本或已发布新版本。
- RLIMIT_FSIZE 造成临时文件分次写入失败，已发布旧版本保留。
- 空心跳不能提交未匹配的旧尾部。
- 日志冲突修复并持久化，禁止覆盖已提交记录。
- InstallSnapshot 安装与重启后的状态完整性。

## tests/integration.py

在独立 ext4 临时目录启动三个真实进程，使用真实 TCP：
- 自动选举、客户端重定向、数据目录锁。
- PUT / GET / DELETE、覆盖、空格与中文、半开区间 SCAN。
- 会话去重和恢复，重试旧 PUT 不会复活已删除的数据。
- 分片发送帧头/帧体，超大帧断连，节点继续服务。
- 三个并发写客户端。
- 两个并发客户端对同一 key 读/写/删除的 8 操作历史：
  穷举符合真实时间先后关系的串行执行，检查该小规模历史可线性化。
- 停掉 follower，越过快照阈值，再启动后通过 InstallSnapshot 追赶。
- SIGKILL Leader，另外两个节点重新选举并读取已确认数据。
- SIGSTOP/SIGCONT 模拟旧 Leader 暂停后重新加入。
- 杀掉另外两个节点，少数派的新写入及强一致读取均无法确认。
- 带有未提交尾部的旧 Leader 停机后，新多数派写入冲突位置；旧节点重启修复。
- 全部进程 SIGKILL 后重启，检查确认值、删除和去重结果。

测试失败时打印节点日志。可用 RAFTKV_KEEP_TEST_DATA=1 保留临时目录，便于排查。
测试只管理自己创建的子进程与临时目录。

## 证据边界

- 小规模历史检查不是对任意执行的形式化证明。
- SIGSTOP 是暂停实验，不是完整的任意丢包、乱序、单向网络分区模型。
- 未运行 Jepsen、TLA+ 模型检查、ThreadSanitizer 或真实断电实验。
- 没有测试动态成员变更（未实现），没有跨机器部署或长期运行测试。
- GitHub Actions 配置已提供；本地通过不等于远程 CI 已通过。
