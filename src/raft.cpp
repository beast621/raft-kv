#include "raft.hpp"
#include <algorithm>
#include <cerrno>
#include <deque>
#include <functional>
#include <iostream>
#include <poll.h>
#include <set>
#include <sys/socket.h>
#include <unistd.h>

namespace rkv {
volatile std::sig_atomic_t interrupted = 0;
using namespace std::chrono_literals;

std::string Config::identity() const {
    std::string result = "fixed-three-v1";
    for(const auto& p : peers) result += "|" + p.str();
    for(const auto& p : clients) result += "|" + p.str();
    return result;
}
void Config::validate() const {
    if(id < 1 || id > 3 || directory.empty() || snapshot_every < 4 || snapshot_every > 1024)
        throw ProtocolError("invalid node configuration");
    std::set<std::string> addresses;
    for(const auto& p : peers) if(!addresses.insert(p.str()).second) throw ProtocolError("duplicate endpoint");
    for(const auto& p : clients) if(!addresses.insert(p.str()).second) throw ProtocolError("duplicate endpoint");
}
bool up_to_date(u64 ci, u64 ct, u64 li, u64 lt) { return ct > lt || (ct == lt && ci >= li); }
void encode(Writer& w, const ClientReply& r) { w.text(r.status); w.text(r.leader); w.text(r.body); }
ClientReply decode_reply(Reader& r) {
    ClientReply out{r.text(64), r.text(128), r.text()}; r.end(); return out;
}
static Config checked(Config config) { config.validate(); return config; }
static const char* role_name(Role role) {
    if(role == Role::Leader) return "Leader";
    if(role == Role::Candidate) return "Candidate";
    return "Follower";
}
Node::Node(Config cfg)
    : config(checked(std::move(cfg))), store(config.directory, config.id, config.identity()),
      state(store.load()), machine(state.snapshot), applied(state.snap_index),
      random(std::random_device{}()) {
    config.validate();
    apply_committed();
    reset_election();
    replication_due = Clock::now();
}
void Node::reset_election() {
    election_due = Clock::now() + std::chrono::milliseconds(std::uniform_int_distribution<int>(900, 1500)(random));
}
// 先持久化新任期，才允许回复或继续参与协议。
void Node::observe_term(u64 term) {
    if(term <= state.term) return;
    state.term = term; state.vote = 0; role = Role::Follower; leader_id = 0;
    reset_election(); store.save(state); changed.notify_all();
}
Writer Node::header(u64 kind) const {
    Writer w; w.number(kind); w.text(config.identity()); w.number(config.id); w.number(state.term); return w;
}
// 重启时从快照恢复，再顺序执行已持久化的 commit 范围。
void Node::apply_committed() {
    while(applied < state.commit) {
        ++applied;
        auto result = machine.apply(state.log.at(static_cast<std::size_t>(applied - state.snap_index - 1)).command);
        auto it = waiting.find({state.term_at(applied), applied});
        if(it != waiting.end()) it->second = std::move(result);
    }
    compact();
    changed.notify_all();
}
// 快照和剩余日志在同一个原子发布文件里，避免跨文件切换窗口。
void Node::compact() {
    if(applied - state.snap_index < config.snapshot_every) return;
    const auto term = state.term_at(applied);
    state.log.erase(state.log.begin(), state.log.begin() + static_cast<std::ptrdiff_t>(applied - state.snap_index));
    state.snap_index = applied; state.snap_term = term; state.snapshot = machine;
    store.save(state);
}
void Node::advance_commit() {
    for(u64 index = state.last(); index > state.commit; --index) {
        if(state.term_at(index) != state.term) continue;
        int votes = 1;
        for(int i = 0; i < 3; ++i) if(i != config.id - 1 && match[i] >= index) ++votes;
        if(votes >= 2) {
            state.commit = index; store.save(state); apply_committed(); return;
        }
    }
}
ClientReply Node::reply(std::string status, std::string fields) const {
    std::string leader = leader_id >= 1 && leader_id <= 3 ? config.clients[leader_id - 1].str() : "";
    auto body = "{\"status\":" + json(status) + ",\"leader\":" + json(leader) + fields + "}";
    return {std::move(status), std::move(leader), std::move(body)};
}

// 所有请求先完整解码，再修改协议状态。
std::string Node::peer_request(std::string_view bytes) {
    Reader r(bytes);
    u64 kind = r.number();
    if(r.text(4096) != config.identity()) throw ProtocolError("cluster mismatch");
    u64 sender = r.number(), term = r.number();
    if(sender < 1 || sender > 3 || sender == static_cast<u64>(config.id) || term == 0)
        throw ProtocolError("invalid peer");
    u64 index = 0, log_term = 0, leader_commit = 0;
    std::vector<Entry> entries;
    Machine snapshot;
    if(kind == 1) { index = r.number(); log_term = r.number(); }
    else if(kind == 2) {
        index = r.number(); log_term = r.number(); leader_commit = r.number();
        u64 count = r.number();
        if(count > 16 || index > std::numeric_limits<u64>::max() - count) throw ProtocolError("append limit");
        u64 previous = log_term;
        for(u64 i = 0; i < count; ++i) {
            Entry e; e.term = r.number(); e.command = decode_command(r); validate_command(e.command, true);
            if(e.term == 0 || e.term < previous || e.term > term) throw ProtocolError("invalid entry term");
            previous = e.term; entries.push_back(std::move(e));
        }
    } else if(kind == 3) {
        index = r.number(); log_term = r.number(); snapshot = decode_machine(r);
        if(index == 0) throw ProtocolError("empty snapshot");
    } else throw ProtocolError("unknown peer request");
    r.end();
    if(log_term > term || ((index == 0) != (log_term == 0))) throw ProtocolError("invalid last/previous term");

    std::lock_guard guard(mutex);
    if(stopping) throw NetworkError("node stopped");
    observe_term(term);
    auto result = [&](bool ok, u64 hint) {
        Writer w; w.number(state.term); w.number(ok); w.number(hint); return w.data;
    };
    if(term < state.term) return result(false, state.last() + 1);

    if(kind == 1) {
        if(!up_to_date(index, log_term, state.last(), state.term_at(state.last())) ||
           (state.vote != 0 && state.vote != sender)) return result(false, 0);
        if(state.vote == 0) { state.vote = sender; store.save(state); }
        reset_election(); return result(true, 0);
    }

    role = Role::Follower; leader_id = static_cast<int>(sender); reset_election(); changed.notify_all();
    if(kind == 3) {
        if(index <= state.commit) return result(true, index);
        if(index <= state.last() && index >= state.snap_index && state.term_at(index) == log_term)
            state.log.erase(state.log.begin(), state.log.begin() + static_cast<std::ptrdiff_t>(index - state.snap_index));
        else state.log.clear();
        state.snap_index = index; state.snap_term = log_term; state.commit = index;
        state.snapshot = std::move(snapshot); machine = state.snapshot; applied = index;
        store.save(state); changed.notify_all();
        return result(true, index);
    }

    if(index < state.snap_index) return result(false, state.snap_index + 1);
    if(index > state.last()) return result(false, state.last() + 1);
    if(state.term_at(index) != log_term) {
        u64 hint = index, conflict = state.term_at(index);
        while(hint > state.snap_index && state.term_at(hint - 1) == conflict) --hint;
        return result(false, std::max<u64>(1, hint));
    }
    bool dirty = false;
    for(std::size_t i = 0; i < entries.size(); ++i) {
        u64 at = index + i + 1;
        if(at <= state.last()) {
            const auto& old = state.log.at(static_cast<std::size_t>(at - state.snap_index - 1));
            if(old.term == entries[i].term) {
                Writer a, b; encode(a, old.command); encode(b, entries[i].command);
                if(a.data != b.data) throw ProtocolError("same-term command mismatch");
                continue;
            }
            if(at <= state.commit) throw ProtocolError("attempt to overwrite committed entry");
            state.log.resize(static_cast<std::size_t>(at - state.snap_index - 1)); dirty = true;
        }
        if(state.log.size() >= MAX_LOG) throw StorageError("log capacity exceeded");
        state.log.push_back(entries[i]); dirty = true;
    }
    // 只提交本次已经匹配的范围，不能顺带提交尚未校验的旧尾部。
    u64 commit = std::min(leader_commit, index + entries.size());
    if(commit > state.commit) { state.commit = commit; dirty = true; }
    if(dirty) store.save(state);
    apply_committed();
    return result(true, index + entries.size());
}

// 先在锁内追加日志；等待复制提交时释放锁，让 RPC 和 ticker 可以继续运行。
ClientReply Node::client_request(const Command& command) {
    validate_command(command, false);
    std::unique_lock lock(mutex);
    if(command.op == Op::Status)
        return reply("OK", ",\"id\":" + std::to_string(config.id) + ",\"role\":" + json(role_name(role)) +
            ",\"term\":" + std::to_string(state.term) + ",\"commit\":" + std::to_string(state.commit) +
            ",\"applied\":" + std::to_string(applied) + ",\"snapshot\":" + std::to_string(state.snap_index) +
            ",\"last\":" + std::to_string(state.last()) + ",\"keys\":" + std::to_string(machine.kv.size()));
    if(stopping || role != Role::Leader) return reply("NOT_LEADER");
    if(state.log.size() >= 4096) return reply("BUSY");
    const bool write = command.op == Op::Put || command.op == Op::Delete;
    Command logged = write ? command : Command{};
    state.log.push_back({state.term, std::move(logged)});
    const u64 index = state.last(), term = state.term;
    waiting[{term, index}] = std::nullopt;
    store.save(state);
    replication_due = Clock::now(); changed.notify_all();
    bool done = changed.wait_for(lock, 3s, [&] {
        return stopping || role != Role::Leader || state.term != term || applied >= index;
    });
    auto outcome = waiting.at({term, index});
    waiting.erase({term, index});
    if(!done || stopping || role != Role::Leader || state.term != term || applied < index) return reply("RETRY");
    if(write) {
        if(!outcome) throw StorageError("committed result missing");
        return reply(*outcome);
    }
    if(command.op == Op::Get) {
        auto it = machine.kv.find(command.key);
        if(it == machine.kv.end()) return reply("NOT_FOUND");
        return reply("OK", ",\"value\":" + json(it->second));
    }
    std::string rows = ",\"rows\":[";
    bool first = true, more = false;
    std::size_t count = 0;
    for(auto it = machine.kv.lower_bound(command.key); it != machine.kv.end() && it->first < command.value; ++it) {
        auto row = "[" + json(it->first) + "," + json(it->second) + "]";
        if(count >= 100 || rows.size() + row.size() > 65536) { more = true; break; }
        if(!first) rows += ",";
        rows += row; first = false; ++count;
    }
    return reply("OK", rows + "],\"more\":" + (more ? "true" : "false"));
}

void Node::campaign() {
    u64 term, last, last_term;
    {
        std::lock_guard lock(mutex);
        if(stopping || role == Role::Leader || Clock::now() < election_due) return;
        if(state.term == std::numeric_limits<u64>::max()) throw StorageError("term exhausted");
        ++state.term; state.vote = config.id; role = Role::Candidate; leader_id = 0;
        reset_election(); store.save(state);
        term = state.term; last = state.last(); last_term = state.term_at(last);
    }
    int votes = 1;
    for(int peer = 0; peer < 3; ++peer) {
        if(peer == config.id - 1) continue;
        Writer request;
        {
            std::lock_guard lock(mutex);
            if(stopping || role != Role::Candidate || state.term != term) return;
            request = header(1); request.number(last); request.number(last_term);
        }
        try {
            auto response = rpc(config.peers[peer], request.data, 300);
            Reader r(response); u64 their_term = r.number(); bool granted = r.flag(); r.number(); r.end();
            std::lock_guard lock(mutex);
            observe_term(their_term);
            if(role != Role::Candidate || state.term != term) return;
            if(granted && their_term == term) ++votes;
            if(votes >= 2) {
                role = Role::Leader; leader_id = config.id;
                next.fill(state.last() + 1); match.fill(0);
                // 当前任期的空操作使旧任期条目能够按提交规则一起提交。
                state.log.push_back({state.term, Command{}});
                store.save(state);
                replication_due = Clock::now(); changed.notify_all();
                std::cerr << "LEADER id=" << config.id << " term=" << state.term << '\n';
                return;
            }
        } catch(const NetworkError&) { }
          catch(const ProtocolError&) { }
    }
}

void Node::replicate() {
    for(int peer = 0; peer < 3; ++peer) {
        if(peer == config.id - 1) continue;
        Writer request;
        u64 term = 0, matched = 0;
        {
            std::lock_guard lock(mutex);
            if(stopping || role != Role::Leader) return;
            term = state.term;
            if(next[peer] <= state.snap_index) {
                request = header(3); request.number(state.snap_index); request.number(state.snap_term);
                encode(request, state.snapshot); matched = state.snap_index;
            } else {
                next[peer] = std::min(next[peer], state.last() + 1);
                u64 previous = next[peer] - 1;
                u64 count = std::min<u64>(16, state.last() - previous);
                request = header(2); request.number(previous); request.number(state.term_at(previous));
                request.number(state.commit); request.number(count);
                for(u64 i = 0; i < count; ++i) {
                    const auto& entry = state.log.at(static_cast<std::size_t>(previous + i - state.snap_index));
                    request.number(entry.term); encode(request, entry.command);
                }
                matched = previous + count;
            }
        }
        try {
            auto response = rpc(config.peers[peer], request.data, 300);
            Reader r(response); u64 their_term = r.number(); bool ok = r.flag(); u64 hint = r.number(); r.end();
            std::lock_guard lock(mutex);
            observe_term(their_term);
            if(role != Role::Leader || state.term != term || their_term != term) continue;
            if(ok) { match[peer] = std::max(match[peer], matched); next[peer] = match[peer] + 1; advance_commit(); }
            else {
                u64 target = std::clamp<u64>(hint, 1, state.last() + 1);
                if(target == next[peer] && target > 1) --target;
                next[peer] = target;
            }
        } catch(const NetworkError&) { }
          catch(const ProtocolError&) { }
    }
}
void Node::ticker() {
    while(!stopping) {
        bool election = false, replication = false;
        {
            std::unique_lock lock(mutex);
            changed.wait_for(lock, 20ms, [&] { return stopping.load(); });
            if(stopping) break;
            election = role != Role::Leader && Clock::now() >= election_due;
            replication = role == Role::Leader && Clock::now() >= replication_due;
            if(replication) replication_due = Clock::now() + 100ms;
        }
        if(election) campaign();
        if(replication) replicate();
    }
}

// 客户端和 Raft RPC 使用不同的有界工作队列，避免等待提交的客户端占满 RPC 线程。
class Pool {
    std::mutex mutex;
    std::condition_variable ready;
    std::deque<int> queue;
    bool stopped = false;
    std::vector<std::thread> workers;
public:
    Pool(int count, std::function<void(int)> handler) {
        for(int i = 0; i < count; ++i) workers.emplace_back([this, handler] {
            while(true) {
                int fd;
                {
                    std::unique_lock lock(mutex);
                    ready.wait(lock, [&] { return stopped || !queue.empty(); });
                    if(stopped) return;
                    fd = queue.front(); queue.pop_front();
                }
                try { handler(fd); }
                catch(const StorageError& e) {
                    std::cerr << "FATAL durability failure: " << e.what() << '\n';
                    _exit(2);  // 不允许在持久化状态不确定后继续服务。
                }
                catch(const NetworkError&) { }
                catch(const ProtocolError&) { } // 非法帧或断连只关闭这条连接。
                catch(const std::exception& e) {
                    std::cerr << "FATAL invariant failure: " << e.what() << '\n';
                    _exit(2);
                }
                close(fd);
            }
        });
    }
    void submit(int fd) {
        std::lock_guard lock(mutex);
        if(stopped || queue.size() >= 64) { close(fd); return; }
        queue.push_back(fd); ready.notify_one();
    }
    ~Pool() {
        {
            std::lock_guard lock(mutex); stopped = true;
            for(int fd : queue) close(fd);
            queue.clear(); ready.notify_all();
        }
        for(auto& thread : workers) thread.join();
    }
};
void Node::run() {
    int peer_fd = listener(config.peers[config.id - 1]);
    int client_fd;
    try { client_fd = listener(config.clients[config.id - 1]); }
    catch(...) { close(peer_fd); throw; }
    std::thread timer([&] {
        try { ticker(); }
        catch(const std::exception& e) {
            std::cerr << "FATAL ticker: " << e.what() << '\n'; _exit(2);
        }
    });
    {
        Pool peers(2, [&](int fd) {
            auto request = receive_frame(fd, Clock::now() + 1000ms);
            auto response = peer_request(request);
            send_frame(fd, response, Clock::now() + 1000ms);
        });
        Pool clients(4, [&](int fd) {
            auto request = receive_frame(fd, Clock::now() + 1000ms);
            Reader r(request); Command c = decode_command(r); r.end();
            auto response = client_request(c);
            Writer w; encode(w, response); send_frame(fd, w.data, Clock::now() + 1000ms);
        });
        std::cerr << "READY id=" << config.id << " peer=" << config.peers[config.id - 1].str()
                  << " client=" << config.clients[config.id - 1].str() << '\n';
        while(!interrupted) {
            pollfd events[2]{{peer_fd, POLLIN, 0}, {client_fd, POLLIN, 0}};
            int count = poll(events, 2, 100);
            if(count < 0 && errno == EINTR) continue;
            if(count < 0) break;
            for(int i = 0; i < 2; ++i) if(events[i].revents & POLLIN) {
                // 每轮限制接入数量，避免持续连接淹没其他端口。
                for(int n = 0; n < 16; ++n) {
                    int fd = accept4(events[i].fd, nullptr, nullptr, SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if(fd < 0) break;
                    if(i == 0) peers.submit(fd); else clients.submit(fd);
                }
            }
        }
        { std::lock_guard lock(mutex); stopping = true; changed.notify_all(); }
    }
    timer.join(); close(peer_fd); close(client_fd);
}
}
