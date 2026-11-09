#include "raft.hpp"
#include <cerrno>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <sys/resource.h>
#include <sys/wait.h>
#include <unistd.h>

using namespace rkv;
static void require(bool ok, const char* message) {
    if(!ok) throw std::runtime_error(message);
}
template<class F> static void rejected(F f) {
    bool failed = false;
    try { f(); } catch(const std::exception&) { failed = true; }
    require(failed, "invalid input was accepted");
}
static Command put(std::string client, u64 seq, std::string key, std::string value) {
    return {Op::Put, std::move(client), std::move(key), std::move(value), seq};
}
int main() {
    try {
        Writer w; w.number(0xfedcba9876543210ULL); w.text(std::string("a\0b", 3));
        Reader r(w.data); require(r.number() == 0xfedcba9876543210ULL, "integer codec");
        require(r.text() == std::string("a\0b", 3), "string codec"); r.end();
        rejected([] { Reader r("short"); r.number(); });
        Writer invalid; invalid.number(MAX_FRAME + 1);
        rejected([&] { Reader r(invalid.data); r.text(); });
        require(up_to_date(1, 3, 100, 2), "last term precedes index");
        require(!up_to_date(99, 2, 100, 2), "shorter equal-term log");
        require(!up_to_date(1000, 1, 2, 2), "longer stale-term log");

        Machine machine;
        require(machine.apply(put("alice", 1, "a", "one")) == "OK", "put");
        require(machine.apply(put("bob", 1, "a", "two")) == "OK", "overwrite");
        require(machine.apply(put("alice", 1, "a", "one")) == "OK", "duplicate retry");
        require(machine.kv.at("a") == "two", "duplicate must not reapply old put");
        require(machine.apply(put("alice", 1, "a", "changed")) == "ID_REUSE", "identity mismatch");
        require(machine.apply(put("alice", 3, "a", "three")) == "SEQUENCE_GAP", "sequence gap");
        Command del{Op::Delete, "alice", "a", "", 2};
        require(machine.apply(del) == "OK" && machine.kv.empty(), "delete");
        require(machine.apply(put("alice", 1, "a", "one")) == "STALE_SEQUENCE", "stale sequence");
        Writer snap; encode(snap, machine); Reader sr(snap.data); auto recovered = decode_machine(sr); sr.end();
        require(recovered.kv.empty() && recovered.sessions.at("alice").seq == 2, "session snapshot");
        rejected([] { auto c = put("x", 1, std::string(257, 'x'), "v"); validate_command(c, true); });
        rejected([] { auto c = put("x", 1, "k", std::string(4097, 'v')); validate_command(c, true); });


        Machine capacity;
        for(u64 seq = 1; seq <= 256; ++seq) {
            auto result = capacity.apply(put("capacity", seq, std::to_string(seq), std::string(4096, 'v')));
            if(seq < 256) require(result == "OK", "capacity setup");
        }
        require(capacity.bytes <= MAX_KV_BYTES, "KV byte cap");
        require(capacity.apply(put("capacity", 257, "overflow", std::string(4096, 'v'))) == "LIMIT", "KV capacity enforcement");
        Machine sessions;
        for(u64 i = 0; i < MAX_SESSIONS; ++i)
            require(sessions.apply(put("s" + std::to_string(i), 1, "k", "v")) == "OK", "session setup");
        require(sessions.apply(put("extra", 1, "k", "v")) == "LIMIT", "session capacity enforcement");
        std::string pattern = (std::filesystem::temp_directory_path() / "raftkv-unit-XXXXXX").string();
        char* name = mkdtemp(pattern.data());
        require(name != nullptr, "mkdtemp");
        const std::string root(name);
        {
            Store store(root, 1, "test-config");
            auto state = store.load();
            state.term = 7; state.vote = 2; state.log.push_back({7, put("c", 1, "hello", "world")});
            state.commit = 1; store.save(state);
            auto loaded = store.load();
            require(loaded.term == 7 && loaded.vote == 2 && loaded.commit == 1 &&
                    loaded.log[0].command.value == "world", "durable round trip");
            rejected([&] { Store duplicate(root, 1, "test-config"); });
        }
        rejected([&] { Store wrong(root, 2, "test-config"); wrong.load(); });
        rejected([&] { Store wrong(root, 1, "other-config"); wrong.load(); });
        for(const auto* point : {"before_rename", "after_rename"}) {
            pid_t pid = fork(); require(pid >= 0, "fork");
            if(pid == 0) {
                try {
                    Store store(root, 1, "test-config"); auto state = store.load();
                    state.term = 8; state.vote = 0;
                    setenv("RAFTKV_FAILPOINT", point, 1); store.save(state); _exit(1);
                } catch(...) { _exit(3); }
            }
            int status; require(waitpid(pid, &status, 0) == pid && WIFEXITED(status) && WEXITSTATUS(status) == 99, "fault exit");
            Store store(root, 1, "test-config");
            require(store.load().term == (std::string(point) == "before_rename" ? 7 : 8), "publication recovery");
        }
        // 限制临时文件大小，确认分次写入失败会向上报告，不覆盖旧文件。
        pid_t pid = fork(); require(pid >= 0, "fork size limit");
        if(pid == 0) {
            signal(SIGXFSZ, SIG_IGN);
            try {
                Store store(root, 1, "test-config"); auto state = store.load();
                rlimit limit{32, 32}; setrlimit(RLIMIT_FSIZE, &limit); state.term = 9;
                try { store.save(state); _exit(1); } catch(const StorageError&) { _exit(0); }
            } catch(...) { _exit(2); }
        }
        int status; waitpid(pid, &status, 0);
        require(WIFEXITED(status) && WEXITSTATUS(status) == 0, "write failure propagation");
        {
            Store store(root, 1, "test-config"); require(store.load().term == 8, "failed save must retain published state");
        }
        {
            std::fstream file(root + "/state.bin", std::ios::in | std::ios::out | std::ios::binary);
            file.seekp(-1, std::ios::end); file.put('X');
        }
        rejected([&] { Store store(root, 1, "test-config"); store.load(); });

        // 直接驱动真实 RPC 处理器，验证日志冲突修复及心跳不能提交未匹配尾部。
        Config cfg;
        cfg.id = 1; cfg.directory = root + "/rpc"; cfg.snapshot_every = 64;
        for(int i = 0; i < 3; ++i) {
            cfg.peers[i] = endpoint("127.0.0.1:" + std::to_string(30001 + i));
            cfg.clients[i] = endpoint("127.0.0.1:" + std::to_string(31001 + i));
        }
        {
            Store store(cfg.directory, 1, cfg.identity());
            Persistent s; s.term = 3;
            s.log = {{1, put("base", 1, "a", "old")}, {2, put("stale", 1, "b", "stale")}};
            store.save(s);
        }
        auto header = [&](u64 kind, u64 term) {
            Writer w; w.number(kind); w.text(cfg.identity()); w.number(2); w.number(term); return w;
        };
        {
            Node node(cfg);
            Writer hb = header(2, 4); hb.number(0); hb.number(0); hb.number(2); hb.number(0);
            auto response = node.peer_request(hb.data);
            Reader r(response); require(r.number() == 4 && r.flag(), "heartbeat acceptance");
            auto status = node.client_request(Command{Op::Status, "", "", "", 0});
            require(status.body.find("\"commit\":0") != std::string::npos, "unmatched tail must not commit");
            Writer append = header(2, 4);
            append.number(1); append.number(1); append.number(2); append.number(1);
            append.number(4); encode(append, put("fresh", 1, "b", "new"));
            response = node.peer_request(append.data);
            Reader ar(response); require(ar.number() == 4 && ar.flag(), "append repair");
            Writer vote = header(1, 5); vote.number(100); vote.number(2);
            response = node.peer_request(vote.data);
            Reader vr(response); require(vr.number() == 5 && !vr.flag(), "deny stale last term despite length");
        }
        {
            Store store(cfg.directory, 1, cfg.identity()); auto s = store.load();
            require(s.term == 5 && s.vote == 0 && s.commit == 2 && s.log[1].term == 4, "repair persisted");
            Machine m = s.snapshot;
            for(u64 i = s.snap_index; i < s.commit; ++i) m.apply(s.log.at(i - s.snap_index).command);
            require(m.kv.at("b") == "new", "old tail removed");
        }
        {
            Node node(cfg);
            Writer bad = header(2, 6); bad.number(0); bad.number(0); bad.number(2); bad.number(1);
            bad.number(6); encode(bad, put("bad", 1, "a", "bad"));
            rejected([&] { node.peer_request(bad.data); });
            Machine snap_machine; snap_machine.apply(put("snap", 1, "z", "snapshot"));
            Writer snap_rpc = header(3, 7); snap_rpc.number(10); snap_rpc.number(6); encode(snap_rpc, snap_machine);
            auto response = node.peer_request(snap_rpc.data);
            Reader sr(response); require(sr.number() == 7 && sr.flag(), "install snapshot");
        }
        {
            Store store(cfg.directory, 1, cfg.identity()); auto s = store.load();
            require(s.snap_index == 10 && s.commit == 10 && s.log.empty() &&
                    s.snapshot.kv.at("z") == "snapshot", "snapshot installation persisted");
        }
        require(utf8("中文") && !utf8(std::string("\xc0\xaf", 2)) &&
                !utf8(std::string("\xed\xa0\x80", 3)), "UTF-8 validation");
        std::filesystem::remove_all(root);
        std::cout << "PASS codec, vote freshness, dedup, deletion, snapshots, locking, corruption, crash publication, write failure\n";
        return 0;
    } catch(const std::exception& e) { std::cerr << "FAIL " << e.what() << '\n'; return 1; }
}
