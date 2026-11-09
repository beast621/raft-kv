#include "storage.hpp"
#include <cerrno>
#include <cstdlib>
#include <cstdio>
#include <filesystem>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
namespace rkv {
static void io_error(const char* action) { throw StorageError(std::string(action) + ": errno=" + std::to_string(errno)); }
static void sync_directory(const std::string& path) {
    int fd = open(path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if(fd < 0) io_error("open directory");
    int result = fsync(fd), saved = errno;
    close(fd);
    if(result < 0) { errno = saved; io_error("fsync directory"); }
}
void validate_command(const Command& c, bool internal) {
    if(c.op == Op::Barrier && internal) {
        if(!c.client.empty() || c.seq || !c.key.empty() || !c.value.empty()) throw ProtocolError("invalid barrier");
        return;
    }
    if(c.op != Op::Put && c.op != Op::Delete && (internal || (c.op != Op::Get && c.op != Op::Scan && c.op != Op::Status)))
        throw ProtocolError("invalid operation");
    if(c.op == Op::Status) return;
    if(c.key.empty() || c.key.size() > 256 || c.value.size() > 4096 || !utf8(c.key) || !utf8(c.value)) throw ProtocolError("key/value limit");
    if(c.op == Op::Scan && (c.value.empty() || c.value.size() > 256 || c.key > c.value)) throw ProtocolError("invalid scan boundaries");
    if((c.op == Op::Delete || c.op == Op::Get) && !c.value.empty()) throw ProtocolError("unexpected value");
    if(c.op == Op::Put || c.op == Op::Delete) {
        if(c.client.empty() || c.client.size() > 64 || c.seq == 0 ||
           c.seq == std::numeric_limits<u64>::max()) throw ProtocolError("invalid client identity");
        for(unsigned char ch : c.client)
            if(!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
                 (ch >= '0' && ch <= '9') || ch == '_' || ch == '-')) throw ProtocolError("invalid client identity");
    }
}
void encode(Writer& w, const Command& c) {
    w.number(static_cast<u64>(c.op)); w.text(c.client); w.number(c.seq); w.text(c.key); w.text(c.value);
}
Command decode_command(Reader& r) {
    Command c;
    c.op = static_cast<Op>(r.number()); c.client = r.text(64); c.seq = r.number();
    c.key = r.text(256); c.value = r.text(4096); return c;
}
static std::string signature(const Command& c) {
    Writer w; w.number(static_cast<u64>(c.op)); w.text(c.key); w.text(c.value); return w.data;
}
std::string Machine::apply(const Command& c) {
    if(c.op == Op::Barrier) return "OK";
    const auto sig = signature(c);
    auto session = sessions.find(c.client);
    if(session != sessions.end()) {
        if(c.seq == session->second.seq) return sig == session->second.signature ? session->second.result : "ID_REUSE";
        if(c.seq < session->second.seq) return "STALE_SEQUENCE";
        if(c.seq != session->second.seq + 1) return "SEQUENCE_GAP";
    } else {
        if(c.seq != 1) return "SEQUENCE_GAP";
        if(sessions.size() >= MAX_SESSIONS) return "LIMIT";
    }
    auto old = kv.find(c.key);
    std::string result = "OK";
    if(c.op == Op::Put) {
        auto new_bytes = bytes + c.key.size() + c.value.size();
        if(old != kv.end()) new_bytes -= old->first.size() + old->second.size();
        if(new_bytes > MAX_KV_BYTES) result = "LIMIT";
        else { kv[c.key] = c.value; bytes = new_bytes; }
    } else if(c.op == Op::Delete) {
        if(old == kv.end()) result = "NOT_FOUND";
        else { bytes -= old->first.size() + old->second.size(); kv.erase(old); }
    } else throw ProtocolError("non-write in state machine");
    sessions[c.client] = {c.seq, sig, result}; return result;
}
void encode(Writer& w, const Machine& machine) {
    w.number(machine.kv.size());
    for(const auto& [key, value] : machine.kv) { w.text(key); w.text(value); }
    w.number(machine.sessions.size());
    for(const auto& [client, session] : machine.sessions) {
        w.text(client); w.number(session.seq); w.text(session.signature); w.text(session.result);
    }
}
Machine decode_machine(Reader& r) {
    Machine machine;
    auto count = r.number();
    if(count > MAX_KV_BYTES) throw ProtocolError("too many keys");
    for(u64 i = 0; i < count; ++i) {
        auto key = r.text(256), value = r.text(4096);
        if(key.empty() || !utf8(key) || !utf8(value) || !machine.kv.emplace(key, value).second) throw ProtocolError("invalid snapshot key");
        machine.bytes += key.size() + value.size();
        if(machine.bytes > MAX_KV_BYTES) throw ProtocolError("snapshot too large");
    }
    count = r.number();
    if(count > MAX_SESSIONS) throw ProtocolError("too many sessions");
    for(u64 i = 0; i < count; ++i) {
        auto client = r.text(64);
        Session s; s.seq = r.number(); s.signature = r.text(8192); s.result = r.text(64);
        Reader sig(s.signature);
        Command c; c.op = static_cast<Op>(sig.number()); c.key = sig.text(256); c.value = sig.text(4096);
        c.client = client; c.seq = s.seq; sig.end(); validate_command(c, true);
        if(c.op == Op::Barrier || (s.result != "OK" && s.result != "NOT_FOUND" && s.result != "LIMIT") ||
           !machine.sessions.emplace(client, s).second) throw ProtocolError("invalid session");
    }
    return machine;
}
u64 Persistent::term_at(u64 index) const {
    if(index == snap_index) return snap_term;
    if(index < snap_index || index > last()) throw ProtocolError("log index outside retained range");
    return log.at(static_cast<std::size_t>(index - snap_index - 1)).term;
}
Store::Store(std::string path, int node_id, std::string cluster)
    : directory(std::move(path)), config(std::move(cluster)), id(node_id) {
    std::filesystem::create_directories(directory);
    for(auto p = std::filesystem::absolute(directory); !p.empty();) {
        sync_directory(p.string());
        auto parent = p.parent_path(); if(parent == p) break; p = parent;
    }
    lock_fd = open((directory + "/LOCK").c_str(), O_RDWR | O_CREAT | O_CLOEXEC, 0600);
    if(lock_fd < 0) io_error("open LOCK");
    if(flock(lock_fd, LOCK_EX | LOCK_NB) < 0) {
        close(lock_fd); lock_fd = -1; throw StorageError("data directory is already locked");
    }
}
Store::~Store() { if(lock_fd >= 0) close(lock_fd); }
Persistent Store::load() {
    int fd = open((directory + "/state.bin").c_str(), O_RDONLY | O_CLOEXEC);
    if(fd < 0) {
        if(errno != ENOENT) io_error("open state");
        Persistent empty; save(empty); return empty;
    }
    std::string bytes; char buffer[16384];
    while(true) {
        ssize_t n = read(fd, buffer, sizeof(buffer));
        if(n < 0 && errno == EINTR) continue;
        if(n < 0) { int saved = errno; close(fd); errno = saved; io_error("read state"); }
        if(n == 0) break;
        bytes.append(buffer, static_cast<std::size_t>(n));
        if(bytes.size() > MAX_STATE) { close(fd); throw StorageError("state exceeds size limit"); }
    }
    if(close(fd) < 0) io_error("close state");
    try {
        Reader file(bytes);
        if(file.text(16) != "RAFTKV02") throw ProtocolError("state format mismatch");
        u64 expected = file.number(); auto body = file.text(MAX_STATE); file.end();
        if(checksum(body) != expected) throw ProtocolError("state checksum mismatch");
        Reader r(body);
        if(r.number() != static_cast<u64>(id) || r.text(4096) != config) throw ProtocolError("node/config mismatch");
        Persistent s;
        s.term = r.number(); s.vote = r.number(); s.commit = r.number();
        s.snap_index = r.number(); s.snap_term = r.number(); s.snapshot = decode_machine(r);
        u64 count = r.number();
        if(count > MAX_LOG || count > std::numeric_limits<u64>::max() - s.snap_index) throw ProtocolError("log size limit");
        u64 previous = s.snap_term;
        for(u64 i = 0; i < count; ++i) {
            Entry e; e.term = r.number(); e.command = decode_command(r); validate_command(e.command, true);
            if(e.term == 0 || e.term < previous || e.term > s.term) throw ProtocolError("invalid log term");
            previous = e.term; s.log.push_back(std::move(e));
        }
        r.end();
        if(s.vote > 3 || (s.term == 0 && s.vote != 0) || s.snap_term > s.term ||
           ((s.snap_index == 0) != (s.snap_term == 0)) || s.commit < s.snap_index || s.commit > s.last())
            throw ProtocolError("invalid persistent state");
        return s;
    } catch(const ProtocolError& e) { throw StorageError(e.what()); }
}
void Store::save(const Persistent& s) {
    if(s.log.size() > MAX_LOG) throw StorageError("log capacity exceeded");
    Writer w;
    w.number(id); w.text(config); w.number(s.term); w.number(s.vote); w.number(s.commit);
    w.number(s.snap_index); w.number(s.snap_term); encode(w, s.snapshot); w.number(s.log.size());
    for(const auto& e : s.log) { w.number(e.term); encode(w, e.command); }
    Writer file; file.text("RAFTKV02"); file.number(checksum(w.data)); file.text(w.data);
    if(file.data.size() > MAX_STATE) throw StorageError("state capacity exceeded");
    const std::string temp = directory + "/state.tmp", final = directory + "/state.bin";
    int fd = open(temp.c_str(), O_WRONLY | O_CREAT | O_TRUNC | O_CLOEXEC, 0600);
    if(fd < 0) io_error("open temporary state");
    std::size_t offset = 0;
    while(offset < file.data.size()) {
        ssize_t n = write(fd, file.data.data() + offset, file.data.size() - offset);
        if(n < 0 && errno == EINTR) continue;
        if(n <= 0) { int saved = n == 0 ? EIO : errno; close(fd); errno = saved; io_error("write state"); }
        offset += static_cast<std::size_t>(n);
    }
    if(fsync(fd) < 0) { int saved = errno; close(fd); errno = saved; io_error("fsync state"); }
    if(close(fd) < 0) io_error("close temporary state");
    const char* fault = std::getenv("RAFTKV_FAILPOINT");
    if(fault && std::string(fault) == "before_rename") _exit(99);
    if(rename(temp.c_str(), final.c_str()) < 0) io_error("rename state");
    if(fault && std::string(fault) == "after_rename") _exit(99);
    sync_directory(directory);
}
}
