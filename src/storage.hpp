#pragma once
#include "codec.hpp"
#include <map>
#include <vector>
namespace rkv {
constexpr std::size_t MAX_KV_BYTES = 1024 * 1024;
constexpr std::size_t MAX_SESSIONS = 1024;
constexpr std::size_t MAX_LOG = 8192;
enum class Op : u64 { Barrier = 0, Put = 1, Delete = 2, Get = 3, Scan = 4, Status = 5 };
struct Command {
    Op op = Op::Barrier;
    std::string client, key, value;
    u64 seq = 0;
};
struct Session { u64 seq = 0; std::string signature, result; };
struct Machine {
    std::map<std::string, std::string> kv;
    std::map<std::string, Session> sessions;
    std::size_t bytes = 0;
    std::string apply(const Command&);
};
struct Entry { u64 term = 0; Command command; };
struct Persistent {
    u64 term = 0, vote = 0, commit = 0, snap_index = 0, snap_term = 0;
    Machine snapshot;
    std::vector<Entry> log;
    u64 last() const { return snap_index + log.size(); }
    u64 term_at(u64 index) const;
};
void encode(Writer&, const Command&);
Command decode_command(Reader&);
void encode(Writer&, const Machine&);
Machine decode_machine(Reader&);
void validate_command(const Command&, bool internal);
struct StorageError : std::runtime_error { using runtime_error::runtime_error; };
// 目录独占 LOCK；每次持久化都采用完整临时文件安全替换。
class Store {
    std::string directory, config;
    int lock_fd = -1;
    int id;
public:
    Store(std::string directory, int id, std::string config);
    ~Store();
    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;
    Persistent load();
    void save(const Persistent&);
};
}
