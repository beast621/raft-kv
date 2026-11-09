#pragma once
#include "storage.hpp"
#include "transport.hpp"
#include <array>
#include <atomic>
#include <condition_variable>
#include <csignal>
#include <mutex>
#include <optional>
#include <random>
#include <thread>

namespace rkv {
extern volatile std::sig_atomic_t interrupted;
struct Config {
    int id = 1;
    std::string directory;
    std::array<Endpoint, 3> peers, clients;
    u64 snapshot_every = 64;
    std::string identity() const;
    void validate() const;
};
enum class Role { Follower, Candidate, Leader };
bool up_to_date(u64 candidate_index, u64 candidate_term, u64 local_index, u64 local_term);
struct ClientReply {
    std::string status, leader, body;
};
void encode(Writer&, const ClientReply&);
ClientReply decode_reply(Reader&);

// Raft 状态与状态机由一把互斥锁保护；网络等待期间不持锁。
class Node {
    Config config;
    Store store;
    Persistent state;
    Machine machine;
    u64 applied = 0;
    Role role = Role::Follower;
    int leader_id = 0;
    std::array<u64, 3> next{}, match{};
    std::map<std::pair<u64, u64>, std::optional<std::string>> waiting;
    std::mutex mutex;
    std::condition_variable changed;
    std::atomic<bool> stopping{false};
    std::mt19937 random;
    Deadline election_due, replication_due;
    void reset_election();
    void observe_term(u64 term);
    void apply_committed();
    void compact();
    void advance_commit();
    void campaign();
    void replicate();
    void ticker();
    Writer header(u64 kind) const;
    ClientReply reply(std::string status, std::string fields = "") const;
public:
    explicit Node(Config);
    void run();
    std::string peer_request(std::string_view bytes);
    ClientReply client_request(const Command&);
};
}
