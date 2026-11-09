#include "raft.hpp"
#include <chrono>
#include <iostream>
#include <map>
#include <sstream>
#include <unistd.h>

namespace rkv {
static void signal_handler(int) { interrupted = 1; }
static std::array<Endpoint, 3> endpoints(const std::string& value) {
    std::array<Endpoint, 3> out;
    std::istringstream input(value); std::string item;
    for(int i = 0; i < 3; ++i) {
        if(!std::getline(input, item, ',')) throw ProtocolError("expected exactly three endpoints");
        out[i] = endpoint(item);
    }
    if(std::getline(input, item, ',')) throw ProtocolError("expected exactly three endpoints");
    return out;
}
static void usage() {
    std::cerr <<
        "raftkv node --id N --dir PATH [--peers IP:PORT,IP:PORT,IP:PORT]\n"
        "            [--clients IP:PORT,IP:PORT,IP:PORT] [--snapshot-every N]\n"
        "raftkv client [--servers IP:PORT,IP:PORT,IP:PORT] [--client-id ID]\n"
        "              [--seq N] [--once] put KEY VALUE|get KEY|delete KEY|scan START END|status\n";
}
static int node_main(int argc, char** argv) {
    Config c;
    c.peers = endpoints("127.0.0.1:9101,127.0.0.1:9102,127.0.0.1:9103");
    c.clients = endpoints("127.0.0.1:9201,127.0.0.1:9202,127.0.0.1:9203");
    std::map<std::string, std::string> options;
    for(int i = 2; i < argc; i += 2) {
        if(i + 1 >= argc || !options.emplace(argv[i], argv[i + 1]).second) throw ProtocolError("invalid/duplicate option");
    }
    if(!options.count("--id") || !options.count("--dir")) throw ProtocolError("--id and --dir are required");
    for(const auto& [key, value] : options) {
        if(key == "--id") { auto id = positive(value); if(id > 3) throw ProtocolError("id must be 1..3"); c.id = static_cast<int>(id); }
        else if(key == "--dir") c.directory = value;
        else if(key == "--peers") c.peers = endpoints(value);
        else if(key == "--clients") c.clients = endpoints(value);
        else if(key == "--snapshot-every") c.snapshot_every = positive(value);
        else throw ProtocolError("unknown option: " + key);
    }
    c.validate();
    std::signal(SIGINT, signal_handler); std::signal(SIGTERM, signal_handler);
    Node node(std::move(c)); node.run(); return 0;
}
static int client_main(int argc, char** argv) {
    auto servers = endpoints("127.0.0.1:9201,127.0.0.1:9202,127.0.0.1:9203");
    Command c;
    c.client = "cli-" + std::to_string(getpid()) + "-" +
        std::to_string(Clock::now().time_since_epoch().count());
    c.seq = 1;
    int i = 2; bool once = false;
    while(i < argc && std::string(argv[i]).starts_with("--")) {
        std::string option = argv[i++];
        if(option == "--once") { once = true; continue; }
        if(i >= argc) throw ProtocolError("missing option value");
        std::string value = argv[i++];
        if(option == "--servers") servers = endpoints(value);
        else if(option == "--client-id") c.client = value;
        else if(option == "--seq") c.seq = positive(value);
        else throw ProtocolError("unknown option: " + option);
    }
    if(i >= argc) throw ProtocolError("missing command");
    std::string op = argv[i++];
    if(op == "put" && argc - i == 2) { c.op = Op::Put; c.key = argv[i]; c.value = argv[i + 1]; }
    else if(op == "get" && argc - i == 1) { c.op = Op::Get; c.key = argv[i]; }
    else if(op == "delete" && argc - i == 1) { c.op = Op::Delete; c.key = argv[i]; }
    else if(op == "scan" && argc - i == 2) { c.op = Op::Scan; c.key = argv[i]; c.value = argv[i + 1]; }
    else if(op == "status" && argc - i == 0) c.op = Op::Status;
    else throw ProtocolError("invalid command arguments");
    validate_command(c, false);
    Writer request; encode(request, c);
    Endpoint target = servers[0];
    ClientReply last{"UNAVAILABLE", "", "{\"status\":\"UNAVAILABLE\"}"};
    for(int attempt = 0; attempt < (once ? 1 : 6); ++attempt) {
        try {
            auto bytes = rpc(target, request.data, 4200);
            Reader r(bytes); last = decode_reply(r);
            if(last.status != "NOT_LEADER" && last.status != "RETRY" && last.status != "BUSY") break;
            if(!last.leader.empty()) target = endpoint(last.leader);
            else target = servers[(attempt + 1) % 3];
        } catch(const NetworkError&) {
            target = servers[(attempt + 1) % 3];
        }
        if(!once) std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    std::cout << last.body << '\n';
    return last.status == "OK" || last.status == "NOT_FOUND" ? 0 : 2;
}
}
int main(int argc, char** argv) {
    try {
        if(argc < 2) { rkv::usage(); return 1; }
        if(std::string(argv[1]) == "node") return rkv::node_main(argc, argv);
        if(std::string(argv[1]) == "client") return rkv::client_main(argc, argv);
        rkv::usage(); return 1;
    } catch(const std::exception& e) {
        std::cerr << "error: " << e.what() << '\n'; return 1;
    }
}
