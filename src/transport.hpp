#pragma once
#include "codec.hpp"
#include <chrono>
namespace rkv {
using Clock = std::chrono::steady_clock;
using Deadline = Clock::time_point;
struct NetworkError : std::runtime_error { using runtime_error::runtime_error; };
struct Endpoint {
    std::string host; int port;
    std::string str() const { return host + ":" + std::to_string(port); }
};
Endpoint endpoint(const std::string&);
int listener(const Endpoint&);
std::string receive_frame(int fd, Deadline);
void send_frame(int fd, std::string_view, Deadline);
std::string rpc(const Endpoint&, std::string_view, int timeout_ms);
}
