#include "transport.hpp"
#include <algorithm>
#include <arpa/inet.h>
#include <cerrno>
#include <climits>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
namespace rkv {
Endpoint endpoint(const std::string& text) {
    auto colon = text.find(':');
    if(colon == std::string::npos) throw ProtocolError("endpoint must be IPv4:port");
    Endpoint out{text.substr(0, colon), 0};
    auto port = positive(text.substr(colon + 1)); in_addr ip{};
    if(port > 65535 || inet_pton(AF_INET, out.host.c_str(), &ip) != 1) throw ProtocolError("invalid IPv4 endpoint");
    out.port = static_cast<int>(port); return out;
}
static sockaddr_in address(const Endpoint& e) {
    sockaddr_in a{}; a.sin_family = AF_INET; a.sin_port = htons(e.port);
    if(inet_pton(AF_INET, e.host.c_str(), &a.sin_addr) != 1) throw ProtocolError("invalid IPv4");
    return a;
}
static void wait_fd(int fd, short events, Deadline end) {
    while(true) {
        auto left = std::chrono::duration_cast<std::chrono::milliseconds>(end - Clock::now()).count();
        if(left <= 0) throw NetworkError("network deadline exceeded");
        pollfd p{fd, events, 0};
        int n = poll(&p, 1, static_cast<int>(std::min<long long>(left + 1, INT_MAX)));
        if(n < 0 && errno == EINTR) continue;
        if(n <= 0) throw NetworkError("poll timeout/error");
        if(p.revents & events) return;
        throw NetworkError("connection closed");
    }
}
// 所有套接字非阻塞，整个帧共享一个截止时间。
static void transfer(int fd, char* data, std::size_t size, bool writing, Deadline end) {
    std::size_t offset = 0;
    while(offset < size) {
        if(Clock::now() >= end) throw NetworkError("network deadline exceeded");
        ssize_t n = writing ? send(fd, data + offset, size - offset, MSG_NOSIGNAL) : recv(fd, data + offset, size - offset, 0);
        if(n < 0 && errno == EINTR) continue;
        if(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) { wait_fd(fd, writing ? POLLOUT : POLLIN, end); continue; }
        if(n <= 0) throw NetworkError("incomplete frame / disconnected");
        offset += static_cast<std::size_t>(n);
    }
}
std::string receive_frame(int fd, Deadline end) {
    std::uint32_t size = 0;
    transfer(fd, reinterpret_cast<char*>(&size), sizeof(size), false, end); size = ntohl(size);
    if(size == 0 || size > MAX_FRAME) throw ProtocolError("frame length limit");
    std::string bytes(size, '\0'); transfer(fd, bytes.data(), bytes.size(), false, end); return bytes;
}
void send_frame(int fd, std::string_view bytes, Deadline end) {
    if(bytes.empty() || bytes.size() > MAX_FRAME) throw ProtocolError("frame length limit");
    std::uint32_t size = htonl(static_cast<std::uint32_t>(bytes.size()));
    transfer(fd, reinterpret_cast<char*>(&size), sizeof(size), true, end);
    transfer(fd, const_cast<char*>(bytes.data()), bytes.size(), true, end);
}
int listener(const Endpoint& e) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if(fd < 0) throw NetworkError("socket failed");
    int one = 1; auto a = address(e);
    if(setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0 ||
       bind(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) < 0 || listen(fd, 64) < 0) {
        close(fd); throw NetworkError("bind/listen failed at " + e.str());
    }
    return fd;
}
std::string rpc(const Endpoint& e, std::string_view request, int timeout_ms) {
    int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0);
    if(fd < 0) throw NetworkError("socket failed");
    try {
        Deadline end = Clock::now() + std::chrono::milliseconds(timeout_ms); auto a = address(e);
        if(connect(fd, reinterpret_cast<sockaddr*>(&a), sizeof(a)) < 0) {
            if(errno != EINPROGRESS) throw NetworkError("connect failed");
            wait_fd(fd, POLLOUT, end);
            int error = 0; socklen_t length = sizeof(error);
            if(getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &length) < 0 || error) throw NetworkError("connect failed");
        }
        send_frame(fd, request, end); auto response = receive_frame(fd, end); close(fd); return response;
    } catch(...) { close(fd); throw; }
}
}
