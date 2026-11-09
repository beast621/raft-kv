#pragma once
#include <cstdint>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
namespace rkv {
using u64 = std::uint64_t;
constexpr std::size_t MAX_FRAME = 16 * 1024 * 1024;
constexpr std::size_t MAX_STATE = 64 * 1024 * 1024;
struct ProtocolError : std::runtime_error { using runtime_error::runtime_error; };
// 固定大端编码，不直接发送结构体。
struct Writer {
    std::string data;
    void number(u64 n) {
        for(int i = 7; i >= 0; --i) data.push_back(static_cast<char>(n >> (i * 8)));
    }
    void text(std::string_view s) { number(s.size()); data.append(s); }
};
struct Reader {
    std::string_view data;
    std::size_t pos = 0;
    explicit Reader(std::string_view s) : data(s) {}
    u64 number() {
        if(data.size() - pos < 8) throw ProtocolError("truncated integer");
        u64 n = 0;
        for(int i = 0; i < 8; ++i) n = (n << 8) | static_cast<unsigned char>(data[pos++]);
        return n;
    }
    std::string text(std::size_t limit = MAX_FRAME) {
        u64 size = number();
        if(size > limit || size > data.size() - pos) throw ProtocolError("invalid string length");
        std::string out(data.substr(pos, static_cast<std::size_t>(size)));
        pos += static_cast<std::size_t>(size); return out;
    }
    bool flag() {
        u64 n = number();
        if(n > 1) throw ProtocolError("invalid boolean");
        return n == 1;
    }
    void end() const { if(pos != data.size()) throw ProtocolError("trailing bytes"); }
};
// 完整性检测，不提供密码学认证。
inline u64 checksum(std::string_view s) {
    u64 hash = 14695981039346656037ULL;
    for(unsigned char c : s) { hash ^= c; hash *= 1099511628211ULL; }
    return hash;
}
inline std::string json(std::string_view s) {
    const char* hex = "0123456789abcdef";
    std::string out = "\"";
    for(unsigned char c : s) {
        if(c == '"' || c == '\\') { out += '\\'; out += static_cast<char>(c); }
        else if(c < 32) { out += "\\u00"; out += hex[c >> 4]; out += hex[c & 15]; }
        else out += static_cast<char>(c);
    }
    return out + "\"";
}
// 严格 UTF-8 校验：拒绝截断、过长编码、代理区和超出 Unicode 范围的值。
inline bool utf8(std::string_view text) {
    std::size_t i = 0;
    while(i < text.size()) {
        unsigned char first = text[i++];
        if(first < 128) continue;
        int count; std::uint32_t point;
        if(first >= 0xc2 && first <= 0xdf) { count = 1; point = first & 31; }
        else if(first >= 0xe0 && first <= 0xef) { count = 2; point = first & 15; }
        else if(first >= 0xf0 && first <= 0xf4) { count = 3; point = first & 7; }
        else return false;
        if(text.size() - i < static_cast<std::size_t>(count)) return false;
        for(int n = 0; n < count; ++n) {
            unsigned char next = text[i++];
            if((next & 0xc0) != 0x80) return false;
            point = (point << 6) | (next & 63);
        }
        if((count == 1 && point < 0x80) || (count == 2 && point < 0x800) ||
           (count == 3 && point < 0x10000) || point > 0x10ffff ||
           (point >= 0xd800 && point <= 0xdfff)) return false;
    }
    return true;
}
inline u64 positive(std::string_view s) {
    if(s.empty()) throw ProtocolError("expected positive integer");
    u64 value = 0;
    for(char c : s) {
        if(c < '0' || c > '9' || value > (std::numeric_limits<u64>::max() - (c - '0')) / 10)
            throw ProtocolError("invalid integer");
        value = value * 10 + (c - '0');
    }
    if(value == 0) throw ProtocolError("integer must be positive");
    return value;
}
}
