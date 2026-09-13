#ifndef ZAP_PROTOCOL_HPP
#define ZAP_PROTOCOL_HPP
#include <cstdint>
#include <cstddef>
#include <cstdio>
#include <algorithm>
#include <sys/socket.h>
#include <netinet/in.h>

#ifndef IP_MTU
#define IP_MTU 14
#endif

constexpr uint16_t SERVER_PORT = 8080;
constexpr uint32_t MAX_DATA_SIZE = 8900;
constexpr uint32_t MIN_DATA_SIZE = 512;
constexpr uint32_t FILENAME_SIZE = 256;

// IPv4 (20) + UDP (8) header.
constexpr uint32_t IP_UDP_HEADER_OVERHEAD = 28;

// number of threads used on both client and server side
constexpr int NUM_THREADS = 2;

// Packet types
constexpr uint32_t PACKET_START = 1;
constexpr uint32_t PACKET_DATA  = 2;
constexpr uint32_t PACKET_END   = 3;
constexpr uint32_t PACKET_NACK  = 4;
constexpr uint32_t PACKET_COMPLETE = 5;

struct Packet
{
    uint32_t type;
    uint32_t sequence;
    uint32_t data_length;

    uint64_t file_size;
    uint32_t total_packets;
    uint32_t chunk_size;   // only meaningful on PACKET_START - negotiated payload bytes/DATA packet

    char data[MAX_DATA_SIZE];
};

constexpr size_t PACKET_HEADER_SIZE = offsetof(Packet, data);

inline size_t packet_wire_size(const Packet& p) {
    return PACKET_HEADER_SIZE + p.data_length;
}

inline uint32_t detect_payload_size(int sockfd) {
    int mtu = 0;
    socklen_t optlen = sizeof(mtu);

    if (getsockopt(sockfd, IPPROTO_IP, IP_MTU, &mtu, &optlen) < 0 || mtu <= 0) {
        perror("getsockopt(IP_MTU) failed, falling back to default payload size");
        return 1024;
    }

    uint32_t usable = static_cast<uint32_t>(mtu) > (IP_UDP_HEADER_OVERHEAD + PACKET_HEADER_SIZE)
                           ? static_cast<uint32_t>(mtu) - IP_UDP_HEADER_OVERHEAD - static_cast<uint32_t>(PACKET_HEADER_SIZE)
                           : MIN_DATA_SIZE;

    return std::min(std::max(usable, MIN_DATA_SIZE), MAX_DATA_SIZE);
}

#endif