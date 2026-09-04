#ifndef ZAP_PROTOCOL_HPP
#define ZAP_PROTOCOL_HPP
#include <cstdint>

constexpr uint16_t SERVER_PORT = 8080;
constexpr uint32_t DATA_SIZE = 1024;
constexpr uint32_t FILENAME_SIZE = 256;

// number of threads used on both client and server side
constexpr int NUM_THREADS = 2;

// Packet types
constexpr uint32_t PACKET_START = 1;
constexpr uint32_t PACKET_DATA  = 2;
constexpr uint32_t PACKET_END   = 3;

struct Packet
{
    uint32_t type;
    uint32_t sequence;
    uint32_t data_length;
    char data[DATA_SIZE];
};

#endif