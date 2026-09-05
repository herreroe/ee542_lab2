#ifndef ZAP_PROTOCOL_HPP
#define ZAP_PROTOCOL_HPP

#include <cstdint>

constexpr uint16_t SERVER_PORT = 8080;
constexpr size_t DATA_SIZE = 1400; // Fits safely under standard 1500 MTU without fragmentation
constexpr size_t FILENAME_SIZE = 256;

enum PacketType : uint8_t {
    PACKET_START = 1,
    PACKET_DATA  = 2,
    PACKET_END   = 3,
    PACKET_ACK   = 4,
    PACKET_NACK  = 5
};

struct Packet {
    uint8_t  type;
    uint32_t sequence;
    uint32_t data_length;
    char     data[DATA_SIZE];
};

#endif