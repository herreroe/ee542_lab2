// Server side implementation of UDP client-server model - optimized but kept simple

#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#include <iostream>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <thread>
#include <vector>
#include <memory>
#include <atomic>
#include <chrono>
#include <new>
#include <algorithm>
#include <cerrno>

#include "zap_protocol.hpp"

struct ReceiverState {
    std::string outputFilename;

    std::unique_ptr<char[]> fileBuffer;
    std::unique_ptr<std::atomic<bool>[]> receivedFlags;
    std::atomic<uint32_t> receivedCount{0};

    uint64_t fileSize = 0;
    uint32_t totalPackets = 0;
    uint32_t chunkSize = 0;

    sockaddr_in clientAddress{};

    std::atomic<bool> stopRequested{false};
    std::atomic<long long> finalRecvTimeUs{0};
};

static long long current_time_us() {
    auto now = std::chrono::high_resolution_clock::now();
    return std::chrono::duration_cast<std::chrono::microseconds>(
        now.time_since_epoch()
    ).count();
}

static bool process_data_packet(ReceiverState& state, const Packet& packet) {
    if (packet.sequence >= state.totalPackets) {
        return false;
    }

    if (packet.data_length > state.chunkSize) {
        return false;
    }

    uint64_t offset = static_cast<uint64_t>(packet.sequence) * state.chunkSize;

    if (offset + packet.data_length > state.fileSize) {
        return false;
    }

    bool expected = false;

    // Only one thread is allowed to process this sequence number.
    if (!state.receivedFlags[packet.sequence].compare_exchange_strong(
            expected,
            true,
            std::memory_order_relaxed)) {
        return true; // duplicate packet
    }

    std::memcpy( state.fileBuffer.get() + offset, packet.data, packet.data_length);
    uint32_t received = state.receivedCount.fetch_add(1, std::memory_order_relaxed) + 1;

    // Record the exact time the last required DATA packet is copied.
    if (received == state.totalPackets) {
        state.finalRecvTimeUs.store( current_time_us(), std::memory_order_relaxed);
    }
    return true;
}

static void receiver_thread(int sockfd, ReceiverState& state, int threadIndex) {
    Packet packet{};
    int timeoutsAfterEnd = 0;

    while (true) {
        ssize_t n = recvfrom(sockfd, &packet, sizeof(packet), 0, nullptr, nullptr);
        if (n < 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                if (state.stopRequested.load(std::memory_order_relaxed)) {
                    timeoutsAfterEnd++;

                    if (timeoutsAfterEnd >= 2) {
                        break;
                    }
                }
                continue;
            }
            perror("recvfrom");
            break;
        }

        timeoutsAfterEnd = 0;

        if (static_cast<size_t>(n) < PACKET_HEADER_SIZE) {
            continue;
        }

        if (packet.data_length > MAX_DATA_SIZE) {
            continue;
        }

        if (static_cast<size_t>(n) <
            PACKET_HEADER_SIZE + packet.data_length) {
            continue;
        }

        if (packet.type == PACKET_DATA) {
            process_data_packet(state, packet);
        }
        else if (packet.type == PACKET_END) {
            state.stopRequested.store(true, std::memory_order_relaxed);
        }
        else if (packet.type == PACKET_START) {
            // Ignore duplicate START packets.
        }
    }

    std::cout << "[thread " << threadIndex << "] receiver finished." << std::endl;
}

static bool send_nack_packets(int sockfd, const std::vector<uint32_t>& missingPackets, const sockaddr_in& clientAddress, uint32_t chunkSize) {
    size_t maxSeqsPerNack = std::max<size_t>(1, chunkSize / sizeof(uint32_t));

    for (size_t i = 0; i < missingPackets.size(); i += maxSeqsPerNack) {
        Packet nackPacket{};
        nackPacket.type = PACKET_NACK;

        size_t count = std::min(maxSeqsPerNack, missingPackets.size() - i);

        std::memcpy(nackPacket.data, missingPackets.data() + i, count * sizeof(uint32_t));
        nackPacket.data_length = static_cast<uint32_t>(count * sizeof(uint32_t));

        ssize_t bytesSent = sendto(
            sockfd,
            &nackPacket,
            packet_wire_size(nackPacket),
            0,
            reinterpret_cast<const sockaddr*>(&clientAddress),
            sizeof(clientAddress)
        );

        if (bytesSent < 0) {
            perror("sendto NACK");
            return false;
        }
    }

    return true;
}

int main() {
    ReceiverState state;

    // Create ONE UDP socket.
    // Both receiver threads will read from this same socket.
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);

    if (sockfd < 0) {
        perror("socket creation failed");
        return EXIT_FAILURE;
    }

    // Give the kernel more room to hold incoming UDP packets.
    int rcvbuf = 32 * 1024 * 1024;

    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf)) < 0) {
        perror("setsockopt(SO_RCVBUF)");
    }

    sockaddr_in servaddr{};
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(SERVER_PORT);

    if (bind( sockfd, reinterpret_cast<sockaddr*>(&servaddr), sizeof(servaddr)) < 0) {
        perror("bind failed");
        close(sockfd);
        return EXIT_FAILURE;
    }

    std::cout << "Server waiting for START packet on port " << SERVER_PORT << "..." << std::endl;

    // Receive START BEFORE launching the receiver threads.
    //
    // This prevents DATA from being discarded while the 1 GB file
    // buffer and packet flags are being prepared.
    Packet startPacket{};
    sockaddr_in clientAddress{};
    socklen_t clientAddressLength = sizeof(clientAddress);

    while (true) {
        ssize_t n = recvfrom(
            sockfd,
            &startPacket,
            sizeof(startPacket),
            0,
            reinterpret_cast<sockaddr*>(&clientAddress),
            &clientAddressLength
        );

        if (n < 0) {
            perror("recvfrom START");
            close(sockfd);
            return EXIT_FAILURE;
        }

        if (static_cast<size_t>(n) < PACKET_HEADER_SIZE) {
            continue;
        }

        if (startPacket.type != PACKET_START) {
            continue;
        }

        if (startPacket.data_length == 0 ||
            startPacket.data_length >= FILENAME_SIZE ||
            startPacket.data_length > MAX_DATA_SIZE) {

            std::cerr << "Invalid START packet." << std::endl;
            continue;
        }

        break;
    }

    state.outputFilename = std::string(
        startPacket.data,
        startPacket.data_length
    );

    state.fileSize = startPacket.file_size;
    state.totalPackets = startPacket.total_packets;
    state.chunkSize = std::min(
        std::max(startPacket.chunk_size, MIN_DATA_SIZE),
        MAX_DATA_SIZE
    );

    state.clientAddress = clientAddress;

    if (state.fileSize == 0 || state.totalPackets == 0) {
        std::cerr << "Invalid file information in START packet."
                  << std::endl;
        close(sockfd);
        return EXIT_FAILURE;
    }

    state.fileBuffer.reset( new (std::nothrow) char[state.fileSize]);

    if (!state.fileBuffer) {
        std::cerr << "Could not allocate file buffer." << std::endl;
        close(sockfd);
        return EXIT_FAILURE;
    }

    state.receivedFlags =  std::make_unique<std::atomic<bool>[]>(state.totalPackets);

    for (uint32_t i = 0; i < state.totalPackets; i++) {
        state.receivedFlags[i].store(false, std::memory_order_relaxed);
    }

    std::cout << "Receiving " << state.fileSize<< " bytes in " << state.totalPackets << " packets using " << NUM_THREADS << " receiver threads." << std::endl;

    // Timeout allows threads to stop shortly after END.
    timeval timeout{};
    timeout.tv_sec = 0;
    timeout.tv_usec = 200000;

    if (setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout)) < 0) {
        perror("setsockopt(SO_RCVTIMEO)");
    }

    // Multiple threads read from the SAME socket.
    // Linux gives each UDP datagram to one waiting thread.
    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);

    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back(receiver_thread, sockfd, std::ref(state), t);
    }

    for (auto& thread : threads) {
        thread.join();
    }

    std::cout << "Initial receive finished." << std::endl;

    // Repair phase.
    while (true) {
        std::vector<uint32_t> missingPackets;

        for (uint32_t seq = 0; seq < state.totalPackets; seq++) {
            if (!state.receivedFlags[seq].load(std::memory_order_relaxed)) {
                missingPackets.push_back(seq);
            }
        }

        std::cout << "Received DATA packets: " << state.receivedCount.load() << " / " << state.totalPackets << std::endl;
        std::cout << "Missing DATA packets: " << missingPackets.size() << std::endl;

        if (missingPackets.empty()) {
            long long finalRecvTime = state.finalRecvTimeUs.load(std::memory_order_relaxed);

            if (finalRecvTime == 0) {
                finalRecvTime = current_time_us();
            }

            std::cout << "Timestamp (Final bit received): " << finalRecvTime << " us (epoch)" << std::endl;

            FILE* outputFile = fopen(state.outputFilename.c_str(), "wb");

            if (outputFile == nullptr) {
                perror("Failed to open output file");
                break;
            }

            size_t bytesWritten = fwrite(state.fileBuffer.get(), 1, state.fileSize, outputFile);
            fclose(outputFile);

            if (bytesWritten != state.fileSize) {
                std::cerr << "Failed to write complete file." << std::endl;
                std::cerr << "Expected: " << state.fileSize << " bytes" << std::endl;
                std::cerr << "Written:  " << bytesWritten << " bytes" << std::endl;

                if (ferror(outputFile)) {
                    perror("fwrite");
                }

                fclose(outputFile);
                break;
            }

            if (fclose(outputFile) != 0) {
                perror("fclose");
                break;
            }

            std::cout << "File written to disk: " << state.outputFilename << std::endl;

            Packet completePacket{};
            completePacket.type = PACKET_COMPLETE;
            completePacket.data_length = 0;

            ssize_t bytesSent = sendto(
                sockfd,
                &completePacket,
                packet_wire_size(completePacket),
                0,
                reinterpret_cast<const sockaddr*>(
                    &state.clientAddress
                ),
                sizeof(state.clientAddress)
            );

            if (bytesSent < 0) {
                perror("sendto COMPLETE");
            }
            else {
                std::cout << "All packets received. COMPLETE sent." << std::endl;
            }

            break;
        }

        if (!send_nack_packets(
                sockfd,
                missingPackets,
                state.clientAddress,
                state.chunkSize)) {

            std::cerr << "Failed to send NACK packets." << std::endl;
            break;
        }

        std::cout << "Waiting for retransmitted packets..." << std::endl;

        while (true) {
            Packet repairPacket{};

            ssize_t n = recvfrom(
                sockfd,
                &repairPacket,
                sizeof(repairPacket),
                0,
                nullptr,
                nullptr
            );

            if (n < 0) {
                if (errno == EWOULDBLOCK || errno == EAGAIN) {
                    break;
                }
                perror("recvfrom repair");
                break;
            }

            if (static_cast<size_t>(n) <
                PACKET_HEADER_SIZE) {
                continue;
            }

            if (repairPacket.type != PACKET_DATA) {
                continue;
            }
            process_data_packet(state, repairPacket);
        }
    }

    close(sockfd);
    return 0;
}
