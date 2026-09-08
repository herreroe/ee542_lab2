// Server side implementation of UDP client-server model - side that will be "receiving" the file

#include <bits/stdc++.h> 
#include <stdlib.h> 
#include <unistd.h> 
#include <string.h> 
#include <fcntl.h>
#include <sys/types.h> 
#include <sys/socket.h> 
#include <arpa/inet.h> 
#include <netinet/in.h> 
  
#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdlib>

#include <thread>
#include <vector>
#include <memory>
#include <mutex>
#include <atomic>

#include "zap_protocol.hpp"


struct ReceiverState {
    // Everything below is written once, under startMutex, when the START
    // packet is processed, then published to the other receiver thread(s)
    // via the `ready` flag - so the DATA hot path never has to take a lock
    // or hash into an unordered_set; it just does an atomic check + pwrite.
    std::mutex startMutex;
    int outputFd = -1;
    uint32_t chunkSize = 0;
    uint64_t fileSize = 0;
    uint32_t totalPackets = 0;
    std::unique_ptr<std::atomic<bool>[]> receivedFlags;
    std::atomic<bool> ready{false};

    std::atomic<uint32_t> receivedCount{0};

    std::mutex clientMutex;
    sockaddr_in clientAddress{};
    bool clientAddressKnown = false;

    std::atomic<bool> stopRequested{false};
    std::atomic<uint32_t> finalTotalPackets{0};
};

static bool send_nack_packets(int sockfd, const std::vector<uint32_t>& missingPackets, const sockaddr_in& clientAddress, uint32_t chunkSize) {
    // Cap each NACK packet's payload by the negotiated per-packet budget
    // for this link (the same chunkSize DATA packets use), not by the
    // max buffer capacity - otherwise a full batch could still exceed
    // the path MTU and fail with EMSGSIZE on low-MTU links.
    size_t maxSeqsPerNack = std::max<size_t>(1, chunkSize / sizeof(uint32_t));

    for (size_t i = 0; i < missingPackets.size(); i += maxSeqsPerNack) {
        Packet nackPacket{};
        nackPacket.type = PACKET_NACK;

        size_t count = std::min(maxSeqsPerNack, missingPackets.size() - i);

        memcpy(nackPacket.data, missingPackets.data() + i, count * sizeof(uint32_t));

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
        std::cout << "Sent NACK for " << count << " missing packets." << std::endl;
    }

    return true;
}

static void receiver_thread(ReceiverState& state, int threadIndex) {
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed (receiver thread)");
        return;
    }

    int optval = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEPORT, &optval, sizeof(optval)) < 0) {
        perror("setsockopt(SO_REUSEPORT) failed");
        close(sockfd);
        return;
    }

    int rcvbuf = 32 * 1024 * 1024;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
  
    struct sockaddr_in servaddr{};
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(SERVER_PORT);

    if (bind(sockfd, reinterpret_cast<sockaddr*>(&servaddr), sizeof(servaddr)) < 0) {
        perror("bind failed (receiver thread)");
        close(sockfd);
        return;
    }

    // add in timeout.
    struct timeval timeout{};
    timeout.tv_sec = 0;
    timeout.tv_usec = 200000; // 200ms
    setsockopt(sockfd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        
    Packet packet{};
    sockaddr_in clientAddress{};
    socklen_t clientAddressLength = sizeof(clientAddress);
    int consecutiveTimeoutsAfterStop = 0;
    
    while (true)
    {
        ssize_t n = recvfrom(
            sockfd,
            &packet,
            sizeof(packet),
            0,
            reinterpret_cast<sockaddr*>(&clientAddress),
            &clientAddressLength
        );

        // handles timeouts/errors
        if (n < 0) {
            if (errno == EWOULDBLOCK || errno == EAGAIN) {
                if (state.stopRequested.load()) {
                    if(++consecutiveTimeoutsAfterStop >= 2) break; 
                }
                continue;
            }
            perror("recvfrom");
            break;
        }
        consecutiveTimeoutsAfterStop = 0;

        // Start msg
        if (packet.type == PACKET_START) {
            std::lock_guard<std::mutex> lock(state.startMutex);
            if (!state.ready.load(std::memory_order_relaxed)) {
                std::cout << "[thread " << threadIndex << "] Received START packet." << std::endl;

                std::string filename(packet.data, packet.data_length);
                uint32_t chunkSize = std::min(std::max(packet.chunk_size, MIN_DATA_SIZE), MAX_DATA_SIZE);
                uint64_t fileSize = packet.file_size;
                uint32_t totalPackets = static_cast<uint32_t>((fileSize + chunkSize - 1) / chunkSize);

                int fd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
                if (fd < 0) {
                    perror("Failed to open output file");
                } else {
                    // Pre-size the file so pwrite() never has to grow it mid-transfer.
                    if (fileSize > 0 && ftruncate(fd, static_cast<off_t>(fileSize)) != 0) {
                        perror("ftruncate failed");
                    }

                    state.receivedFlags = std::make_unique<std::atomic<bool>[]>(totalPackets);
                    for (uint32_t i = 0; i < totalPackets; ++i) {
                        state.receivedFlags[i].store(false, std::memory_order_relaxed);
                    }

                    state.outputFd = fd;
                    state.chunkSize = chunkSize;
                    state.fileSize = fileSize;
                    state.totalPackets = totalPackets;
                    state.receivedCount.store(0, std::memory_order_relaxed);

                    std::cout << "[thread " << threadIndex << "] Opened " << filename
                              << " for writing (" << fileSize << " bytes, " << totalPackets
                              << " packets, " << chunkSize << " bytes/packet)." << std::endl;

                    // Publish the setup above to the other receiver thread(s).
                    // DATA packets check this flag (acquire) before touching
                    // any of the fields set here, so no further locking is
                    // needed on the hot path.
                    state.ready.store(true, std::memory_order_release);
                }
            }

        }
        
        // paylaod msg 
        else if (packet.type == PACKET_DATA) {
            if (!state.ready.load(std::memory_order_acquire)) {
                std::cerr << "[thread " << threadIndex << "] Received DATA before START." << std::endl;
                continue;
            }

            if (packet.sequence >= state.totalPackets || packet.data_length > MAX_DATA_SIZE) {
                std::cerr << "[thread " << threadIndex << "] Invalid DATA packet (sequence "
                          << packet.sequence << ")." << std::endl;
                continue;
            }

            // duplicate packet check - lock-free flag lookup, no fseek/fwrite
            // serialization between the two receiver threads
            if (state.receivedFlags[packet.sequence].load(std::memory_order_relaxed)) {
                continue;
            }

            // pwrite() writes to an explicit offset and doesn't touch a
            // shared file position, so both receiver threads can write to
            // the same fd concurrently (to different regions) with no lock.
            off_t offset = static_cast<off_t>(packet.sequence) * static_cast<off_t>(state.chunkSize);
            ssize_t bytesWritten = pwrite(state.outputFd, packet.data, packet.data_length, offset);
            if (bytesWritten != static_cast<ssize_t>(packet.data_length)) {
                perror("pwrite");
                continue;
            }

            bool expected = false;
            if (state.receivedFlags[packet.sequence].compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
                uint32_t received = state.receivedCount.fetch_add(1, std::memory_order_relaxed) + 1;
                if (packet.sequence % 1000 == 0) { // occasional print
                    std::cout << "[thread " << threadIndex << "] Received packet " << packet.sequence
                              << " (" << packet.data_length << " bytes), " << received << " total." << std::endl;
                }
            }
        }
        // end msg
        else if (packet.type == PACKET_END) {
            std::cout << "[thread " << threadIndex << "] Received END packet." << std::endl;
            {
                std::lock_guard<std::mutex> lock(state.clientMutex);
                state.clientAddress = clientAddress;
                state.clientAddressKnown = true;
            }

            state.finalTotalPackets.store(packet.sequence);
            state.stopRequested.store(true);
        }
        else {
            std::cerr << "[thread " << threadIndex << "] Unknown packet type: " << packet.type << std::endl;
        }
    }

    close(sockfd);
    std::cout << "[thread " << threadIndex << "] Closing connection." << std::endl;
}

int main() {
    std::cout << "Starting " << NUM_THREADS << " receiver threads, each bound to UDP port " << SERVER_PORT << " via SO_REUSEPORT." << std::endl;

    ReceiverState state;
    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back(receiver_thread, std::ref(state), t);
    }

    for (auto& th : threads) {
        th.join();
    }

    // Prefer the packet count derived from START (it's what state.chunkSize
    // and the preallocated bitmap were sized against); fall back to END's
    // count only if START was somehow never processed.
    uint32_t totalPackets = state.totalPackets != 0 ? state.totalPackets : state.finalTotalPackets.load();
    uint32_t endReportedPackets = state.finalTotalPackets.load();
    if (endReportedPackets != 0 && endReportedPackets != totalPackets) {
        std::cerr << "Warning: END packet reported " << endReportedPackets
                  << " total packets but START-derived count was " << totalPackets << std::endl;
    }

    std::cout << "Expected DATA packets: " << totalPackets << std::endl;

    if (state.outputFd < 0) {
        std::cerr << "Output file is not available." << std::endl;
        return EXIT_FAILURE;
    }

    sockaddr_in clientAddr{};

    {
        std::lock_guard<std::mutex> lock(state.clientMutex);
        if (!state.clientAddressKnown) {
            std::cerr << "Cannot repair transfer: client address is unknown." << std::endl;
            close(state.outputFd);
            return EXIT_FAILURE;
        }
        clientAddr = state.clientAddress;
    }

    // Create one socket for the repair phase.
    // This socket sends NACKs and receives retransmitted DATA.
    int repairSock = socket(AF_INET, SOCK_DGRAM, 0);

    if (repairSock < 0) {
        perror("socket creation failed (repair)");
        close(state.outputFd);
        return EXIT_FAILURE;
    }

    sockaddr_in repairAddress{};
    repairAddress.sin_family = AF_INET;
    repairAddress.sin_addr.s_addr = INADDR_ANY;
    repairAddress.sin_port = htons(SERVER_PORT);

    if (bind(repairSock, reinterpret_cast<sockaddr*>(&repairAddress), sizeof(repairAddress)) < 0) {
        perror("bind failed (repair)");
        close(repairSock);
        close(state.outputFd);
        return EXIT_FAILURE;
    }

    // Larger buffer for retransmitted packets
    int rcvbuf = 32 * 1024 * 1024;
    setsockopt(repairSock, SOL_SOCKET, SO_RCVBUF,  &rcvbuf, sizeof(rcvbuf));

    // After 500 ms without another repair packet,
    // check the whole file again.
    struct timeval timeout{};
    timeout.tv_sec = 0;
    timeout.tv_usec = 500000;

    setsockopt(repairSock, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));

    while (true) {
        std::vector<uint32_t> missingPackets;

        // Check the entire sequence range again
        for (uint32_t seq = 0; seq < totalPackets; ++seq) {
            if (!state.receivedFlags[seq].load(std::memory_order_relaxed)) {
                missingPackets.push_back(seq);
            }
        }

        std::cout << "Received DATA packets: " << state.receivedCount.load() << std::endl;
        std::cout << "Missing DATA packets: " << missingPackets.size() << std::endl;

        // Everything has arrived
        if (missingPackets.empty()) {
            auto finalRecvTime = std::chrono::high_resolution_clock::now();
            auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(finalRecvTime.time_since_epoch()).count();
            std::cout << "Timestamp (Final bit received): " << duration_us << " us (epoch)" << std::endl;            
            Packet completePacket{};
            completePacket.type = PACKET_COMPLETE;
            completePacket.data_length = 0;

            ssize_t bytesSent = sendto(
                repairSock,
                &completePacket,
                packet_wire_size(completePacket),
                0,
                reinterpret_cast<const sockaddr*>(&clientAddr),
                sizeof(clientAddr)
            );

            if (bytesSent < 0) {
                perror("sendto COMPLETE");
            }
            else {
                std::cout << "All packets received. COMPLETE sent." << std::endl;
            }
            break;
        }

        // Tell client which packets are still missing
        if (!send_nack_packets(repairSock, missingPackets, clientAddr, state.chunkSize)) {
            std::cerr << "Failed to send NACK packets." << std::endl;
            break;
        }

        std::cout << "Waiting for retransmitted packets..." << std::endl;

        // Receive only retransmitted DATA packets
        while (true) {
            Packet repairPacket{};

            ssize_t n = recvfrom(repairSock, &repairPacket, sizeof(repairPacket), 0, nullptr, nullptr);

            if (n < 0) {
                if (errno == EWOULDBLOCK || errno == EAGAIN) {
                    std::cout << "Repair timeout. Rechecking missing packets..." << std::endl;
                    break;
                }

                perror("recvfrom repair");
                break;
            }

            if (repairPacket.type != PACKET_DATA) {
                continue;
            }

            if (repairPacket.sequence >= totalPackets) {
                std::cerr << "Invalid retransmitted sequence: " << repairPacket.sequence << std::endl;
                continue;
            }

            if (repairPacket.data_length > MAX_DATA_SIZE) {
                std::cerr << "Invalid retransmitted packet length." << std::endl;
                continue;
            }

            // Already received this sequence
            if (state.receivedFlags[repairPacket.sequence].load(std::memory_order_relaxed)) {
                continue;
            }

            off_t offset = static_cast<off_t>(repairPacket.sequence) * static_cast<off_t>(state.chunkSize);

            ssize_t bytesWritten = pwrite(state.outputFd, repairPacket.data, repairPacket.data_length, offset);

            if (bytesWritten != static_cast<ssize_t>(repairPacket.data_length)) {
                std::cerr << "Failed to write retransmitted packet " << repairPacket.sequence << std::endl;
                continue;
            }

            bool expected = false;
            if (state.receivedFlags[repairPacket.sequence].compare_exchange_strong(expected, true, std::memory_order_relaxed)) {
                state.receivedCount.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }

    close(repairSock);

    if (state.outputFd >= 0) {
        fsync(state.outputFd);
        close(state.outputFd);
    }

    return 0;
}