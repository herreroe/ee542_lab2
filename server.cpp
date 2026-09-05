// Server side implementation of UDP client-server model - side that will be "receiving" the file
// https://www.geeksforgeeks.org/cpp/udp-server-client-implementation-c/

#include <bits/stdc++.h> 
#include <stdlib.h> 
#include <unistd.h> 
#include <fcntl.h> 
#include <string.h> 
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
#include <mutex>
#include <atomic>
#include <chrono>

#include "zap_protocol.hpp"


struct ReceiverState {
    std::mutex fileMutex;
    int outputFd = -1;
    std::atomic<bool> receivingFile{false};
    std::vector<std::atomic<uint8_t>> receivedSequences;

    std::atomic<bool> stopRequested{false};
    std::atomic<uint32_t> finalTotalPackets{0};
    std::chrono::high_resolution_clock::time_point finalRecvTime;
};

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

    int bufsize = 32 * 1024 * 1024; // 32MB .. can increase/decrease this
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));

    int actual_buf = 0;
    socklen_t optlen = sizeof(actual_buf);
    getsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &actual_buf, &optlen);
    std::cout << "Actual OS Receive Buffer Size: " << actual_buf << " bytes\n";
  
    struct sockaddr_in servaddr{};
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = INADDR_ANY;
    servaddr.sin_port = htons(SERVER_PORT);

    if (bind(sockfd, reinterpret_cast<sockaddr*>(&servaddr), sizeof(servaddr)) < 0) {
        perror("bind failed (receiver thread)");
        close(sockfd);
        return;
    }

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
            std::lock_guard<std::mutex> lock(state.fileMutex);
            if (state.outputFd == -1) {
                std::cout << "[thread " << threadIndex << "] Received START packet." << std::endl;
                uint64_t fileSize = 0;
                memcpy(&fileSize, packet.data, sizeof(fileSize));
                std::string filename(packet.data + sizeof(fileSize), packet.data_length);

                state.outputFd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
                if (state.outputFd < 0) {
                    perror("Failed to open output file");
                    state.receivingFile.store(false);
                } else if (ftruncate(state.outputFd, static_cast<off_t>(fileSize)) != 0) {
                    perror("Failed to truncate output file");
                    close(state.outputFd);
                    state.outputFd = -1;
                    state.receivingFile.store(false);
                } else {
                    state.receivedSequences = std::vector<std::atomic<uint8_t>>(packet.sequence);
                    for (auto& received : state.receivedSequences) {
                        received.store(0, std::memory_order_relaxed);
                    }
                    state.receivingFile.store(true, std::memory_order_release);
                    std::cout << "[thread " << threadIndex << "] Opened "
                              << filename << " for writing." << std::endl;
                }
            }

        }
        
        // paylaod msg 
        else if (packet.type == PACKET_DATA) {
            if (!state.receivingFile.load(std::memory_order_acquire)) {
                std::cerr << "[thread " << threadIndex << "] Received DATA before START." << std::endl;
                continue;
            }

            // duplicate packet check
            // if sequence number is already in the set, drop this copy and keep original
            if (packet.sequence >= state.receivedSequences.size() ||
                state.receivedSequences[packet.sequence].exchange(1, std::memory_order_relaxed) != 0) {
                continue;
            }

            // order by sequence number
            // seek where the chunk belongs in the file before writing it
            off_t offset = static_cast<off_t>(packet.sequence) * static_cast<off_t>(DATA_SIZE);

            ssize_t bytesWritten = pwrite(state.outputFd, packet.data, packet.data_length, offset);
            if (bytesWritten != static_cast<ssize_t>(packet.data_length)) {
                std::cerr << "[thread " << threadIndex << "] Failed to write all data" << std::endl;
                state.receivedSequences[packet.sequence].store(0, std::memory_order_relaxed);
                continue;
            }

            if (packet.sequence %100 == 0) {
                std::cout << "[thread " << threadIndex << "] Received packet " << packet.sequence << " (" << packet.data_length  << " bytes)" << std::endl;
            }
        }
        // end msg
        else if (packet.type == PACKET_END) {
            auto recv_time = std::chrono::high_resolution_clock::now();
            std::cout << "[thread " << threadIndex << "] Received END packet." << std::endl;
            state.finalTotalPackets.store(packet.sequence);
            state.stopRequested.store(true);
            state.finalRecvTime = recv_time;
        }
        else {
            std::cerr << "[thread " << threadIndex << "] Unknown packet type: " << packet.type << std::endl;
        }
    }

    close(sockfd);
    std::cout << "[thread " << threadIndex << "] Closing connection." << std::endl;

}

int main() {
    std::cout << "Starting " << NUM_THREADS
              << " receiver threads, each bound to UDP port "
              << SERVER_PORT << " via SO_REUSEPORT." << std::endl;

    ReceiverState state;
    std::vector<std::thread> threads;
    threads.reserve(NUM_THREADS);
    for (int t = 0; t < NUM_THREADS; t++) {
        threads.emplace_back(receiver_thread, std::ref(state), t);
    }

    for (auto& th : threads) {
        th.join();
    }

    uint32_t totalPackets = state.finalTotalPackets.load();
    uint32_t receivedPackets = 0;
    std::vector<uint32_t> missingPackets;
    for (uint32_t seq = 0; seq < totalPackets; ++seq) {
        if (seq >= state.receivedSequences.size() ||
            state.receivedSequences[seq].load(std::memory_order_relaxed) == 0) {
            missingPackets.push_back(seq);
        } else {
            receivedPackets++;
        }
    }

    std::cout << "Expected DATA packets: " << totalPackets << std::endl;
    std::cout << "Received DATA packets: " << receivedPackets << std::endl;
    std::cout << "Missing DATA packets: " << missingPackets.size() << std::endl;

    if (!missingPackets.empty()) {
        std::cout << "Missing sequence numbers: ";
        for (uint32_t seq : missingPackets) {
            std::cout << seq << " ";
        }
        std::cout << std::endl;
    }

    if (state.outputFd >= 0) {
        close(state.outputFd);
    }

    std::cout << "File transfer complete." << std::endl;

    auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(state.finalRecvTime.time_since_epoch()).count();
    std::cout << "Timestamp (Final bit received): " << duration_us << " us (epoch)" << std::endl;

    return 0;

}