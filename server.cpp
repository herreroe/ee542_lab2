// Server side implementation of UDP client-server model - side that will be "receiving" the file
// https://www.geeksforgeeks.org/cpp/udp-server-client-implementation-c/

#include <bits/stdc++.h> 
#include <stdlib.h> 
#include <unistd.h> 
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

#include "zap_protocol.hpp"


struct ReceiverState {
    std::mutex fileMutex;
    FILE* outputFile = nullptr;
    bool receivingFile = false;
    std::unordered_set<uint32_t> receivedSequences;

    std::mutex clientMutex;
    sockaddr_in clientAddress{};
    bool clientAddressKnown = false;

    std::atomic<bool> stopRequested{false};
    std::atomic<uint32_t> finalTotalPackets{0};
};

static bool send_nack_packets(
    const std::vector<uint32_t>& missingPackets,
    const sockaddr_in& clientAddress)
{
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed (NACK)");
        return false;
    }

    sockaddr_in serverAddress{};
    serverAddress.sin_family = AF_INET;
    serverAddress.sin_addr.s_addr = INADDR_ANY;
    serverAddress.sin_port = htons(SERVER_PORT);

    if (bind(
            sockfd,
            reinterpret_cast<sockaddr*>(&serverAddress),
            sizeof(serverAddress)) < 0) {
        perror("bind failed (NACK)");
        close(sockfd);
        return false;
    }

    constexpr size_t MAX_SEQS_PER_NACK =
        DATA_SIZE / sizeof(uint32_t);

    for (size_t i = 0; i < missingPackets.size(); i += MAX_SEQS_PER_NACK) {
        Packet nackPacket{};
        nackPacket.type = PACKET_NACK;

        size_t count = std::min(
            MAX_SEQS_PER_NACK,
            missingPackets.size() - i
        );

        memcpy(
            nackPacket.data,
            missingPackets.data() + i,
            count * sizeof(uint32_t)
        );

        nackPacket.data_length =
            static_cast<uint32_t>(count * sizeof(uint32_t));

        ssize_t bytesSent = sendto(
            sockfd,
            &nackPacket,
            sizeof(nackPacket),
            0,
            reinterpret_cast<const sockaddr*>(&clientAddress),
            sizeof(clientAddress)
        );

        if (bytesSent < 0) {
            perror("sendto NACK");
            close(sockfd);
            return false;
        }

        std::cout << "Sent NACK for "
                  << count
                  << " missing packets." << std::endl;
    }

    close(sockfd);
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

    // added bc no flow control or ACKs yet
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
            if (state.outputFile == nullptr) {
                std::cout << "[thread " << threadIndex << "] Received START packet." << std::endl;
                state.receivedSequences.clear();
                std::string filename(packet.data, packet.data_length);

                state.outputFile = fopen(filename.c_str(), "wb");
                if (state.outputFile == nullptr) {
                    perror("Failed to open output file");
                    state.receivingFile = false;
                } else {
                    state.receivingFile = true;
                    std::cout << "[thread " << threadIndex << "] Opened "
                              << filename << " for writing." << std::endl;
                }
            }

        }
        
        // paylaod msg 
        else if (packet.type == PACKET_DATA) {
            std::lock_guard<std::mutex> lock(state.fileMutex);
            
            if (!state.receivingFile) {
                std::cerr << "[thread " << threadIndex << "] Received DATA before START." << std::endl;
                continue;
            }

            // duplicate packet check
            // if sequence number is already in the set, drop this copy and keep original
            if (state.receivedSequences.find(packet.sequence) != state.receivedSequences.end()) {
                std::cout << "[thread " << threadIndex << "] Received duplicate packet "
                          << packet.sequence << " - dropping (original kept)" << std::endl;
                continue;
            }

            // order by sequence number
            // seek where the chunk belongs in the file before writing it
            long offset = static_cast<long>(packet.sequence) * static_cast<long>(DATA_SIZE);
            if (fseek(state.outputFile, offset, SEEK_SET) != 0) {
                perror("fseek");
                continue;
            }

            size_t bytesWritten = fwrite(packet.data, 1, packet.data_length, state.outputFile);
            if (bytesWritten != packet.data_length) {
                std::cerr << "[thread " << threadIndex << "] Failed to write all data" << std::endl;
                fclose(state.outputFile);
                state.outputFile = nullptr;
                state.receivingFile = false;
                break;
            }

            state.receivedSequences.insert(packet.sequence);

            if (packet.sequence %100 == 0) {
                std::cout << "[thread " << threadIndex << "] Received packet " << packet.sequence << " (" << packet.data_length  << " bytes)" << std::endl;
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
    std::vector<uint32_t> missingPackets;
    for (uint32_t seq = 0; seq < totalPackets; ++seq) {
        if (state.receivedSequences.find(seq) == state.receivedSequences.end()) {
            missingPackets.push_back(seq);
        }
    }

    std::cout << "Expected DATA packets: " << totalPackets << std::endl;
    std::cout << "Received DATA packets: " << state.receivedSequences.size() << std::endl;
    std::cout << "Missing DATA packets: " << missingPackets.size() << std::endl;

        if (!missingPackets.empty()) {
        std::cout << "Missing sequence numbers: ";
        for (uint32_t seq : missingPackets) {
            std::cout << seq << " ";
        }
        std::cout << std::endl;

        if (state.clientAddressKnown) {
            sockaddr_in clientAddr;

            {
                std::lock_guard<std::mutex> lock(state.clientMutex);
                clientAddr = state.clientAddress;
            }

            if (!send_nack_packets(missingPackets, clientAddr)) {
                std::cerr << "Failed to send NACK packets." << std::endl;
            }
        }
        else {
            std::cerr << "Cannot send NACK: client address is unknown."
                      << std::endl;
        }
    }

    if (state.outputFile != nullptr) {
        fclose(state.outputFile);
    }

    std::cout << "File transfer complete." << std::endl;

    return 0;
}