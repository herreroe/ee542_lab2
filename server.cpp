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
    bool receivingFile = false;

    std::string outputFilename;

    std::vector<char> fileBuffer;
    std::vector<uint8_t> received;

    uint64_t fileSize = 0;
    uint32_t totalPackets = 0;
    
    std::mutex clientMutex;
    sockaddr_in clientAddress{};
    bool clientAddressKnown = false;

    std::atomic<bool> stopRequested{false};
    std::atomic<uint32_t> finalTotalPackets{0};
};

static bool send_nack_packets(
    int sockfd,
    const std::vector<uint32_t>& missingPackets,
    const sockaddr_in& clientAddress)
{
    constexpr size_t MAX_SEQS_PER_NACK =
        DATA_SIZE / sizeof(uint32_t);

    for (size_t i = 0; i < missingPackets.size(); i += MAX_SEQS_PER_NACK) {
        Packet nackPacket{};
        nackPacket.type = PACKET_NACK;

        size_t count = std::min(MAX_SEQS_PER_NACK, missingPackets.size() - i);

        memcpy( nackPacket.data, missingPackets.data() + i, count * sizeof(uint32_t));

        nackPacket.data_length = static_cast<uint32_t>(count * sizeof(uint32_t));

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
            if (!state.receivingFile) {
                std::cout << "[thread " << threadIndex << "] Received START packet." << std::endl;
                
                state.outputFilename = std::string (packet.data, packet.data_length);

                state.fileSize = packet.file_size;
                state.totalPackets = packet.total_packets;

                state.fileBuffer.resize(state.fileSize);
                state.received.assign(state.totalPackets, 0);

                state.receivingFile = true;

                std::cout << "[thread " << threadIndex << "] Prepared " << state.fileSize << " bytes in memory for " <<state.totalPackets << " packets." << std::endl;
            }

        }
        
        // paylaod msg 
        else if (packet.type == PACKET_DATA) {
            std::lock_guard<std::mutex> lock(state.fileMutex);
            
            if (!state.receivingFile) {
                std::cerr << "[thread " << threadIndex << "] Received DATA before START." << std::endl;
                continue;
            }

            if (packet.sequence >= state.totalPackets) {
                std::cerr << "[thread " << threadIndex << "] Invalid sequence: " << packet.sequence << std::endl;
                continue;
            }

            if (packet.data_length > DATA_SIZE) {
                std::cerr << "[thread " << threadIndex << "] Invalid DATA length." << std::endl;
                continue;
            }

            // duplicate packet check
            // if sequence number is already in the set, drop this copy and keep original
            if (state.received[packet.sequence]) {
                continue;
            }

            uint64_t offset = static_cast<uint64_t>(packet.sequence) * DATA_SIZE;

            std::memcpy(state.fileBuffer.data() + offset, packet.data, packet.data_length);

            state.received[packet.sequence] = 1;

            if (packet.sequence % 1000 == 0) {
                std::cout << "[thread " << threadIndex << "] Received packet " << packet.sequence << " (" << packet.data_length << " bytes)" << std::endl;
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

    uint32_t totalPackets = state.finalTotalPackets.load();

    std::cout << "Expected DATA packets: " << totalPackets << std::endl;

    if (!state.receivingFile) {
        std::cerr << "No file transfer was started." << std::endl;
        return EXIT_FAILURE;
    }

    sockaddr_in clientAddr{};

    {
        std::lock_guard<std::mutex> lock(state.clientMutex);

        if (!state.clientAddressKnown) {
            std::cerr
                << "Cannot repair transfer: client address is unknown."
                << std::endl;

            return EXIT_FAILURE;
        }

        clientAddr = state.clientAddress;
    }

    // Create one socket for the repair phase.
    // This socket sends NACKs and receives retransmitted DATA.
    int repairSock = socket(AF_INET, SOCK_DGRAM, 0);

    if (repairSock < 0) {
        perror("socket creation failed (repair)");
        return EXIT_FAILURE;
    }

    sockaddr_in repairAddress{};
    repairAddress.sin_family = AF_INET;
    repairAddress.sin_addr.s_addr = INADDR_ANY;
    repairAddress.sin_port = htons(SERVER_PORT);

    if (bind( repairSock, reinterpret_cast<sockaddr*>(&repairAddress), sizeof(repairAddress)) < 0) {
        perror("bind failed (repair)");
        close(repairSock);
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
            if (!state.received[seq]) {
                missingPackets.push_back(seq);
            }
        }

        std::cout << "Missing DATA packets: " << missingPackets.size() << std::endl;

        // Everything has arrived
        if (missingPackets.empty()) {
            auto finalRecvTime = std::chrono::high_resolution_clock::now();
            auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(finalRecvTime.time_since_epoch()).count();
            std::cout << "Timestamp (Final bit received): " << duration_us << " us (epoch)" << std::endl;            
            
            FILE* outputFile = fopen(state.outputFilename.c_str(), "wb");

            if (outputFile == nullptr) {
                perror("Failed to open output file");
                break;
            }

            size_t bytesWritten = fwrite(
                state.fileBuffer.data(),
                1,
                state.fileSize,
                outputFile
            );

            fclose(outputFile);

            auto writeCompleteTime = std::chrono::high_resolution_clock::now();
            auto writeComplete_us = std::chrono::duration_cast<std::chrono::microseconds>(writeCompleteTime.time_since_epoch()).count();
            
            std::cout << "Timestamp (File write completed): " << writeComplete_us << " us (epoch)" << std::endl;
            
            if (bytesWritten != state.fileSize) {
                std::cerr << "Failed to write complete file." << std::endl;
                break;
            }

            std::cout << "File written to disk: " << state.outputFilename << std::endl;

            Packet completePacket{};
            completePacket.type = PACKET_COMPLETE;
            completePacket.data_length = 0;

            ssize_t bytesSent = sendto(
                repairSock,
                &completePacket,
                sizeof(completePacket),
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
        if (!send_nack_packets(repairSock, missingPackets, clientAddr)) {
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

            if (repairPacket.data_length > DATA_SIZE) {
                std::cerr << "Invalid retransmitted packet length." << std::endl;
                continue;
            }

            if (state.received[repairPacket.sequence]) {
                continue;
            }

            uint64_t offset = static_cast<uint64_t>(repairPacket.sequence) * DATA_SIZE;

            std::memcpy(state.fileBuffer.data() + offset, repairPacket.data, repairPacket.data_length);

            state.received[repairPacket.sequence] = 1 ;
        }
    }

    close(repairSock);

    return 0;
}