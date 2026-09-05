// Client side implementation of UDP client-server model - side that will be "sending" the file.
// https://www.geeksforgeeks.org/cpp/udp-server-client-implementation-c/
#include <bits/stdc++.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>

#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdlib>
#include <chrono>
#include <thread>
#include <vector>
#include <atomic>
#include <mutex>
#include <unordered_set>

#include "zap_protocol.hpp"
#include "zap_cli.hpp"

// Utility to set CPU core affinity
void pin_thread_to_core(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    pthread_t current_thread = pthread_self();
    if (pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset) != 0) {
        std::cerr << "Error setting thread affinity to core " << core_id << std::endl;
    }
}

std::atomic<bool> transfer_complete(false);
std::unordered_set<uint32_t> retransmit_queue;
std::mutex retransmit_mutex;

// Thread 2 (Core 1): ACK/NACK Receiver thread
void ack_listener_thread(int sockfd, int core_id) {
    pin_thread_to_core(core_id);
    std::cout << "[Core " << core_id << "] ACK/NACK listener thread running." << std::endl;

    Packet ackPacket{};
    sockaddr_in fromAddr{};
    socklen_t addrLen = sizeof(fromAddr);

    while (!transfer_complete) {
        ssize_t n = recvfrom(sockfd, &ackPacket, sizeof(ackPacket), 0,
                             reinterpret_cast<sockaddr*>(&fromAddr), &addrLen);
        if (n > 0) {
            if (ackPacket.type == PACKET_NACK) {
                // Add missing packet to retransmission queue
                std::lock_guard<std::mutex> lock(retransmit_mutex);
                retransmit_queue.insert(ackPacket.sequence);
            } else if (ackPacket.type == PACKET_END) {
                transfer_complete = true;
            }
        }
    }
}

int main(int argc, char* argv[]) {
    Args args;
    if (!parse_args(argc, argv, &args)) {
        return EXIT_FAILURE;
    }

    int sockfd;
    const char* serverIP = args.ip_address.c_str();
    const char* filename = args.file_path.c_str();
    struct sockaddr_in servaddr;

    // Fast Memory-Mapped File I/O setup
    int fd = open(filename, O_RDONLY);
    if (fd < 0) {
        std::cerr << "Could not open file: " << filename << std::endl;
        return EXIT_FAILURE;
    }

    struct stat st;
    fstat(fd, &st);
    size_t fileSize = st.st_size;

    char* mapped_file = static_cast<char*>(mmap(NULL, fileSize, PROT_READ, MAP_SHARED, fd, 0));
    if (mapped_file == MAP_FAILED) {
        perror("mmap failed");
        close(fd);
        return EXIT_FAILURE;
    }
    madvise(mapped_file, fileSize, MADV_SEQUENTIAL | MADV_WILLNEED);

    std::cout << "Opened and mapped file: " << filename << " (" << fileSize << " bytes)" << std::endl;

    // Create UDP socket
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed");
        munmap(mapped_file, fileSize);
        close(fd);
        exit(EXIT_FAILURE);
    }

    int bufsize = 32 * 1024 * 1024; // 32MB socket buffer
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));

    std::cout << "UDP socket created." << std::endl;

    memset(&servaddr, 0, sizeof(servaddr));

    // Fill server address info
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(args.port);

    if (inet_pton(AF_INET, serverIP, &servaddr.sin_addr) <= 0) {
        std::cerr << "Invalid server IP address: " << serverIP << std::endl;
        munmap(mapped_file, fileSize);
        close(fd);
        close(sockfd);
        return EXIT_FAILURE;
    }

    if (connect(sockfd, reinterpret_cast<sockaddr*>(&servaddr), sizeof(servaddr)) < 0) {
        perror("connect");
        munmap(mapped_file, fileSize);
        close(fd);
        close(sockfd);
        return EXIT_FAILURE;
    }

    std::cout << "UDP socket connected to " << serverIP << ":" << args.port << std::endl;

    // Pin main transmission thread to Core 0
    pin_thread_to_core(0);

    uint32_t total_packets = (fileSize + DATA_SIZE - 1) / DATA_SIZE;

    // Send START packet with filesize metadata
    Packet startPacket{};
    startPacket.type = PACKET_START;
    startPacket.sequence = total_packets;
    std::string savePath = args.save_dir.empty() ? args.file_path : args.save_dir;
    startPacket.data_length = savePath.size();
    memcpy(startPacket.data, savePath.c_str(), startPacket.data_length);

    send(sockfd, &startPacket, sizeof(startPacket), 0);
    std::cout << "START packet sent. Total packets expected: " << total_packets << std::endl;

    // Spawn ACK/NACK Receiver on Core 1
    std::thread ack_thread(ack_listener_thread, sockfd, 1);

    constexpr double TARGET_BPS = 95.0 * 1000.0 * 1000.0; // Paced near 95 Mbps line rate
    constexpr double BITS_PER_PACKET = sizeof(Packet) * 8.0;
    const auto packet_interval = std::chrono::nanoseconds(
        static_cast<long long>((BITS_PER_PACKET / TARGET_BPS) * 1e9)
    );

    auto next_send_time = std::chrono::high_resolution_clock::now();

    // Primary Transmission Loop (Core 0)
    for (uint32_t seq = 0; seq < total_packets; ++seq) {
        // Priority check for retransmissions
        {
            std::lock_guard<std::mutex> lock(retransmit_mutex);
            if (!retransmit_queue.empty()) {
                auto it = retransmit_queue.begin();
                uint32_t r_seq = *it;
                retransmit_queue.erase(it);

                Packet rPacket{};
                rPacket.type = PACKET_DATA;
                rPacket.sequence = r_seq;
                size_t offset = (size_t)r_seq * DATA_SIZE;
                size_t bytesToRead = std::min((size_t)DATA_SIZE, fileSize - offset);
                rPacket.data_length = bytesToRead;
                memcpy(rPacket.data, mapped_file + offset, bytesToRead);

                send(sockfd, &rPacket, sizeof(rPacket), 0);
            }
        }

        Packet dataPacket{};
        dataPacket.type = PACKET_DATA;
        dataPacket.sequence = seq;

        size_t offset = (size_t)seq * DATA_SIZE;
        size_t bytesToRead = std::min((size_t)DATA_SIZE, fileSize - offset);
        dataPacket.data_length = static_cast<uint32_t>(bytesToRead);
        memcpy(dataPacket.data, mapped_file + offset, bytesToRead);

        send(sockfd, &dataPacket, sizeof(dataPacket), 0);

        if (seq % 10000 == 0) {
            std::cout << "[Core 0] Sent packet " << seq << "/" << total_packets << std::endl;
        }

        next_send_time += packet_interval;
        while (std::chrono::high_resolution_clock::now() < next_send_time) {
            #if defined(__x86_64__) || defined(_M_X64)
            __builtin_ia32_pause();
            #endif
        }
    }

    // Retransmission Sweep Loop (Core 0) until Server sends completion ACK
    while (!transfer_complete) {
        uint32_t r_seq = 0;
        bool has_retransmit = false;
        {
            std::lock_guard<std::mutex> lock(retransmit_mutex);
            if (!retransmit_queue.empty()) {
                auto it = retransmit_queue.begin();
                r_seq = *it;
                retransmit_queue.erase(it);
                has_retransmit = true;
            }
        }

        if (has_retransmit) {
            Packet rPacket{};
            rPacket.type = PACKET_DATA;
            rPacket.sequence = r_seq;
            size_t offset = (size_t)r_seq * DATA_SIZE;
            size_t bytesToRead = std::min((size_t)DATA_SIZE, fileSize - offset);
            rPacket.data_length = bytesToRead;
            memcpy(rPacket.data, mapped_file + offset, bytesToRead);

            send(sockfd, &rPacket, sizeof(rPacket), 0);
        } else {
            // Re-notify END to elicit final confirmation
            Packet endPacket{};
            endPacket.type = PACKET_END;
            send(sockfd, &endPacket, sizeof(endPacket), 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    }

    if (ack_thread.joinable()) {
        ack_thread.join();
    }

    // Cleanup fast memory maps
    munmap(mapped_file, fileSize);
    close(fd);
    close(sockfd);

    std::cout << "File transfer complete." << std::endl;
    return 0;
}