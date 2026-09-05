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
#include <sys/mman.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <pthread.h>
#include <sched.h>

#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdlib>
#include <vector>
#include <atomic>
#include <thread>
#include <mutex>

#include "zap_protocol.hpp"

void pin_thread_to_core(int core_id) {
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);
    pthread_t current_thread = pthread_self();
    if (pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset) != 0) {
        std::cerr << "Error setting thread affinity to core " << core_id << std::endl;
    }
}

std::atomic<bool> receiving_file(false);
std::atomic<bool> transfer_done(false);
std::atomic<uint32_t> total_expected_packets(0);
std::atomic<uint32_t> received_packets_count(0);

std::vector<bool> received_bitmap;
std::mutex bitmap_mutex;

sockaddr_in global_client_addr{};
socklen_t global_client_len = sizeof(global_client_addr);

// Thread 2 (Core 1): Selective NACK / Status Monitor thread
void nack_generator_thread(int sockfd, int core_id) {
    pin_thread_to_core(core_id);
    std::cout << "[Core " << core_id << "] NACK monitor thread running." << std::endl;

    while (!transfer_done) {
        std::this_thread::sleep_for(std::chrono::milliseconds(30));

        if (!receiving_file) continue;

        uint32_t total = total_expected_packets.load();
        if (total == 0) continue;

        // Scan bitmap and transmit NACKs for missing segments
        for (uint32_t i = 0; i < total; ++i) {
            bool is_recvd = false;
            {
                std::lock_guard<std::mutex> lock(bitmap_mutex);
                is_recvd = received_bitmap[i];
            }

            if (!is_recvd) {
                Packet nack{};
                nack.type = PACKET_NACK;
                nack.sequence = i;
                sendto(sockfd, &nack, sizeof(nack), 0,
                       reinterpret_cast<sockaddr*>(&global_client_addr), global_client_len);
            }
        }

        if (received_packets_count.load() >= total) {
            transfer_done = true;
            // Send completion ACK back to client
            Packet endAck{};
            endAck.type = PACKET_END;
            for (int i = 0; i < 5; ++i) { // Send multiple to ensure delivery over lossy links
                sendto(sockfd, &endAck, sizeof(endAck), 0,
                       reinterpret_cast<sockaddr*>(&global_client_addr), global_client_len);
            }
            break;
        }
    }
}

int main() { 
    int sockfd; 
    struct sockaddr_in servaddr, cliaddr; 
      
    // Creating socket file descriptor 
    if ( (sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0 ) { 
        perror("socket creation failed"); 
        exit(EXIT_FAILURE); 
    } 

    int bufsize = 32 * 1024 * 1024; // 32MB socket buffer
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &bufsize, sizeof(bufsize));
    setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &bufsize, sizeof(bufsize));
    std::cout << "UDP socket created." << std::endl;
  
    memset(&servaddr, 0, sizeof(servaddr)); 
    memset(&cliaddr, 0, sizeof(cliaddr)); 
      
    // Filling server information 
    servaddr.sin_family      = AF_INET; // IPv4 
    servaddr.sin_addr.s_addr = INADDR_ANY; 
    servaddr.sin_port        = htons(SERVER_PORT); 
      
    // Bind the socket with the server address 
    if ( bind(sockfd, (const struct sockaddr *)&servaddr, sizeof(servaddr)) < 0 ) { 
        perror("bind failed"); 
        exit(EXIT_FAILURE); 
    } 
      
    std::cout << "Server listening on UDP port " << SERVER_PORT << std::endl;
        
    Packet packet{};
    sockaddr_in clientAddress{};
    socklen_t clientAddressLength = sizeof(clientAddress);

    char* mapped_file = nullptr;
    int out_fd = -1;
    size_t file_size = 0;

    // Pin Receiver Loop to Core 0
    pin_thread_to_core(0);

    std::thread nack_thread;

    while (!transfer_done) {
        ssize_t n = recvfrom(
            sockfd,
            &packet,
            sizeof(packet),
            0,
            reinterpret_cast<sockaddr*>(&clientAddress),
            &clientAddressLength
        );

        if (n < 0) {
            perror("recvfrom");
            break;
        }

        global_client_addr = clientAddress;
        global_client_len = clientAddressLength;

        // Start msg
        if (packet.type == PACKET_START) {
            std::cout << "Received START packet." << std::endl;

            std::string filename(packet.data, packet.data_length);
            uint32_t total_pkts = packet.sequence;
            total_expected_packets.store(total_pkts);
            file_size = static_cast<size_t>(total_pkts) * DATA_SIZE;

            // Prepare 1GB sparse file and memory map for zero-copy writes
            out_fd = open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
            if (out_fd < 0 || ftruncate(out_fd, file_size) != 0) {
                perror("Failed to allocate file space");
                receiving_file = false;
            } else {
                mapped_file = static_cast<char*>(mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, out_fd, 0));
                if (mapped_file == MAP_FAILED) {
                    perror("mmap failed");
                    receiving_file = false;
                } else {
                    madvise(mapped_file, file_size, MADV_WILLNEED);
                    received_bitmap.assign(total_pkts, false);
                    receiving_file = true;

                    // Launch Core 1 NACK monitoring thread
                    if (!nack_thread.joinable()) {
                        nack_thread = std::thread(nack_generator_thread, sockfd, 1);
                    }
                }
            }

            std::cout << "Opened " << filename << " and pre-allocated memory mapping." << std::endl;
        }
        // payload msg 
        else if (packet.type == PACKET_DATA) {
            if (!receiving_file) {
                std::cerr << "Received DATA before START." << std::endl;
                continue;
            }

            uint32_t seq = packet.sequence;
            size_t offset = (size_t)seq * DATA_SIZE;

            bool already_received = false;
            {
                std::lock_guard<std::mutex> lock(bitmap_mutex);
                if (seq < received_bitmap.size()) {
                    already_received = received_bitmap[seq];
                    if (!already_received) {
                        received_bitmap[seq] = true;
                    }
                }
            }

            if (!already_received && mapped_file != nullptr) {
                // Direct zero-copy copy into memory map offset
                memcpy(mapped_file + offset, packet.data, packet.data_length);
                received_packets_count++;
            }

            if (seq % 10000 == 0) {
                std::cout << "[Core 0] Received packet " << seq << " (" << packet.data_length << " bytes)" << std::endl;
            }
        }
        // end msg
        else if (packet.type == PACKET_END) {
            if (received_packets_count.load() >= total_expected_packets.load()) {
                transfer_done = true;
            }
        }
    }

    if (nack_thread.joinable()) {
        nack_thread.join();
    }

    // Save and cleanup memory mappings
    if (mapped_file != nullptr && mapped_file != MAP_FAILED) {
        msync(mapped_file, file_size, MS_SYNC);
        munmap(mapped_file, file_size);
    }
    if (out_fd >= 0) {
        close(out_fd);
    }

    close(sockfd);
    std::cout << "File transfer complete." << std::endl;
    return 0; 
}