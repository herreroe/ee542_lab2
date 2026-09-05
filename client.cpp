// Client side implementation of UDP client-server model - side that will be "sending" the file.
// https://www.geeksforgeeks.org/cpp/udp-server-client-implementation-c/
#include <bits/stdc++.h>
#include <unistd.h>
#include <string.h>
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
#include <algorithm>

#include "zap_protocol.hpp"
#include "zap_cli.hpp"

static std::mutex g_coutMutex;

static bool send_chunk(const std::string& filename,
                        const std::string& serverIP,
                        int port,
                        uint64_t startOffset,
                        uint64_t endOffset,
                        uint32_t startSeq,
                        int threadIndex) {
    int sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed (thread)");
        return false;
    }

    struct sockaddr_in servaddr{};
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    if (inet_pton(AF_INET, serverIP.c_str(), &servaddr.sin_addr) <= 0) {
        std::cerr << "Invalid server IP address in thread " << threadIndex << std::endl;
        close(sockfd);
        return false;
    }

    if (connect(sockfd, reinterpret_cast<sockaddr*>(&servaddr), sizeof(servaddr)) < 0) {
        perror("connect (thread)");
        close(sockfd);
        return false;
    }

    std::ifstream file(filename, std::ios::binary);

    if (!file.is_open()) {
        std::cerr << "Thread " << threadIndex << " could not open file" << std::endl;
        close(sockfd);
        return false;
    }
    file.seekg(static_cast<std::streamoff>(startOffset));

    uint32_t sequence = startSeq;
    uint64_t bytesRemaining = endOffset - startOffset;

    while (bytesRemaining > 0) {
        Packet dataPacket{};
        dataPacket.type = PACKET_DATA;
        dataPacket.sequence = sequence;

        uint32_t toRead = static_cast<uint32_t>(std::min<uint64_t>(DATA_SIZE, bytesRemaining));
        file.read(dataPacket.data, toRead);
        std::streamsize bytesRead = file.gcount();
        if (bytesRead <= 0) break;

        dataPacket.data_length = static_cast<uint32_t>(bytesRead);

        bool sendFailed = false;

        for (int copy = 0; copy < 2; ++copy) {
            ssize_t bytesSent = send(sockfd, &dataPacket, sizeof(dataPacket), 0);
            if (bytesSent < 0) {
                perror("send (thread)");
                sendFailed = true;
                break;
            }
        }
        if (sendFailed) {
            break;
        }

        bytesRemaining -= static_cast<uint64_t>(bytesRead);
        sequence++;
    }

    {
        std::lock_guard<std::mutex> lock(g_coutMutex);
        std::cout << "Thread " << threadIndex << " sent packets ["
                  << startSeq << ", " << sequence << ")" << std::endl;
    }

    file.close();
    close(sockfd);
    return true;
}

// TODO: 
// notes: we should probs move the packet_start outside so that it must send an ack to confirm file can be sent. 
// Then while loop for as long as it takes to recv all of the packets. then send an fin message - might be able to do away with the 
// "PACKET_END" as in once all of the data from the file is sent and acked fully/
// PACKET_START will likely need to specify how large the size of the file to be sent is - so the receiver can know how many packets to expect,etc

static bool resend_packet(
    int sockfd,
    const std::string& filePath,
    uint32_t sequence,
    uint64_t fileSize)
{
    std::ifstream file(filePath, std::ios::binary);

    if (!file.is_open()) {
        std::cerr << "Failed to open file for retransmission." << std::endl;
        return false;
    }

    uint64_t offset =
        static_cast<uint64_t>(sequence) * DATA_SIZE;

    if (offset >= fileSize) {
        std::cerr << "Invalid retransmission sequence: "
                  << sequence << std::endl;
        return false;
    }

    file.seekg(offset);

    uint64_t remaining = fileSize - offset;
    uint32_t bytesToRead =
        static_cast<uint32_t>(
            std::min<uint64_t>(DATA_SIZE, remaining)
        );

    Packet packet{};
    packet.type = PACKET_DATA;
    packet.sequence = sequence;
    packet.data_length = bytesToRead;

    file.read(packet.data, bytesToRead);

    if (!file) {
        std::cerr << "Failed to read packet "
                  << sequence << std::endl;
        return false;
    }

    ssize_t bytesSent = send(
        sockfd,
        &packet,
        sizeof(packet),
        0
    );

    if (bytesSent < 0) {
        perror("send retransmission");
        return false;
    }

    std::cout << "Retransmitted packet "
              << sequence << std::endl;

    return true;
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
    std::ifstream inputFile(filename, std::ios::binary);

    if (!inputFile.is_open()){
        std::cerr<< "Could not open file: " << filename << std::endl;
        return EXIT_FAILURE;
    }

    std::cout << "Opened file: " << filename << std::endl;

     // figure out the file size so we can split it into chunks
    inputFile.seekg(0, std::ios::end);
    uint64_t fileSize = static_cast<uint64_t>(inputFile.tellg());
    inputFile.seekg(0, std::ios::beg);
    uint32_t totalPackets = static_cast<uint32_t>((fileSize + DATA_SIZE - 1) / DATA_SIZE);

    // Create UDP socket
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    std::cout << "UDP socket created."  << std::endl;

    memset(&servaddr, 0, sizeof(servaddr));

    // Fill server address info
    servaddr.sin_family = AF_INET;              // IPv4
    servaddr.sin_port   = htons(args.port);          // Server port

    // checkk server addr
    if (inet_pton(AF_INET, serverIP,  &servaddr.sin_addr ) <= 0) {
        std::cerr
            << "Invalid server IP address: "
            << serverIP
            << std::endl;
        close(sockfd);

        return EXIT_FAILURE;
    }

    // connect UDP socket tyo server
    if (connect(sockfd, reinterpret_cast<sockaddr*>(&servaddr), sizeof(servaddr)) < 0) {
        perror("connect");
        close(sockfd);
        return EXIT_FAILURE;
    }

    std::cout << "UDP socket connected to " << serverIP << ":" << args.port << std::endl;

    Packet packet{};

    packet.type = PACKET_START;
    packet.sequence = 0;

    std::string savePath = args.save_dir.empty() ? args.file_path : args.save_dir;
    packet.data_length = savePath.size();

    if (packet.data_length == 0 || packet.data_length >= FILENAME_SIZE) {// check filename 
        std::cerr << "Filename is too long." << std::endl;
        close(sockfd);
        return EXIT_FAILURE;
    }

    // copy to data
    memcpy( packet.data,  savePath.c_str(), packet.data_length );
    // send data
    ssize_t bytesSent = send(sockfd, &packet,  sizeof(packet), 0);

    if (bytesSent < 0) {
        perror("send");
        close(sockfd);
        return EXIT_FAILURE;
    }

    std::cout << "START packet sent." << std::endl;


    // send file 
    uint32_t packetsPerThread = (totalPackets + NUM_THREADS - 1) / NUM_THREADS;

    std::vector<std::thread> threads;
    for (int t = 0; t < NUM_THREADS; t++) {
        uint32_t startSeq = static_cast<uint32_t>(t) * packetsPerThread;
        if (startSeq >= totalPackets) break; 
        
    uint32_t endSeqExclusive = std::min(startSeq + packetsPerThread, totalPackets);
    uint64_t startOffset = static_cast<uint64_t>(startSeq) * DATA_SIZE;
    uint64_t endOffset = std::min(static_cast<uint64_t>(endSeqExclusive) * DATA_SIZE, fileSize);

    threads.emplace_back(send_chunk, filename, args.ip_address, args.port, startOffset, endOffset, startSeq, t);

    }

    for (auto& th : threads) {
        th.join();
    }
    
    std::cout << "All " << threads.size() << " sender threads finished (" << totalPackets << " packets total)." << std::endl;

    // end msg
    Packet endPacket{}; 

    endPacket.type = PACKET_END;
    endPacket.sequence = totalPackets;
    endPacket.data_length = 0;

    bool endSendFailed = false;
    for (int i = 0; i < 5; ++i) {
        bytesSent = send(sockfd, &endPacket, sizeof(endPacket), 0);
        if (bytesSent < 0) {
            perror("send END");
            endSendFailed = true;
            break;
        }
    }
    
    if (!endSendFailed) {
        std::cout << "END packet sent 5 times." << std::endl;
    }
    std::cout << "Waiting for NACK or COMPLETE..." << std::endl;

    while (true) {
        Packet response{};

        ssize_t n = recv(
            sockfd,
            &response,
            sizeof(response),
            0
        );

        if (n < 0) {
            perror("recv control packet");
            break;
        }

        if (response.type == PACKET_NACK) {
            if (response.data_length > DATA_SIZE ||
                response.data_length % sizeof(uint32_t) != 0) {
                std::cerr << "Invalid NACK packet." << std::endl;
                continue;
            }

            size_t count =
                response.data_length / sizeof(uint32_t);

            std::vector<uint32_t> missingSequences(count);

            memcpy(
                missingSequences.data(),
                response.data,
                response.data_length
            );

            std::cout << "Received NACK for "
                      << count
                      << " packets." << std::endl;

            for (uint32_t seq : missingSequences) {
                resend_packet(
                    sockfd,
                    args.file_path,
                    seq,
                    fileSize
                );
            }
        }
        else if (response.type == PACKET_COMPLETE) {
            std::cout << "Server confirmed file transfer complete."
                      << std::endl;
            break;
        }
        else {
            std::cerr << "Unexpected control packet type: "
                      << response.type << std::endl;
        }
    }

    close(sockfd);

    std::cout << "File transfer complete." << std::endl;

    return 0;
}