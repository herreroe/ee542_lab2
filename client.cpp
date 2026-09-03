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

#include "zap_protocol.hpp"
#include "zap_cli.hpp"
// TODO: 
// notes: we should probs move the packet_start outside so that it must send an ack to confirm file can be sent. 
// Then while loop for as long as it takes to recv all of the packets. then send an fin message - might be able to do away with the 
// "PACKET_END" as in once all of the data from the file is sent and acked fully/
// PACKET_START will likely need to specify how large the size of the file to be sent is - so the receiver can know how many packets to expect,etc

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

    // Create UDP socket
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed");
        inputFile.close(); // close file here 
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

        inputFile.close();
        close(sockfd);

        return EXIT_FAILURE;
    }

    // connect UDP socket tyo server
    if (connect(sockfd, reinterpret_cast<sockaddr*>(&servaddr), sizeof(servaddr)) < 0) {
        perror("connect");
        inputFile.close();
        close(sockfd);
        return EXIT_FAILURE;
    }

    std::cout << "UDP socket connected to " << serverIP << ":" << args.port << std::endl;

    // SEND START
    Packet packet{};
    packet.type = PACKET_START;
    packet.sequence = 0;

    std::string savePath = args.save_dir.empty() ? args.file_path : args.save_dir;
    packet.data_length = savePath.size();

    if (packet.data_length == 0 || packet.data_length >= FILENAME_SIZE) {// check filename 
        std::cerr << "Filename is too long." << std::endl;
        inputFile.close();
        close(sockfd);
        return EXIT_FAILURE;
    }

    // copy to data
    memcpy( packet.data,  savePath.c_str(), packet.data_length );
    // send data
    ssize_t bytesSent;
    for(int i = 0; i < 5; i++) {  // send 5 times
        bytesSent = send(sockfd, &packet,  sizeof(packet), 0);

        if (bytesSent < 0) {
            perror("send");
            inputFile.close();
            close(sockfd);
            return EXIT_FAILURE;
        }
    }

    std::cout << "START packet sent." << std::endl;


    // send file 
    uint32_t sequence = 0;

    while (true)
    {
        Packet dataPacket{};

        dataPacket.type = PACKET_DATA; // #2
        dataPacket.sequence = sequence;

        // Read up to DATA_SIZE bytes of the file to be transferred
        inputFile.read( dataPacket.data, DATA_SIZE );
        std::streamsize bytesRead = inputFile.gcount();

        // No more data
        if (bytesRead == 0) { break; }

        dataPacket.data_length = static_cast<uint32_t>(bytesRead);

        // Send packet
        for(int i = 0; i < 10; i++) {
            bytesSent = send(sockfd, &dataPacket, sizeof(dataPacket), 0);

            if (bytesSent < 0) { 
                perror("send");
                break;
            }
        }
        // bytesSent = send(sockfd, &dataPacket, sizeof(dataPacket), 0);

        // if (bytesSent < 0) { 
        //     perror("send");
        //     break;
        // }

       if (sequence %100 == 0) {
            std::cout << "Sent packet "  << sequence << " ("  << bytesRead << " bytes)" << std::endl;
        }
        sequence++;
    }


    // SEND END msg
    Packet endPacket{}; 
    endPacket.type = PACKET_END;
    endPacket.sequence = sequence;
    endPacket.data_length = 0;

    for(int i = 0; i < 5; i++) {
        bytesSent = send( sockfd, &endPacket, sizeof(endPacket), 0);
        if (bytesSent < 0) { 
        perror("send");
        }
    }

    std::cout << "END packet sent." << std::endl;


    // cleanup
    inputFile.close();
    close(sockfd);


    std::cout << "File transfer complete." << std::endl;

    return 0;
}