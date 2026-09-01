// Client side implementation of UDP client-server model
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


int main() {
    int sockfd;
    char* serverIP, filename;
    struct sockaddr_in servaddr;
    // TODO: Combine the work from isabel with this  to get the actrual ip and filenames. 
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
    servaddr.sin_port   = htons(SERVER_PORT);          // Server port

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

    std::cout << "UDP socket connected to " << serverIP << ":" << SERVER_PORT << std::endl;

    Packet packet{};

    packet.type = PACKET_START;
    packet.sequence = 0; // TODO

    packet.data_length = strlen(filename);


    if (packet.data_length >= FILENAME_SIZE) {// check filename 
        std::cerr << "Filename is too long." << std::endl;
        inputFile.close();
        close(sockfd);
        return EXIT_FAILURE;
    }

    // copy to data
    memcpy( packet.data,  filename, packet.data_length );
    // send data
    ssize_t bytesSent = send(sockfd, &packet,  sizeof(packet), 0);

    if (bytesSent < 0) {
        perror("send");
        inputFile.close();
        close(sockfd);
        return EXIT_FAILURE;
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
        bytesSent = send(sockfd, &dataPacket, sizeof(dataPacket), 0);

        if (bytesSent < 0) { 
            perror("send");
            break;
        }

        std::cout << "Sent packet "  << sequence << " ("  << bytesRead << " bytes)" << std::endl;
        sequence++;
    }


    // end msg
    Packet endPacket{}; 

    endPacket.type = PACKET_END;
    endPacket.sequence = sequence;
    endPacket.data_length = 0;


    bytesSent = send( sockfd, &endPacket, sizeof(endPacket), 0);

    if (bytesSent < 0) { 
        perror("send");
    }
    else{
        std::cout << "END packet sent." << std::endl;
    }

    // cleanup
    inputFile.close();
    close(sockfd);


    std::cout << "File transfer complete." << std::endl;

    return 0;
}