// Server side implementation of UDP client-server model 
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

#include "zap_protocol.hpp"


  
// Driver code 
int main() { 
    int sockfd; 
    struct sockaddr_in servaddr, cliaddr; 
      
    // Creating socket file descriptor 
    if ( (sockfd = socket(AF_INET, SOCK_DGRAM, 0)) < 0 ) { 
        perror("socket creation failed"); 
        exit(EXIT_FAILURE); 
    } 
    std::cout << "UDP socket created." << std::endl;
  
    memset(&servaddr, 0, sizeof(servaddr)); 
    memset(&cliaddr, 0, sizeof(cliaddr)); 
      
    // Filling server information 
    servaddr.sin_family    = AF_INET; // IPv4 
    servaddr.sin_addr.s_addr = INADDR_ANY; 
    servaddr.sin_port = htons(SERVER_PORT); 
      
    // Bind the socket with the server address 
    if ( bind(sockfd, (const struct sockaddr *)&servaddr,  
            sizeof(servaddr)) < 0 ) 
    { 
        perror("bind failed"); 
        exit(EXIT_FAILURE); 
    } 
     
    std::cout<< "Server listening on UDP port " << SERVER_PORT << std::endl;
        
    Packet packet{};
    sockaddr_in clientAddress{};
    socklen_t clientAddressLength = sizeof(clientAddress);

    // std::ofstream outputFile;
    FILE * outputFile = NULL;

    bool receivingFile = false;


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
            perror("recvfrom");
            break;
        }

        // Start msg
        if (packet.type == PACKET_START) {
            std::cout << "Received START packet." << std::endl;

            std::string filename(packet.data, packet.data_length);

            outputFile = fopen(filename.c_str(), "wb");
            if (outputFile == NULL) {
                perror("Failed to open output file");
                receivingFile = false;
            } else {
                receivingFile = true;
            }

            std::cout
                << "Opened received_file for writing."
                << std::endl;
        }
        // paylaod msg 
        else if (packet.type == PACKET_DATA) {
            if (!receivingFile) {
                std::cerr<< "Received DATA before START." << std::endl;
                continue;

            }

            size_t bytesWritten = fwrite(packet.data, 1, packet.data_length, outputFile);
            if (bytesWritten != packet.data_length) {
                std::cerr << "Failed to write all data" << std::endl;
                fclose(outputFile);
                outputFile = NULL;
                receivingFile = false;
                break;
            }

            std::cout << "Received packet " << packet.sequence << " (" << packet.data_length  << " bytes)" << std::endl;
        }
        // end msg
        else if (packet.type == PACKET_END) {
            std::cout << "Received END packet." << std::endl;

           // save stuff
            if (outputFile != NULL) {
                fclose(outputFile);
                outputFile = NULL;
            }
            receivingFile = false;
            std::cout << "File transfer complete."  << std::endl;
            break;
        }
        else {
            std::cerr
                << "Unknown packet type: "
                << packet.type
                << std::endl;
        }
    }

    if (outputFile != NULL) {
        fclose(outputFile);
    }

    close(sockfd);
      
    return 0; 
}