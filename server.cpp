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

    // added bc no flow control or ACKs yet
    int rcvbuf = 32 * 1024 * 1024;
    setsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
  
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
    std::unordered_set<uint32_t> receivedSequences;

 // notes: we should probs move the packet_start outside so that it must send an ack to confirm file can be sent. 
    // Then while loop for as long as it takes to recv all of the packets. then send an fin message - might be able to do away with the 
    // "PACKET_END" as in once all of the data from the file is sent and acked fully/
    // PACKET_START will likely need to specify how large the size of the file to be sent is - so the receiver can know how many packets to expect,etc
    
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

            receivedSequences.clear();
            
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

            // duplicate packet check
            // if sequence number is already in the set, drop this copy and keep original
            if (receivedSequences.find(packet.sequence) != receivedSequences.end()) {
                std::cout << "Received duplicate packet " << packet.sequence << " received - dropping (original kept)" << std::endl;
                continue;
            }

            // order by sequence number
            // seek where the chunk belongs in the file before writing it
            long offset = static_cast<long>(packet.sequence) * static_cast<long>(DATA_SIZE);
            if (fseek(outputFile, offset, SEEK_SET) != 0) {
                perror("fseek");
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

            receivedSequences.insert(packet.sequence);

            if (packet.sequence %100 == 0) {
                std::cout << "Received packet " << packet.sequence << " (" << packet.data_length  << " bytes)" << std::endl;
            }
        }
        // end msg
        else if (packet.type == PACKET_END) {
            std::cout << "Received END packet." << std::endl;

           // save stuff
           uint32_t totalPackets = packet.sequence;
           std::vector<uint32_t> missingPackets;
           for (uint32_t seq = 0; seq < totalPackets; ++seq) {
            if (receivedSequences.find(seq) == receivedSequences.end()) {
                missingPackets.push_back(seq);
            }
        }

           std::cout << "Expected DATA packets: "
                     << totalPackets << std::endl;

           std::cout << "Received DATA packets: "
                     << receivedSequences.size() << std::endl;

           std::cout << "Missing DATA packets: "
                     << missingPackets.size() << std::endl;

           if (!missingPackets.empty()) {
               std::cout << "Missing sequence numbers: ";

               for (uint32_t seq : missingPackets) {
                std::cout << seq << " ";
                }

                std::cout << std::endl;
            }
            
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