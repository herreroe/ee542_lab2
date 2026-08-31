// Client side implementation of UDP client-server model
// https://www.geeksforgeeks.org/cpp/udp-server-client-implementation-c/
#include <bits/stdc++.h>
#include <unistd.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#define PORT     8080
#define MAXLINE  1024

int main() {
    int sockfd;
    char buffer[MAXLINE];
    const char *hello = "Hello from client";
    struct sockaddr_in servaddr;

    // Create UDP socket
    sockfd = socket(AF_INET, SOCK_DGRAM, 0);
    if (sockfd < 0) {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    memset(&servaddr, 0, sizeof(servaddr));

    // Fill server address info
    servaddr.sin_family = AF_INET;              // IPv4
    servaddr.sin_port   = htons(PORT);          // Server port
    servaddr.sin_addr.s_addr = inet_addr("127.0.0.1"); // Server IP

    socklen_t len = sizeof(servaddr);

    // Send message to server
    sendto(sockfd, hello, strlen(hello), MSG_CONFIRM,
           (const struct sockaddr *)&servaddr, sizeof(servaddr));
    printf("Hello message sent.\n");

    // Receive reply from server
    int n = recvfrom(sockfd, buffer, MAXLINE, MSG_WAITALL,
                     (struct sockaddr *)&servaddr, &len);

    buffer[n] = '\0';   // Null terminate received data
    printf("Server: %s\n", buffer);

    // Close socket
    close(sockfd);

    return 0;
}