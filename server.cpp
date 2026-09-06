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
#include <fcntl.h>      // open()
  
#include <iostream>
#include <fstream>
#include <cstring>
#include <cstdlib>

#include <thread>
#include <vector>
#include <mutex>
#include <atomic>
#include <chrono>

#include "zap_protocol.hpp"

// DIAGNOSTIC TOGGLE: flip to 0 once you've root-caused the drops, to remove
// the recvmsg/cmsg overhead from the hot path.
#define ZAP_TRACK_KERNEL_DROPS 1

struct ReceiverState {
    // Guards ONLY receivedSequences + the one-time file-open transition.
    // Actual disk writes no longer take this lock (see pwrite in receiver_thread).
    std::mutex fileMutex;
    int outputFd = -1;
    bool receivingFile = false;
    std::unordered_set<uint32_t> receivedSequences;

    std::mutex clientMutex;
    sockaddr_in clientAddress{};
    bool clientAddressKnown = false;

    std::atomic<bool> stopRequested{false};
    std::atomic<uint32_t> finalTotalPackets{0};

    // --- instrumentation ---
    std::atomic<uint64_t> packetsWritten{0};
    std::atomic<uint64_t> bytesWritten{0};
    std::atomic<uint64_t> duplicatesDropped{0};
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

    // DIAGNOSTIC: confirm what buffer size the kernel actually granted.
    // Linux silently clamps requests above net.core.rmem_max, so "32MB" in
    // code doesn't mean 32MB in practice unless that sysctl allows it.
    {
        int actualRcvbuf = 0;
        socklen_t optlen = sizeof(actualRcvbuf);
        if (getsockopt(sockfd, SOL_SOCKET, SO_RCVBUF, &actualRcvbuf, &optlen) == 0) {
            std::cout << "[thread " << threadIndex << "] SO_RCVBUF actually granted: "
                      << actualRcvbuf << " bytes (requested " << rcvbuf << ")" << std::endl;
        }
    }

#if ZAP_TRACK_KERNEL_DROPS
    // SO_RXQ_OVFL gives us the kernel's own cumulative "datagrams dropped
    // because the receive buffer was full" counter, delivered as ancillary
    // data on every recvmsg(). This tells us definitively whether loss is
    // happening BELOW our application (kernel buffer overflow -> raise
    // SO_RCVBUF / drain faster) or somewhere else (e.g. real network loss,
    // or a bug). Without this you're guessing from retransmit counts alone.
    int rxqOvflOpt = 1;
    setsockopt(sockfd, SOL_SOCKET, SO_RXQ_OVFL, &rxqOvflOpt, sizeof(rxqOvflOpt));
    uint32_t lastKernelDropCount = 0;
#endif

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
    int consecutiveTimeoutsAfterStop = 0;

#if ZAP_TRACK_KERNEL_DROPS
    char cmsgBuf[CMSG_SPACE(sizeof(uint32_t))];
#endif

    while (true)
    {
#if ZAP_TRACK_KERNEL_DROPS
        struct iovec iov{};
        iov.iov_base = &packet;
        iov.iov_len = sizeof(packet);

        struct msghdr msg{};
        msg.msg_name = &clientAddress;
        msg.msg_namelen = sizeof(clientAddress); // recvmsg overwrites this each call
        msg.msg_iov = &iov;
        msg.msg_iovlen = 1;
        msg.msg_control = cmsgBuf;
        msg.msg_controllen = sizeof(cmsgBuf);

        ssize_t n = recvmsg(sockfd, &msg, 0);

        if (n >= 0) {
            for (struct cmsghdr* cmsg = CMSG_FIRSTHDR(&msg); cmsg != nullptr; cmsg = CMSG_NXTHDR(&msg, cmsg)) {
                if (cmsg->cmsg_level == SOL_SOCKET && cmsg->cmsg_type == SO_RXQ_OVFL) {
                    uint32_t kernelDropCount = 0;
                    memcpy(&kernelDropCount, CMSG_DATA(cmsg), sizeof(kernelDropCount));
                    if (kernelDropCount != lastKernelDropCount) {
                        std::cout << "[thread " << threadIndex << "] *** KERNEL DROPPED "
                                  << (kernelDropCount - lastKernelDropCount)
                                  << " datagrams (cumulative " << kernelDropCount
                                  << ") -- receive buffer overflowed, app wasn't draining fast enough ***"
                                  << std::endl;
                        lastKernelDropCount = kernelDropCount;
                    }
                }
            }
        }
#else
        socklen_t clientAddressLength = sizeof(clientAddress);
        ssize_t n = recvfrom(
            sockfd,
            &packet,
            sizeof(packet),
            0,
            reinterpret_cast<sockaddr*>(&clientAddress),
            &clientAddressLength
        );
#endif

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
            // Still needs a lock: this is a one-time state transition
            // (open the file, clear the set) that both threads could race on.
            std::lock_guard<std::mutex> lock(state.fileMutex);
            if (state.outputFd == -1) {
                std::cout << "[thread " << threadIndex << "] Received START packet." << std::endl;
                state.receivedSequences.clear();
                std::string filename(packet.data, packet.data_length);

                state.outputFd = open(filename.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
                if (state.outputFd == -1) {
                    perror("Failed to open output file");
                    state.receivingFile = false;
                } else {
                    state.receivingFile = true;
                    std::cout << "[thread " << threadIndex << "] Opened " << filename << " for writing." << std::endl;
                }
            }

        }
        
        // paylaod msg 
        else if (packet.type == PACKET_DATA) {
            if (!state.receivingFile) {
                std::cerr << "[thread " << threadIndex << "] Received DATA before START." << std::endl;
                continue;
            }

            // duplicate packet check
            // if sequence number is already in the set, drop this copy and keep original
            // NOTE: this is still the only mutex on the hot path, and it's now
            // only guarding an unordered_set operation (fast, no I/O) instead
            // of wrapping a disk write. Watch this contention with the timer
            // below -- if it's still high, shard receivedSequences by thread.
            bool alreadyReceived;
            {
                auto lockStart = std::chrono::steady_clock::now();
                std::lock_guard<std::mutex> lock(state.fileMutex);
                auto lockWaitUs = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::steady_clock::now() - lockStart).count();
                if (lockWaitUs > 2000) { // only log meaningfully long stalls
                    std::cout << "[thread " << threadIndex << "] mutex wait: "
                              << lockWaitUs << " us" << std::endl;
                }

                alreadyReceived = state.receivedSequences.count(packet.sequence) != 0;
                if (!alreadyReceived) {
                    state.receivedSequences.insert(packet.sequence);
                }
            }

            if (alreadyReceived) {
                state.duplicatesDropped.fetch_add(1, std::memory_order_relaxed);
                continue;
            }

            // KEY CHANGE: pwrite() takes an explicit offset and does not touch
            // a shared file cursor, so concurrent threads can write to
            // different regions of the same fd with NO lock and no risk of
            // corruption (this is the POSIX-guaranteed behavior pwrite exists
            // for). This removes the serialization point that was previously
            // stalling each thread's recvfrom loop during disk I/O -- which
            // is the most likely source of your kernel-buffer packet drops.
            off_t offset = static_cast<off_t>(packet.sequence) * static_cast<off_t>(DATA_SIZE);
            ssize_t bytesWritten = pwrite(state.outputFd, packet.data, packet.data_length, offset);
            if (bytesWritten != static_cast<ssize_t>(packet.data_length)) {
                perror("pwrite");
                std::cerr << "[thread " << threadIndex << "] Failed to write all data for seq "
                          << packet.sequence << std::endl;
                // Un-mark it so a retransmission can still fill this hole in.
                std::lock_guard<std::mutex> lock(state.fileMutex);
                state.receivedSequences.erase(packet.sequence);
                continue;
            }

            state.packetsWritten.fetch_add(1, std::memory_order_relaxed);
            state.bytesWritten.fetch_add(bytesWritten, std::memory_order_relaxed);

            if (packet.sequence %1000 == 0) { // occasional
                std::cout << "[thread " << threadIndex << "] Received packets " << packet.sequence << " (" << packet.data_length  << " bytes)" << std::endl;
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
    std::cout << "[instrumentation] packets written: " << state.packetsWritten.load()
               << ", bytes written: " << state.bytesWritten.load()
               << ", duplicate packets dropped: " << state.duplicatesDropped.load() << std::endl;

    if (state.outputFd == -1) {
        std::cerr << "Output file is not available." << std::endl;
        return EXIT_FAILURE;
    }

    sockaddr_in clientAddr{};

    {
        std::lock_guard<std::mutex> lock(state.clientMutex);

        if (!state.clientAddressKnown) {
            std::cerr
                << "Cannot repair transfer: client address is unknown."
                << std::endl;

            close(state.outputFd);
            return EXIT_FAILURE;
        }

        clientAddr = state.clientAddress;
    }

    // Create one socket for the repair phase.
    // This socket sends NACKs and receives retransmitted DATA.
    int repairSock = socket(AF_INET, SOCK_DGRAM, 0);

    if (repairSock < 0) {
        perror("socket creation failed (repair)");
        close(state.outputFd);
        return EXIT_FAILURE;
    }

    sockaddr_in repairAddress{};
    repairAddress.sin_family = AF_INET;
    repairAddress.sin_addr.s_addr = INADDR_ANY;
    repairAddress.sin_port = htons(SERVER_PORT);

    if (bind( repairSock, reinterpret_cast<sockaddr*>(&repairAddress), sizeof(repairAddress)) < 0) {
        perror("bind failed (repair)");
        close(repairSock);
        close(state.outputFd);
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
            if (state.receivedSequences.find(seq) == state.receivedSequences.end()) {
                missingPackets.push_back(seq);
            }
        }

        std::cout << "Received DATA packets: " << state.receivedSequences.size() << std::endl;
        std::cout << "Missing DATA packets: " << missingPackets.size() << std::endl;

        // Everything has arrived
        if (missingPackets.empty()) {
            auto finalRecvTime = std::chrono::high_resolution_clock::now();
            auto duration_us = std::chrono::duration_cast<std::chrono::microseconds>(finalRecvTime.time_since_epoch()).count();
            std::cout << "Timestamp (Final bit received): " << duration_us << " us (epoch)" << std::endl;            
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

            // Already received this sequence
            if (state.receivedSequences.find(repairPacket.sequence) != state.receivedSequences.end()) {
                continue;
            }

            off_t offset = static_cast<off_t>(repairPacket.sequence) * static_cast<off_t>(DATA_SIZE);

            ssize_t bytesWritten = pwrite(state.outputFd, repairPacket.data, repairPacket.data_length, offset);

            if (bytesWritten != static_cast<ssize_t>(repairPacket.data_length)) {
                perror("pwrite repair");
                std::cerr << "Failed to write retransmitted packet " << repairPacket.sequence << std::endl;
                continue;
            }

            state.receivedSequences.insert(
                repairPacket.sequence
            );
        }
    }

    close(repairSock);

    if (state.outputFd != -1) {
        fsync(state.outputFd);
        close(state.outputFd);
    }

    return 0;
}