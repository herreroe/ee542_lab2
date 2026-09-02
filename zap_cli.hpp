// zap client
// zap myfile.txt 192.168.1.1 -o /path/to/save
#ifndef ZAP_CLI_HPP
#define ZAP_CLI_HPP

#include <iostream>
#include <string>
#include <cstring>
#include <cstdlib>

using namespace std;

struct Args {
    string file_path; // required local file to send
    string ip_address; // required IP of the server to send to
    string save_dir; // optional empty means path to save was not specified
    int port = 8080; // default port
};

// prints how to use the program
// how linux prints the help message for a command
static void print_usage(const char* program_name) {
    cerr << "Usage: " << program_name << " <file> <ip_address> [-o <save_dir>] [-p <port>]" << endl;
}

// reads argc/argv and fills in out
// returns false if the arguments are invalid
// layout:
    // argv[0] = program name
    // argv[1] = file path
    // argv[2] = IP address
    // argv[3] = optional save directory
    // argv[4] = optional port
static bool parse_args(int argc, char* argv[], Args* out) {
    // need at least 3 arguments (program name, file path, IP address)
    if (argc < 3) {
        print_usage(argv[0]);
        return false;
    }

    out->file_path = argv[1];
    out->ip_address = argv[2];

    // start at index 3 because 1 and 2 are file path and IP address
    for (int i = 3; i < argc; i++) {
        // check for optional arguments
        if (strcmp(argv[i], "-o") == 0 ) {
            if (i + 1 < argc) {
                out->save_dir = argv[++i];
            } else {
                cerr << "Error: -o requires an argument" << endl;
                return false;
            }
            // check for -p option
        } else if (strcmp(argv[i], "-p") == 0) {
            if (i + 1 < argc) {
                out->port = atoi(argv[++i]);
            } else {
                cerr << "Error: -p requires an argument" << endl;
                return false;
            }
        } else {
            cerr << "Error: unknown option " << argv[i] << endl;
            print_usage(argv[0]);
            return false;
        }
    }

    return true;
}

#endif
