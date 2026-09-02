all: zap server

zap: client.cpp zap_protocol.hpp zap_cli.hpp
	g++ -std=c++17 -o zap client.cpp
client: client.cpp zap_protocol.hpp
	g++ -std=c++17 -o client client.cpp
server: server.cpp zap_protocol.hpp
	g++ -std=c++17 -o server server.cpp

clean: 
	rm -f zap client server