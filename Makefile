all: zap client server

zap: zap.cpp
	g++ -std=c++17 -o zap zap.cpp

client: client.cpp
	g++ -std=c++17 -o client client.cpp

server: server.cpp
	g++ -std=c++17 -o server server.cpp

clean: 
	rm -f zap client server