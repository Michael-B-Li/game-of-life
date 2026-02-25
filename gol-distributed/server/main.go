package main

import (
	"flag"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strings"
)

// Starts the Gol worker rpc server on specified port
func main() {
	port := flag.String("port", "8030", "Port to listen on")
	var serverListStr string
	flag.StringVar(&serverListStr, "servers", "", "A comma-separated list of worker addresses (e.g., '1.1.1.1:80,2.2.2.2:80')")
	flag.Parse()

	if serverListStr == "" {
		fmt.Println("Error: -servers flag is required.")
		os.Exit(1)
	}
	serverAddresses := strings.Split(serverListStr, ",")
	fmt.Println("Registering worker servers:", serverAddresses)

	broker := &Broker{
		workerAddresses: serverAddresses,
		resumeCh:        make(chan struct{}),
		shutdownCh:      make(chan struct{}),
		doneCh:          make(chan struct{}),
	}

	fmt.Println("GOL Server starting on port", *port)
	err := startGolServer(*port, broker)
	if err != nil {
		panic(err)
	}

}

// startGolServer starts the RPC server for the GOL worker
func startGolServer(port string, b *Broker) error {
	// Register GolWorker type so the methods can be called via the RPC
	rpc.Register(b)
	// Listen for TCP connections on the port
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		return err
	}
	defer listener.Close()
	// Accept and handle RPC connections
	rpc.Accept(listener)
	return nil
}
