package main

import (
	"bufio"
	"context"
	"fmt"
	pb "gRPC_Laborator1_NircaD/Consumer/internal/pb/proto"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"strings"
)

type notificationServer struct {
	pb.UnimplementedNotifierServer
}

func (s *notificationServer) Notify(ctx context.Context, request *pb.NotifyRequest) (*pb.NotifyResponse, error) {
	log.Printf("Received notification: %v\n", request.Message)
	return &pb.NotifyResponse{Status: true}, nil
}

func findAvailablePort() string {
	listen, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		log.Fatalf("Failed to find available port: %v\n", err)
	}

	defer listen.Close()
	_, portStr, _ := net.SplitHostPort(listen.Addr().String())
	return portStr
}

func startNotificationServer() string {
	port := findAvailablePort()
	address := fmt.Sprintf("127.0.0.1:%s", port)

	listen, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen: %v\n", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterNotifierServer(grpcServer, &notificationServer{})

	log.Printf("Receiver is listening on %s\n", address)

	go func() {
		if err := grpcServer.Serve(listen); err != nil {
			log.Printf("Failed to serve: %v\n", err)
		}
	}()

	return address
}

func startSubscriber(brokerAddress, topic string) {
	conn, err := grpc.Dial(brokerAddress, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect to Broker: %v\n", err)
		return
	}
	defer conn.Close()

	client := pb.NewSubscriberClient(conn)

	request := &pb.SubscribeRequest{
		Topic:   topic,
		Address: startNotificationServer(),
	}

	response, err := client.Subscribe(context.Background(), request)
	if err != nil {
		log.Printf("Error subscribing to the topic: %v\n", err)
		return
	}

	log.Printf("Subscribe Reply: %v\n", response.Status)

}

func main() {
	log.Println("Receiver")

	log.Printf("Enter the topic you want to subscribe to: ")
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	topic := strings.ToLower(scanner.Text())

	startSubscriber("127.0.0.1:5001", topic)
	select {}
}
