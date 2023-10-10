package main

import (
	"bufio"
	"context"
	pb "gRPC_Laborator1_NircaD/Producer/internal/pb/proto"
	"google.golang.org/grpc"
	"log"
	"os"
	"strings"
)

type EndpointsConstants struct {
	BrokerAddress     string
	SubscriberAddress string
}

var endpoints EndpointsConstants

func init() {
	endpoints.BrokerAddress = "127.0.0.1:5001"
	endpoints.SubscriberAddress = "127.0.0.1:0"
}

func main() {
	log.Println("Publisher")

	conn, err := grpc.Dial(endpoints.BrokerAddress, grpc.WithInsecure())
	if err != nil {
		log.Printf("Failed to connect to gRPC server: %v\n", err)
		return
	}
	defer conn.Close()

	client := pb.NewPublisherClient(conn)

	for {
		log.Print("Enter the topic: ")
		scanner := bufio.NewScanner(os.Stdin)
		scanner.Scan()
		topic := strings.ToLower(scanner.Text())

		log.Print("Enter content: ")
		scanner.Scan()
		content := scanner.Text()

		request := &pb.ProduceRequest{
			Topic:   topic,
			Message: content,
		}

		response, err := client.ProduceMessage(context.Background(), request)
		if err != nil {
			log.Printf("Error publishing the message: %v\n", err)
		} else {
			log.Printf("Publish Reply: %v\n", response.Status)
		}
	}
}
