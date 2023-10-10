package main

import (
	"context"
	"fmt"
	"gRPC_Laborator1_NircaD/Broker/internal/models"
	pb "gRPC_Laborator1_NircaD/Broker/internal/pb/proto"
	"google.golang.org/grpc"
	"log"
	"net"
	"sync"
	"time"
)

type Connection struct {
	Address string
	Topic   string
	Channel *grpc.ClientConn
}

type IMessageStorageService interface {
	Add(message *models.Message)
	GetNext() *models.Message
	IsEmpty() bool
}

type MessageStorageService struct {
	messages []*models.Message
	mu       sync.Mutex
}

func (s *MessageStorageService) Add(message *models.Message) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages = append(s.messages, message)
}

func (s *MessageStorageService) GetNext() *models.Message {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.messages) > 0 {
		message := s.messages[0]
		s.messages = s.messages[1:]
		return message
	}
	return nil
}

func (s *MessageStorageService) IsEmpty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.messages) == 0
}

type ConnectionStorageService struct {
	connections map[string][]*Connection
	mu          sync.Mutex
}

func NewConnectionStorageService() *ConnectionStorageService {
	return &ConnectionStorageService{
		connections: make(map[string][]*Connection),
	}
}

func (s *ConnectionStorageService) Add(connection *Connection) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.connections[connection.Topic] = append(s.connections[connection.Topic], connection)
}

func (s *ConnectionStorageService) GetConnectionsByTopic(topic string) []*Connection {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.connections[topic]
}

func (s *ConnectionStorageService) Remove(address string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for topic, connections := range s.connections {
		var updatedConnections []*Connection
		for _, conn := range connections {
			if conn.Address != address {
				updatedConnections = append(updatedConnections, conn)
			}
		}
		s.connections[topic] = updatedConnections
	}
}

type PublisherService struct {
	messageStorage IMessageStorageService
	pb.UnimplementedPublisherServer
}

func NewPublisherService(messageStorage IMessageStorageService) *PublisherService {
	return &PublisherService{
		messageStorage: messageStorage,
	}
}

func (s *PublisherService) PublishMessage(ctx context.Context, request *pb.ProduceRequest) (*pb.ProduceReply, error) {
	fmt.Printf("Received: %s %s\n", request.Topic, request.Message)

	message := models.Message{
		Topic:   request.Topic,
		Message: request.Message,
	}

	s.messageStorage.Add(&message)

	return &pb.ProduceReply{Status: true}, nil
}

type SenderWorker struct {
	messageStorage    IMessageStorageService
	connectionStorage *ConnectionStorageService
}

func NewSenderWorker(messageStorage IMessageStorageService, connectionStorage *ConnectionStorageService) *SenderWorker {
	return &SenderWorker{
		messageStorage:    messageStorage,
		connectionStorage: connectionStorage,
	}
}

func (w *SenderWorker) Start() {
	ticker := time.NewTicker(2 * time.Second)

	for range ticker.C {
		if w.messageStorage == nil {
			fmt.Println("Message storage is nil.")
			continue
		}

		if w.connectionStorage == nil {
			fmt.Println("Connection storage is nil.")
			continue
		}

		for !w.messageStorage.IsEmpty() {
			message := w.messageStorage.GetNext()

			if message != nil {
				if w.connectionStorage == nil {
					fmt.Println("Connection storage is nil.")
					continue
				}

				connections := w.connectionStorage.GetConnectionsByTopic(message.Topic)

				if connections == nil {
					fmt.Printf("No connections found for topic: %s\n", message.Topic)
					continue
				}

				for _, connection := range connections {
					if connection.Channel == nil {
						fmt.Printf("Channel for connection %s is nil. Establishing channel...\n", connection.Address)
						conn, err := grpc.Dial(connection.Address, grpc.WithInsecure())
						if err != nil {
							fmt.Printf("Error establishing channel for connection %s: %v\n", connection.Address, err)
							continue
						}
						connection.Channel = conn
					}

					_, err := pb.NewNotifierClient(connection.Channel).Notify(context.Background(), &pb.NotifyRequest{Message: message.Message})

					if err == nil {
						fmt.Printf("Notified subscriber %s with %s\n", connection.Address, message.Message)
					} else {
						fmt.Printf("Error notifying subscriber %s. %v\n", connection.Address, err)
						w.connectionStorage.Remove(connection.Address)
					}
				}
			}
		}
	}
}

type SubscriberService struct {
	connectionStorage *ConnectionStorageService
	pb.UnimplementedSubscriberServer
}

func NewSubscriberService(connectionStorage *ConnectionStorageService) *SubscriberService {
	return &SubscriberService{
		connectionStorage: connectionStorage,
	}
}

func (s *SubscriberService) Subscribe(ctx context.Context, request *pb.SubscribeRequest) (*pb.SubscribeResponse, error) {
	fmt.Printf("New client trying to subscribe: %s %s\n", request.Address, request.Topic)

	connection := &Connection{
		Address: request.Address,
		Topic:   request.Topic,
		Channel: nil,
	}

	s.connectionStorage.Add(connection)

	return &pb.SubscribeResponse{Status: true}, nil
}

func main() {
	log.Println("Broker")

	messageStorage := &MessageStorageService{
		messages: make([]*models.Message, 0),
	}

	connectionStorage := NewConnectionStorageService()

	publisherService := NewPublisherService(messageStorage)
	subscriberService := NewSubscriberService(connectionStorage)
	senderWorker := NewSenderWorker(messageStorage, connectionStorage)

	go senderWorker.Start()

	grpcServer := grpc.NewServer()

	pb.RegisterPublisherServer(grpcServer, publisherService)
	pb.RegisterSubscriberServer(grpcServer, subscriberService)

	address := "127.0.0.1:5001"

	listener, err := net.Listen("tcp", address)
	if err != nil {
		log.Fatalf("Failed to listen: %v\n", err)
	}

	fmt.Printf("Starting gRPC server on %s...\n", address)
	if err := grpcServer.Serve(listener); err != nil {
		log.Fatalf("Failed to serve: %v\n", err)
	}
}
