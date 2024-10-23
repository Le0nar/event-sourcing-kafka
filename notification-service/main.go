package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/Le0nar/event-sourcing-kafka/notification-service/internal/models"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/google/uuid"
)

const (
    ConsumerGroup      = "payments-group_2"
    ConsumerTopic      = "payments"
    ConsumerPort       = ":8082"
    KafkaServerAddress = "localhost:9092"
)


type Consumer struct {
}

func (*Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (consumer *Consumer) ConsumeClaim(
    sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    for msg := range claim.Messages() {
        var message models.Message
        err := json.Unmarshal(msg.Value, &message)
        if err != nil {
            log.Printf("failed to unmarshal message: %v", err)
            continue
        }
        
		fakeSendMessage(message.UserId, message.Status)

        sess.MarkMessage(msg, "")
    }
    return nil
}


func fakeSendMessage(userId uuid.UUID, status string)  {
	fmt.Printf("userId: %v\n", userId)
	fmt.Printf("status: %v\n", status)
}

func initializeConsumerGroup() (sarama.ConsumerGroup, error) {
    config := sarama.NewConfig()

    consumerGroup, err := sarama.NewConsumerGroup([]string{KafkaServerAddress}, ConsumerGroup, config)
    if err != nil {
        return nil, fmt.Errorf("failed to initialize consumer group: %w", err)
    }

    return consumerGroup, nil
}

func setupConsumerGroup(ctx context.Context) {
    consumerGroup, err := initializeConsumerGroup()
    if err != nil {
        log.Printf("initialization error: %v", err)
    }
    defer consumerGroup.Close()

    consumer := &Consumer{
    }

    for {
        err = consumerGroup.Consume(ctx, []string{ConsumerTopic}, consumer)
        if err != nil {
            log.Printf("error from consumer: %v", err)
        }
    }
}

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    go setupConsumerGroup(ctx)
    defer cancel()

	router := chi.NewRouter()
	router.Use(middleware.Logger)

	http.ListenAndServe(ConsumerPort, router)
}