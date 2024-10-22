package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/IBM/sarama"
	"github.com/Le0nar/event-sourcing-kafka/read-service/internal/models"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
	"github.com/go-chi/render"
)

const (
    ConsumerGroup      = "payments-group"
    ConsumerTopic      = "payments"
    ConsumerPort       = ":8081"
    KafkaServerAddress = "localhost:9092"
)

type UserPayments map[string][]models.Message


type PaymentStore struct {
    data UserPayments
    mu   sync.RWMutex
}

func (ns *PaymentStore) Add(userID string,
    notification models.Message) {
    ns.mu.Lock()
    defer ns.mu.Unlock()
    ns.data[userID] = append(ns.data[userID], notification)
}

func (ns *PaymentStore) Get(userID string) []models.Message {
    ns.mu.RLock()
    defer ns.mu.RUnlock()
    return ns.data[userID]
}

type Consumer struct {
    store *PaymentStore
}

func (*Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (consumer *Consumer) ConsumeClaim(
    sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
    for msg := range claim.Messages() {
        userID := string(msg.Key)
        var notification models.Message
        err := json.Unmarshal(msg.Value, &notification)
        if err != nil {
            log.Printf("failed to unmarshal notification: %v", err)
            continue
        }
        consumer.store.Add(userID, notification)
        sess.MarkMessage(msg, "")
    }
    return nil
}

func initializeConsumerGroup() (sarama.ConsumerGroup, error) {
    config := sarama.NewConfig()

    consumerGroup, err := sarama.NewConsumerGroup([]string{KafkaServerAddress}, ConsumerGroup, config)
    if err != nil {
        return nil, fmt.Errorf("failed to initialize consumer group: %w", err)
    }

    return consumerGroup, nil
}

func setupConsumerGroup(ctx context.Context, store *PaymentStore) {
    consumerGroup, err := initializeConsumerGroup()
    if err != nil {
        log.Printf("initialization error: %v", err)
    }
    defer consumerGroup.Close()

    consumer := &Consumer{
        store: store,
    }

    for {
        err = consumerGroup.Consume(ctx, []string{ConsumerTopic}, consumer)
        if err != nil {
            log.Printf("error from consumer: %v", err)
        }
    }
}


func getPayments(w http.ResponseWriter, r *http.Request, store *PaymentStore)  {
	userID := chi.URLParam(r, "userID")


    messages := store.Get(userID)

	render.JSON(w, r, messages)
}

func main() {
	store := &PaymentStore{
        data: make(UserPayments),
    }

    ctx, cancel := context.WithCancel(context.Background())
    go setupConsumerGroup(ctx, store)
    defer cancel()

	router := chi.NewRouter()
	router.Use(middleware.Logger)

	router.Get("/payment/{userID}", func (w http.ResponseWriter, r *http.Request)  {
		getPayments(w, r, store)
	})

	http.ListenAndServe(":8081", router)
}