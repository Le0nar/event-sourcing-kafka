package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/Le0nar/event-sourcing-kafka/write-service/internal/models"
	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
)

const (
    ProducerPort       = ":8080"
    KafkaServerAddress = "localhost:9092"
    KafkaTopic         = "payments"
)


func sendPayment(w http.ResponseWriter, r *http.Request, producer sarama.SyncProducer)  {
	var message models.Message

	err := json.NewDecoder(r.Body).Decode(&message)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	err = sendKafkaMessage(producer, message)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
}


func sendKafkaMessage(
	producer sarama.SyncProducer,
	message models.Message,
) error {
    messageJSON, err := json.Marshal(message)
    if err != nil {
        return fmt.Errorf("failed to marshal message: %w", err)
    }

    msg := &sarama.ProducerMessage{
        Topic: KafkaTopic,
        Key:   sarama.StringEncoder( message.UserId.String()),
        Value: sarama.StringEncoder(messageJSON),
    }

    _, _, err = producer.SendMessage(msg)
    return err
}


func setupProducer() (sarama.SyncProducer, error) {
    config := sarama.NewConfig()
    config.Producer.Return.Successes = true
    producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress}, config)
    if err != nil {
        return nil, fmt.Errorf("failed to setup producer: %w", err)
    }
    return producer, nil
}

func main() {
	producer, err := setupProducer()
	if err != nil {
		log.Fatalf("failed to initialize producer: %v", err)
	}
	defer producer.Close()


	router := chi.NewRouter()
	router.Use(middleware.Logger)
	router.Post("/payment", func (w http.ResponseWriter, r *http.Request)  {
		sendPayment(w, r, producer)
	})

	http.ListenAndServe(ProducerPort, router)
}
