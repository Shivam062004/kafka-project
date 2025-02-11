package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"kafka-project/pkg/models"
	"log"
	"net/http"
	"sync"

	"github.com/IBM/sarama"
	"github.com/gorilla/mux"
)

const (
	ConsumerGroup      = "notifications-group"
	ConsumerTopic      = "notifications"
	ConsumerPort       = ":8081"
	KafkaServerAddress = "localhost:9092"
)

var ErrNoMessagesFound = errors.New("no messages found")

type UserNotifications map[string][]models.Notification

type NotificationResponse struct {
	Notification []models.Notification `json:"notification"`
}

type NotificationStore struct {
	data UserNotifications
	mu   sync.RWMutex
}

type Consumer struct {
	store *NotificationStore
}

func (ns *NotificationStore) Add(userID string,
	notification models.Notification) {
	ns.mu.Lock()
	defer ns.mu.Unlock()
	ns.data[userID] = append(ns.data[userID], notification)
}

func (ns *NotificationStore) Get(userID string) []models.Notification {
	ns.mu.RLock()
	defer ns.mu.RUnlock()
	return ns.data[userID]
}

func (*Consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*Consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (consumer *Consumer) ConsumeClaim(
	sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		userID := string(msg.Key)
		var notification models.Notification
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

func InitializeConsumerGroup() (sarama.ConsumerGroup, error) {
	config := sarama.NewConfig()

	consumerGroup, err := sarama.NewConsumerGroup([]string{KafkaServerAddress}, ConsumerGroup, config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize consumer group: %w", err)
	}

	return consumerGroup, nil
}

func setupConsumerGroup(ctx context.Context, store *NotificationStore) {
	consumerGroup, err := InitializeConsumerGroup()
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
		if ctx.Err() != nil {
			return
		}
	}
}

func HandleNotification(w http.ResponseWriter, r *http.Request, store *NotificationStore) {
	id := mux.Vars(r)["id"]
	notes := store.Get(id)
	WriteJSON(w, http.StatusOK, notes)
}


func main() {
    store := &NotificationStore{
        data: make(UserNotifications),
    }

    ctx, cancel := context.WithCancel(context.Background())
    go setupConsumerGroup(ctx, store)
    defer cancel()

	router := mux.NewRouter()
	router.HandleFunc("/notfication/{id}", CreateHTTPHandleFunc(HandleNotification, store))
    fmt.Printf("Kafka CONSUMER (Group: %s) 👥📥 "+ "started at http://localhost%s\n", ConsumerGroup, ConsumerPort)
	http.ListenAndServe(":9401", router)
}


