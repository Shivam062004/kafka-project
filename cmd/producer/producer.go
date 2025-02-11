package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"kafka-project/pkg/models"
	"log"
	"net/http"
	"strconv"

	"github.com/IBM/sarama"
	"github.com/gorilla/mux"
)

var ErrUserNotFound = errors.New("user not found")

const (
	ProducerPort       = ":8080"
	KafkaServerAddress = "localhost:9092"
	KafkaTopic         = "notifications"
)

func FindUser(id int, users []models.User) (models.User, error) {
	for _, value := range users {
		if value.ID == id {
			return value, nil
		}
	}
	return models.User{}, ErrUserNotFound
}

func SendKafkaMessage(producer sarama.SyncProducer, req *models.Request, users []models.User) error {
	toUser, err := FindUser(req.ToID, users)
	if err != nil {
		return ErrUserNotFound
	}
	fromUser, err := FindUser(req.FromID, users)
	if err != nil {
		return ErrUserNotFound
	}
	notification := models.Notification{
		From:    fromUser,
		To:      toUser,
		Message: req.Message,
	}
	bytesOfNotification, err := json.Marshal(notification)
	if err != nil {
		return err
	}
	msg := &sarama.ProducerMessage{
		Topic: KafkaTopic,
		Key:   sarama.StringEncoder(strconv.Itoa(toUser.ID)),
		Value: sarama.StringEncoder(bytesOfNotification),
	}

	_, _, err = producer.SendMessage(msg)
	return err
}

func HandleSendMessage(w http.ResponseWriter, r *http.Request, users []models.User, producer sarama.SyncProducer) error {
	req := &models.Request{}
	err := json.NewDecoder(r.Body).Decode(req)
	defer r.Body.Close()
	if err != nil {
		return err
	}
	err2 := SendKafkaMessage(producer, req, users)
	if err2 != nil {
		return err2
	}

	WriteJSON(w, http.StatusOK, models.Response{
		Message: "message send sucessfully",
	})
	return nil
}

func SetupProducer() (sarama.SyncProducer, error) {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer([]string{KafkaServerAddress},config)
	if err != nil {
		return nil, fmt.Errorf("failed to setup producer: %w", err)
	}
	return producer, nil
}


func main() {
	users := []models.User{
		{ID: 1, Name: "Emma"},
		{ID: 2, Name: "Bruno"},
		{ID: 3, Name: "Rick"},
		{ID: 4, Name: "Lena"},
	}
	producer, err := SetupProducer()
	if err != nil {
		log.Fatalf("failed to initialize producer: %v", err)
	}
	defer producer.Close()
	router := mux.NewRouter()
	router.HandleFunc("/send", CreateHTTPHandleFunc(HandleSendMessage, users, producer))
	fmt.Printf("Kafka PRODUCER 📨 started at http://localhost%s\n", ProducerPort)
	http.ListenAndServe(":9400", router)
}
