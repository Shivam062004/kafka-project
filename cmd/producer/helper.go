package main

import (
	"encoding/json"
	"net/http"
	"kafka-project/pkg/models"
	"github.com/IBM/sarama"
)

type apiFunc func(http.ResponseWriter, *http.Request, []models.User, sarama.SyncProducer) error

func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func CreateHTTPHandleFunc(handlerFunc apiFunc, users []models.User, producer sarama.SyncProducer) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		err := handlerFunc(w, r, users, producer)
		if err != nil {
			WriteJSON(w, http.StatusBadRequest, models.Response {
				Message: err.Error(),
			})
			return
		}
	}
}
