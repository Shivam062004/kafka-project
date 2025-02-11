package main

import (
	"encoding/json"
	"net/http"
)

type apiFunc func(http.ResponseWriter, *http.Request, *NotificationStore)

func WriteJSON(w http.ResponseWriter, status int, v any) {
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}


func CreateHTTPHandleFunc(handlerFunc apiFunc, store *NotificationStore) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		handlerFunc(w, r, store)
	}
}