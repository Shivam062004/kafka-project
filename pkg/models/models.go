package models

type User struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

type Notification struct {
	From    User   `json:"from"`
	To      User   `json:"to"`
	Message string `json:"message"`
}

type Request struct {
	FromID  int    `json:"fromId"`
	ToID    int    `json:"toId"`
	Message string `json:"message"`
}

type Response struct {
	Message string `json:"message"`
}
