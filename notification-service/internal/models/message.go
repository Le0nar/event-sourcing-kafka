package models

import "github.com/google/uuid"

type Message struct {
	Id uuid.UUID `json:"id"`
	UserId uuid.UUID `json:"userId"`
	Status string `json:"status"`
}