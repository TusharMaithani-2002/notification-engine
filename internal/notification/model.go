package notification

import (
	"time"

	"github.com/google/uuid"
)

type NotificationStatus string

const (
	StatusAccepted NotificationStatus = "ACCEPTED"
)

type Notification struct {
	ID             uuid.UUID
	RecipientID    string
	TemplateID     string
	Status         NotificationStatus
	IdempotencyKey string
	Payload        []byte
	CreatedAt      time.Time
	SendAt         *time.Time
}
