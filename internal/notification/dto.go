package notification

import (
	"encoding/json"
	"time"
)

type CreateNotificationRequest struct {
	RecipientID    string          `json:"recipient_id"`
	TemplateID     string          `json:"template_id"`
	IdempotencyKey string          `json:"idempotency_key"`
	Payload        json.RawMessage `json:"payload"`
	SendAt         *time.Time      `json:"send_at,omitempty"`
}

type CreateNotificationResponse struct {
	ID     string `json:"id"`
	Status string `json:"status"`
}
