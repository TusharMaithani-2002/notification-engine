package task

import (
	"time"

	"github.com/hibiken/asynq"
)

// Task names
const (
	TypeNotificationDelivery = "notification:deliver"
)

type NotificationDeliveryPayload struct {
	NotificationID string
	RecipientID    string
	TemplateID     string
	Data           []byte
	SendAt         *time.Time
}

func NewNotificationDeliveryTask(payloadData []byte) (*asynq.Task, error) {
	return asynq.NewTask(
		TypeNotificationDelivery,
		payloadData,
		asynq.MaxRetry(5), // Automatically retry 5 times with exponential backoff if it fails!
	), nil
}

// func HandleNotificationDeliveryTask(cxt context.Context, t *asynq.Task) error {
// 	var payload NotificationDeliveryPayload

// 	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
// 		// If the JSON is completely broken, we return an error wrapped with asynq.SkipRetry
// 		return fmt.Errorf("json unmarshal failed: %v: %w", err, asynq.SkipRetry)
// 	}

// 	log.Printf("📬 Attempting to send notification to Recipient: %s", payload.RecipientID)

// 	// Simulate sending the email/SMS via a 3rd party API
// 	time.Sleep(2 * time.Second)

// 	log.Printf("✅ Successfully sent notification %s!", payload.NotificationID)

// 	// return fmt.Errorf("Failed to send notification")
// 	return nil
// }
