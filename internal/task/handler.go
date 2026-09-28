package task

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
)

// 1. Define exactly what the handler needs locally! No imports needed.
type NotificationUpdater interface {
	UpdateStatus(ctx context.Context, id uuid.UUID, status string) error
}

type NotificationHandler struct {
	repo NotificationUpdater // 2. Use the interface here
}

func NewNotificationHandler(repo NotificationUpdater) *NotificationHandler {
	return &NotificationHandler{repo: repo}
}

func (h *NotificationHandler) HandleNotificationDeliveryTask(ctx context.Context, t *asynq.Task) error {
	var payload NotificationDeliveryPayload

	if err := json.Unmarshal(t.Payload(), &payload); err != nil {
		// If the JSON is completely broken, we return an error wrapped with asynq.SkipRetry
		return fmt.Errorf("json unmarshal failed: %v: %w", err, asynq.SkipRetry)
	}

	// Simulate sending the email/SMS via a 3rd party API
	time.Sleep(2 * time.Second)

	// resp, err := callTwilioAPI(payload)

	// if err != nil {
	// 	// Network timeout! Twilio is down.
	//     // Return normal error -> Asynq will apply Exponential Backoff!
	// 	return fmt.Errorf("network error calling twilio: %w", err)
	// }
	// if resp.StatusCode == 400 {
	// 	// Fake phone number! Retrying won't help.
	//     // We use asynq.SkipRetry to bypass backoff and go straight to DLQ.
	//     h.repo.UpdateStatus(ctx, payload.NotificationID, "FAILED")
	// 	return fmt.Errorf("invalid phone number: %w", asynq.SkipRetry)
	// }

	notificationID, _ := uuid.Parse(payload.NotificationID)
	err := h.repo.UpdateStatus(ctx, notificationID, "SENT")

	if err != nil {
		return err // Retry if DB is down!
	}

	return nil
}
