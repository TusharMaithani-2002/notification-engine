package notification

import (
	"context"
	"encoding/json"
	"errors"
	"notification-engine/internal/database"
	"notification-engine/internal/outbox"
	"notification-engine/internal/task"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type Service struct {
	notificationRepo Repository
	outboxRepo       outbox.Repository
	uow              database.UnitOfWork
}

func NewService(notificationRepo Repository, outboxRepo outbox.Repository, uow database.UnitOfWork) *Service {
	return &Service{
		notificationRepo: notificationRepo,
		outboxRepo:       outboxRepo,
		uow:              uow,
	}
}

func (s *Service) CreateNotification(
	ctx context.Context,
	req CreateNotificationRequest,
) (*Notification, error) {

	existing, err := s.notificationRepo.GetByIdempotencyKey(ctx, req.IdempotencyKey)

	if err == nil {
		return existing, nil
	}

	if !errors.Is(err, ErrNotFound) {
		return nil, err
	}

	notification := &Notification{
		ID:             uuid.New(),
		RecipientID:    req.RecipientID,
		IdempotencyKey: req.IdempotencyKey,
		Payload:        req.Payload,
		TemplateID:     req.TemplateID,
		Status:         StatusAccepted,
		SendAt:         req.SendAt,
	}

	taskPayload := task.NotificationDeliveryPayload{
		NotificationID: notification.ID.String(),
		RecipientID:    notification.RecipientID,
		TemplateID:     notification.TemplateID,
		Data:           notification.Payload,
		SendAt:         notification.SendAt,
	}

	// converting payload to bytes
	payloadBytes, _ := json.Marshal(taskPayload)

	event := &outbox.Event{
		ID:          uuid.New(),
		AggregateID: notification.ID,
		EventType:   "NOTIFICATION_CREATED",
		Payload:     payloadBytes,
		SendAt:      notification.SendAt,
	}

	// service will get event and notification then call their respective repositories to save them in a single transaction
	err = s.uow.Execute(ctx, func(tx pgx.Tx) error {
		if err := s.notificationRepo.Create(
			ctx,
			tx,
			notification,
		); err != nil {
			return err
		}

		if err := s.outboxRepo.Create(ctx, tx, event); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return notification, nil
}
