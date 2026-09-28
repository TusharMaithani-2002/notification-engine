package notification

import (
	"context"
	"notification-engine/internal/database"

	"github.com/google/uuid"
)

type Repository interface {
	Create(ctx context.Context, tx database.DBTX, n *Notification) error

	GetByIdempotencyKey(
		ctx context.Context,
		key string,
	) (*Notification, error)

	UpdateStatus(ctx context.Context, id uuid.UUID, status string) error
}
