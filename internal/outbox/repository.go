package outbox

import (
	"context"
	"notification-engine/internal/database"

	"github.com/google/uuid"
)

type Repository interface {
	Create(ctx context.Context, tx database.DBTX, n *Event) error
	FetchUnprocessed(ctx context.Context, tx database.DBTX, limit int) ([]*Event, error)
	MarkProcessed(ctx context.Context, tx database.DBTX, eventIds []uuid.UUID) error
}
