package notification

import (
	"context"
	"errors"
	"notification-engine/internal/database"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgreRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgreRepository {
	return &PostgreRepository{pool: pool}
}

// implementing repository interface
func (r *PostgreRepository) Create(
	ctx context.Context,
	db database.DBTX,
	n *Notification,
) error {
	const query = `
	INSERT INTO notifications(
		id,
		recipient_id,
		template_id,
		status,
		idempotency_key,
		payload,
		send_at
	)
	VALUES($1,$2,$3,$4,$5,$6,$7)
	`

	_, err := db.Exec(
		ctx,
		query,
		n.ID,
		n.RecipientID,
		n.TemplateID,
		n.Status,
		n.IdempotencyKey,
		n.Payload,
		n.SendAt,
	)

	return err
}

func (r *PostgreRepository) GetByIdempotencyKey(
	ctx context.Context,
	key string,
) (*Notification, error) {
	const query = `
	SELECT 
		id,
		recipient_id,
		template_id,
		status,
		idempotency_key
	
	FROM notifications
	WHERE idempotency_key = $1
	`

	var n Notification

	err := r.pool.QueryRow(
		ctx,
		query,
		key,
	).Scan(
		&n.ID,
		&n.RecipientID,
		&n.TemplateID,
		&n.Status,
		&n.IdempotencyKey,
	)

	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
	}

	return &n, nil
}

func (r *PostgreRepository) UpdateStatus(ctx context.Context, id uuid.UUID, status string) error {
	// not using transaction for update
	const query = `UPDATE notifications SET status = $1 WHERE id = $2`

	_, err := r.pool.Exec(ctx, query, status, id)

	return err
}
