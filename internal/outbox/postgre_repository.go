package outbox

import (
	"context"
	"notification-engine/internal/database"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgreRepository struct {
	pool *pgxpool.Pool
}

func NewPostgresRepository(pool *pgxpool.Pool) *PostgreRepository {
	return &PostgreRepository{pool: pool}
}

func (r *PostgreRepository) Create(
	ctx context.Context,
	db database.DBTX,
	event *Event,
) error {
	const query = `
	INSERT INTO outbox(
		id,
		aggregate_id,
		event_type,
		payload,
		send_at,
		status
	)
	
	VALUES($1,$2,$3,$4,$5,'PENDING')
	`

	_, err := db.Exec(
		ctx,
		query,
		event.ID,
		event.AggregateID,
		event.EventType,
		event.Payload,
		event.SendAt,
	)

	return err
}

func (r *PostgreRepository) FetchUnprocessed(ctx context.Context, db database.DBTX, limit int) ([]*Event, error) {
	const query = `
		UPDATE outbox
		SET status = 'PROCESSING'
		WHERE id IN (
			SELECT id FROM outbox
			WHERE
			(
				status = 'PENDING'
				OR
				-- Zombie Recovery: Grab events that crashed mid-process 5 minutes ago!
				(status = 'PROCESSING' AND created_at < NOW() - INTERVAL '5 minutes')
			)
			-- The 1-hour sliding window filter:
    		AND (send_at IS NULL OR send_at <= NOW() + INTERVAL '1 hour') 
			ORDER BY created_at ASC
			LIMIT $1
			FOR UPDATE SKIP LOCKED
		)

		RETURNING id, aggregate_id, event_type, payload, status, created_at
	`

	// query streams data over the network
	rows, err := db.Query(ctx, query, limit)

	if err != nil {
		return nil, err
	}

	// as data from Query is streamed this is necessary to close the connection preventing connection leak
	defer rows.Close()

	var events []*Event

	for rows.Next() {
		var e Event
		if err := rows.Scan(&e.ID, &e.AggregateID, &e.EventType, &e.Payload, &e.Status, &e.CreatedAt, &e.SendAt); err != nil {
			return nil, err
		}
		events = append(events, &e)
	}

	// Always check rows.Err() after a loop: it is possible for row.next to return false as data is streamed
	// over a connection
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func (r *PostgreRepository) MarkProcessed(ctx context.Context, db database.DBTX, eventIds []uuid.UUID) error {
	query := `UPDATE outbox SET status = 'COMPLETED' where id = ANY($1)`

	_, err := db.Exec(ctx, query, eventIds)

	return err
}
