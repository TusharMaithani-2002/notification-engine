package outbox

import (
	"context"
	"encoding/json"
	"log"
	"notification-engine/internal/database"
	"notification-engine/internal/task"

	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/hibiken/asynq"
	"github.com/jackc/pgx/v5"
)

type Relay struct {
	repo        Repository
	uow         database.UnitOfWork
	asyncClient *asynq.Client
	workerSize  int
	batchSize   int
}

func NewRelay(repo Repository, uow database.UnitOfWork, asyncClient *asynq.Client, workerSize int, batchSize int) *Relay {
	return &Relay{
		repo:        repo,
		uow:         uow,
		workerSize:  workerSize,
		batchSize:   batchSize,
		asyncClient: asyncClient,
	}
}

// boots up worker pool and dispatcher loop
func (r *Relay) Start(ctx context.Context) {
	eventsChan := make(chan *Event, r.batchSize)

	var wg sync.WaitGroup

	// start the workers
	for i := range r.workerSize {
		wg.Add(1)
		go r.worker(ctx, i, eventsChan, &wg)
	}

	// start the dispatcher ticker
	ticker := time.NewTicker(time.Second * 2)
	defer ticker.Stop()

	log.Println("Outbox relay started...")

	for {
		select {
		case <-ctx.Done(): // graceful shutdown
			log.Println("Shutting down outbox relay...")
			close(eventsChan) // Signals workers to stop
			wg.Wait()         // wait for workers to finish current jobs
			return
		case <-ticker.C:
			r.dispatch(ctx, eventsChan)
		}
	}
}

func (r *Relay) dispatch(ctx context.Context, eventsChan chan<- *Event) {
	// We use the UnitOfWork to create a transaction.
	// We need the lock to stay active while the workers process the events!
	r.uow.Execute(ctx, func(tx pgx.Tx) error {
		events, err := r.repo.FetchUnprocessed(ctx, tx, r.batchSize)
		if err != nil {
			return err
		}

		for _, e := range events {
			eventsChan <- e
		}

		return nil
	})
}

func (r *Relay) worker(ctx context.Context, workerID int, eventsChan <-chan *Event, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Printf("Worker %d started", workerID)

	for event := range eventsChan {
		log.Printf("Worker %d processing event: %s", workerID, event.ID)

		// creating new task to put in asynq
		asyncTask, err := task.NewNotificationDeliveryTask(event.Payload)

		// var taskData task.
		var taskData task.NotificationDeliveryPayload
		json.Unmarshal(event.Payload, &taskData)

		var options []asynq.Option

		if taskData.SendAt != nil {
			options = append(options, asynq.ProcessAt(*taskData.SendAt))
		}

		if err != nil {
			log.Printf("Failed to create task for event %s: %v", event.ID, err)
			continue
		}

		info, err := r.asyncClient.EnqueueContext(ctx, asyncTask, options...)
		if err != nil {
			// If Redis is down, we log the error and move on.
			// The event stays as 'PROCESSING' in Postgres and will be retried later!
			log.Printf("Failed to enqueue event %s to Redis: %v", event.ID, err)
		}

		log.Printf("Successfully enqueued task to Redis! Task ID: %s", info.ID)

		err = r.uow.Execute(ctx, func(tx pgx.Tx) error {
			return r.repo.MarkProcessed(ctx, tx, []uuid.UUID{event.ID})
		})

		if err != nil {
			log.Printf("Worker %d failed to mark event %s as processed: %v", workerID, event.ID, err)
		} else {
			log.Printf("Worker %d successfully processed event %s", workerID, event.ID)
		}
	}

}
