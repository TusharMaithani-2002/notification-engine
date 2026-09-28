package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"notification-engine/internal/config"
	"notification-engine/internal/database"
	"notification-engine/internal/notification"
	"notification-engine/internal/outbox"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/hibiken/asynq"
)

func main() {
	ctx := context.Background()
	cfg := config.Load()

	// 1. Run Database Migrations FIRST
	log.Println("Running database migrations...")

	// Note: Make sure cfg.DatabaseURL includes "?sslmode=disable" if running locally
	m, err := migrate.New(
		"file://migrations", // Points to your migrations folder
		cfg.DatabaseURL,
	)

	if err != nil {
		log.Fatalf("Migration initialization failed: %v", err)
	}

	// Apply the migrations
	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		log.Fatalf("Migration failed: %v", err)
	}

	log.Println("Database migrations applied successfully!")

	pool, err := database.NewPool(ctx, cfg.DatabaseURL)

	if err != nil {
		log.Fatal(err)
	}

	defer pool.Close()

	fmt.Println("Database connected")

	outboxRepo := outbox.NewPostgresRepository(pool)
	uow := database.NewPgxUnitOfWork(pool)

	redisOpt := asynq.RedisClientOpt{Addr: cfg.RedisURL}
	asynqClient := asynq.NewClient(redisOpt)
	defer asynqClient.Close()

	relay := outbox.NewRelay(outboxRepo, uow, asynqClient, 5, 10)
	go relay.Start(ctx)

	notificationRepo := notification.NewPostgresRepository(pool)
	notificationSvc := notification.NewService(notificationRepo, outboxRepo, uow)

	// Minimal HTTP handler
	http.HandleFunc("/notify", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var req notification.CreateNotificationRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		notif, err := notificationSvc.CreateNotification(r.Context(), req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(notif)
	})

	log.Println("Starting API server on :8080...")
	if err := http.ListenAndServe(":8080", nil); err != nil {
		log.Fatal(err)
	}
}
