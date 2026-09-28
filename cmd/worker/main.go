package main

import (
	"context"
	"log"
	"notification-engine/internal/config"
	"notification-engine/internal/database"
	"notification-engine/internal/notification"
	"notification-engine/internal/task"

	"github.com/hibiken/asynq"
)

func main() {
	ctx := context.Background()
	cfg := config.Load()

	redisOpt := asynq.RedisClientOpt{Addr: cfg.RedisURL}

	pool, _ := database.NewPool(ctx, cfg.DatabaseURL)
	notificationRepo := notification.NewPostgresRepository(pool)

	// Initialize the Asynq Server
	// This dictates how many goroutines the worker pool can use (Concurrency)
	srv := asynq.NewServer(
		redisOpt,
		asynq.Config{
			Concurrency: 10, // Process up to 10 notifications simultaneously
			Queues: map[string]int{
				"default": 1, // You can prioritize queues later (e.g., "critical": 5)
			},
		},
	)

	handler := task.NewNotificationHandler(notificationRepo)

	// 3. Create a ServeMux (Router)
	// This works exactly like an HTTP router! It matches the Task Type string to the Handler function.
	mux := asynq.NewServeMux()
	mux.HandleFunc(task.TypeNotificationDelivery, handler.HandleNotificationDeliveryTask)
	// 4. Start the server!
	log.Println("👷 Starting Asynq Worker Server...")
	if err := srv.Run(mux); err != nil {
		log.Fatalf("could not run server: %v", err)
	}
}
