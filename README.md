# 🚀 Distributed Notification Engine

An enterprise-grade, distributed notification system built in Go. Designed to handle high-throughput, reliable delivery of transactional messages (Emails, SMS, Push) with guarantees against data loss, dual-write failures, and broker crashes.

## 🧠 Core Architecture & Systems Design

This project serves as a masterclass in distributed systems design, implementing several advanced patterns to ensure absolute data integrity and scalability:

* **The Transactional Outbox Pattern:** Eliminates the "Dual-Write Problem". The API never talks directly to the message broker. Notifications and Outbox events are saved simultaneously inside a single ACID PostgreSQL transaction.
* **Row-Level Locking (`SKIP LOCKED`):** The Outbox Relay achieves horizontal scalability and prevents race conditions by using Postgres `FOR UPDATE SKIP LOCKED`, allowing multiple Relay instances to poll concurrently without fetching duplicate rows.
* **Sliding Window Scheduling:** Efficient long-term task scheduling. Instead of flooding Redis RAM with future tasks, tasks scheduled far in the future rest in Postgres. The Relay continuously sweeps a 1-hour look-ahead window, moving upcoming tasks to Redis for millisecond-precision execution.
* **At-Least-Once Delivery & Exponential Backoff:** Built on top of **Asynq** (Redis), worker tasks are guaranteed to execute. If a 3rd-party provider (e.g., Twilio/SendGrid) goes down, jobs are safely retried using exponential backoff before being routed to a Dead Letter Queue (DLQ).
* **Bounded Worker Pools:** The Outbox Relay uses Go channels and a strictly bounded pool of goroutines to prevent memory leaks and CPU starvation during massive polling spikes.
* **Graceful Shutdown:** Intercepts OS signals (`SIGTERM`) to cleanly drain Go channels and WaitGroups, ensuring no notifications are dropped during Kubernetes deployments.

## 🛠️ Tech Stack
* **Language:** Go (Golang)
* **Database:** PostgreSQL (pgx, golang-migrate)
* **Message Broker:** Redis & Asynq
* **Infrastructure:** Docker Compose

---

## 🚦 How the Data Flows

1. **API Phase:** Client sends an HTTP POST. The API opens a DB transaction, creates a `Notification` (Domain) and an `Outbox Event` (Integration), and commits.
2. **Relay Phase:** A background dispatcher polls Postgres for `PENDING` outbox rows in the current time window, locks them, pushes them to Redis via Asynq, and instantly deletes the outbox row.
3. **Worker Phase:** Independent consumer workers pull jobs from Redis, trigger the 3rd-party sender APIs, and update the primary `notifications` table to `SENT` to close the analytics loop.

---

## 💻 Getting Started

### 1. Start Infrastructure (Postgres & Redis)
Ensure you have Docker installed, then spin up the databases:
```bash
docker-compose up -d
```

### 2. Start the API Server & Relay
In your first terminal, start the API (this will also automatically run DB migrations):
```bash
go run cmd/api/main.go
```

### 3. Start the Consumer Worker
In a second terminal, boot up the isolated background worker:
```bash
go run cmd/worker/main.go
```

### 4. Trigger a Notification
Send a POST request to schedule a notification. 

*To send immediately, omit the `send_at` field.*
*To schedule for the future, provide a UTC timestamp in the `send_at` field.*

```bash
curl -X POST http://localhost:8080/notify \
  -H "Content-Type: application/json" \
  -d '{
        "recipient_id": "user_123",
        "template_id": "welcome_email",
        "idempotency_key": "unique_key_001",
        "payload": {"name": "Tushar"},
        "send_at": "2026-10-31T09:00:00Z"
      }'
```

Watch your API terminal save it, the Relay forward it, and the Worker terminal process it!

## 🧪 Future Optimizations (Scale > 10,000 TPS)
To push this architecture to handle 10,000+ notifications per second, the following optimizations can be layered on:
* **Batching DB Updates:** Buffer successful notification IDs in memory and flush updates to Postgres using array-based updates (`WHERE id = ANY($1)`) to reduce transaction overhead.
* **Postgres LISTEN/NOTIFY:** Replace the 2-second Relay polling interval with a Postgres Pub/Sub trigger for sub-millisecond, zero-latency outbox processing.
* **Webhook Ingestion:** Shift the `SENT` database update from the Worker to an asynchronous webhook ingestion pipeline to decouple internal infrastructure from 3rd-party network latency.
