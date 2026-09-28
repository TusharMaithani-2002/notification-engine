# Systems Design & Backend Interview Guide
*(Tailored for Mid-Level / 2-3 YOE Go Developer)*

This guide covers the exact systems design concepts you have implemented in your Notification Engine. Interviewers use these questions to separate junior developers (who just write CRUD APIs) from mid-level engineers (who understand distributed systems, failure modes, and concurrency).

---

## 1. The Dual-Write Problem
**Question:** *"If our API accepts a notification, saves it to the database, and then immediately publishes it to a message broker (like RabbitMQ/Redis)—what happens if the API loses power right between the database save and the message broker publish?"*

**What they are testing:** Do you understand distributed transactions and the "Dual-Write" problem? 

**Your Answer:** 
"To solve this, I implemented the **Transactional Outbox Pattern**. I never write directly to the message broker from the API. Instead, I open a single Postgres transaction (Unit of Work) and write the `Notification` record and an `Outbox Event` record simultaneously. If the power cuts out, the Postgres transaction rolls back, guaranteeing no ghost states. A separate background Relay process is then responsible for pushing the outbox events to Redis."

**Code Evidence (`service.go`):**
```go
// Both creations happen inside a single pgx.Tx transaction
err = s.uow.Execute(ctx, func(tx pgx.Tx) error {
    if err := s.notificationRepo.Create(ctx, tx, notification); err != nil {
        return err // Rolls back both!
    }
    if err := s.outboxRepo.Create(ctx, tx, event); err != nil {
        return err // Rolls back both!
    }
    return nil
})
```

---

## 2. Horizontal Scaling & Row Contention
**Question:** *"Your Outbox Relay polls the database for pending events. If we deploy 5 instances of your Relay service to Kubernetes, how do you prevent them from grabbing the same database rows and sending duplicate emails?"*

**What they are testing:** Do you understand database locking and concurrency control?

**Your Answer:**
"I used Postgres row-level locking, specifically the `FOR UPDATE SKIP LOCKED` clause. When a dispatcher queries for `PENDING` rows, it locks them. If a second dispatcher queries a millisecond later, `SKIP LOCKED` tells it to instantly skip the locked rows and grab the next available batch. Furthermore, I wrap this in an `UPDATE ... RETURNING` statement so the rows instantly transition to `PROCESSING` atomically, entirely eliminating race conditions."

**Code Evidence (`postgre_repository.go`):**
```sql
UPDATE outbox
SET status = 'PROCESSING'
WHERE id IN (
    SELECT id FROM outbox
    WHERE status = 'PENDING'
    ORDER BY created_at ASC
    LIMIT $1
    FOR UPDATE SKIP LOCKED
)
RETURNING id, aggregate_id, event_type, payload, status, created_at
```

---

## 3. Worker Pools & Go Concurrency
**Question:** *"How does your Relay actually process the database rows once it fetches them? If it fetches 50 rows, does it process them sequentially?"*

**What they are testing:** Do you know how to use Go channels and goroutines effectively, without leaking memory?

**Your Answer:**
"No, they aren't processed sequentially. I implemented a bounded Worker Pool using the Dispatcher-Worker pattern. The Dispatcher loop fetches batches and pushes them into a buffered Go channel. I pre-spawn a fixed number of worker goroutines (e.g., 5-10) that range over that channel. This prevents overwhelming the CPU or memory while allowing highly concurrent network requests to Redis."

**Code Evidence (`relay.go`):**
```go
eventsChan := make(chan *Event, r.batchSize)
var wg sync.WaitGroup

// 1. Pre-spawn a bounded pool of workers
for i := 1; i <= r.workerSize; i++ {
    wg.Add(1)
    go r.worker(ctx, i, eventsChan, &wg)
}

// 2. Dispatcher pushes to the channel
for _, e := range events {
    eventsChan <- e
}
```

---

## 4. Failure Recovery & Orphaned Jobs (PEL Concept)
**Question:** *"Your Consumer worker pulls a job from Redis to send an email. What happens if the worker's hardware crashes exactly while the email is being sent? How do you ensure the job isn't lost forever?"*

**What they are testing:** Do you understand At-Least-Once delivery, message acknowledgments (ACKs), and broker recovery mechanisms (like Redis PEL)?

**Your Answer:**
"I use Asynq, which guarantees **At-Least-Once delivery**. When a worker pulls a task, it doesn't delete it from Redis immediately. Instead, Asynq moves it to an 'Active' queue. The worker continuously sends a 'heartbeat' to Redis while processing. If the worker crashes, the heartbeat stops. A background inspector detects the dead worker, reclaims the orphaned task, and moves it back to the pending queue for another worker to process."

**Code Evidence (`handler.go`):**
```go
func HandleNotificationDeliveryTask(ctx context.Context, t *asynq.Task) error {
    // If the 3rd party API times out, or we return an error,
    // Asynq automatically schedules a retry with Exponential Backoff!
    if err := sendEmailToTwilio(); err != nil {
        return fmt.Errorf("api failure, forcing retry: %w", err)
    }

    // Only when we return nil does Asynq safely remove the job from the queue.
    return nil
}
```

---

## 5. Graceful Shutdown
**Question:** *"When we deploy a new version of your service, Kubernetes will send a SIGTERM signal to shut down the old pod. How do you handle this without interrupting a worker mid-email?"*

**What they are testing:** Do you know how `context` and `sync.WaitGroup` are used in production to drain connections cleanly?

**Your Answer:**
"I intercept the OS shutdown signal using `os.Signal` and use it to cancel a global `context`. My Dispatcher listens for `<-ctx.Done()`. When it triggers, the Dispatcher closes the `eventsChan`. This signals the workers to stop pulling new jobs, but allows them to finish their current job. Finally, I call `wg.Wait()` on my WaitGroup to block the main thread until all workers have gracefully drained."

**Code Evidence (`relay.go`):**
```go
select {
case <-ctx.Done(): // Triggered by OS SIGTERM
    log.Println("Shutting down Outbox Relay...")
    close(eventsChan) // Prevents new jobs from entering
    wg.Wait()         // Blocks until all active workers finish
    return
}
```

---

## 6. Long-Term Task Scheduling (Redis vs. Database Polling)
**Question:** *"We want to allow users to schedule a notification 6 months in advance. Should we push that scheduled job directly into Redis/Asynq, or keep it in Postgres and poll for it? What are the trade-offs?"*

**What they are testing:** Understanding of in-memory data structures (Redis ZSETs) vs. durable disk storage, RAM cost, and architectural scaling trade-offs.

**Your Answer:**
"There are three ways to handle this, depending on scale:

**Approach A (Pure Redis):** Push tasks directly to Asynq (Redis Sorted Set). This fires with millisecond precision and handles 1 million tasks in just ~500MB of RAM. It's the standard for most apps, but gets expensive at extreme scale.

**Approach B (Pure Database Polling):** Leave rows in Postgres and poll every minute for `send_at <= NOW()`. This saves RAM but hammers the primary database with continuous read-load and loses millisecond precision.

**Approach C (The Sliding Window / Hybrid Pattern):** This is the ultimate Senior-level solution. I would store everything long-term in Postgres. Then, I would write a daily cron job that queries for notifications scheduled in the *next 48 hours* and pushes them into Redis. 
This is brilliant because:
1. It limits Redis RAM usage to a rolling 48-hour window.
2. The database is only queried once a day, eliminating continuous polling load.
3. The 48-hour window provides a 24-hour safety buffer if the daily cron job fails.

To handle edge cases (e.g., a user scheduling a notification for 1 hour from now), the API routing logic simply checks: if `send_at < 48 hours`, push straight to Redis. Otherwise, leave it in Postgres for the cron job."

---

## 7. Outbox Table vs. Polling the Primary Table
**Question:** *"Why did you create a dedicated 'outbox' table? Couldn't you just run the polling query directly on the 'notifications' table to save space?"*

**What they are testing:** Understanding of Clean Architecture, table bloat management, and extensible event-driven design.

**Your Answer:**
"While polling the main table works for small apps, it fails at enterprise scale for three reasons:
1. **Table Bloat & Pruning:** The `notifications` table is a historical record that will grow to millions of rows, causing polling queries to slow down over time. The `outbox` table, however, is ephemeral. Once the Relay pushes the job to Redis, it can instantly `DELETE` the outbox row. The outbox stays incredibly tiny and lightning fast.
2. **Universal Event Bus (Future-Proofing):** A generic outbox table can handle *any* system event (`USER_UPDATED`, `BILLING_FAILED`). This means I only have to write and maintain one Relay for the entire company, rather than writing a new poller for every domain table.
3. **Separation of Concerns:** The `notifications` table holds Domain/Business logic. The `outbox` table holds Integration/Infrastructure logic. Mixing them violates single-responsibility principles."

---

## 8. The Closed Loop & Analytics (Status Tracking)
**Question:** *"If the outbox row is deleted after the Relay pushes it to Redis, how do we know the notification was actually sent? How do our analytics dashboards get their data?"*

**What they are testing:** Understanding of asynchronous lifecycles, eventual consistency, and consumer responsibilities.

**Your Answer:**
"The Outbox Relay is only responsible for safely getting the message to the broker (Enqueued status). It does not send the email, so it cannot mark it as 'Sent'. 

To close the loop, the **Consumer Worker** (which actually calls the 3rd party API like Twilio or SendGrid) is given a database connection. When the 3rd party API returns a 200 OK, the worker executes a query to update the `notifications` table directly: `UPDATE notifications SET status = 'SENT', sent_at = NOW() WHERE id = $1`.

Because of this, **Analytics never touches the outbox.** The analytics dashboard queries the `notifications` table exclusively, completely ignoring the ephemeral outbox structure."

---

## 9. High-Volume Database Updates & Bottlenecks
**Question:** *"Once an email is sent, the worker updates the `notifications` table to `SENT`. If we send 10,000 emails per second, that's 10,000 individual `UPDATE` queries per second. Won't a 50-million-row table make this incredibly slow? How do we optimize it?"*

**What they are testing:** Understanding of B-Tree index performance, database connection pool limits, and batching strategies.

**Your Answer:**
"First, the sheer size of the table (50 million rows) is entirely irrelevant to the query speed. Because we update by the Primary Key (`WHERE id = $1`), Postgres uses a B-Tree index to find and update the row in under 1 millisecond.

However, the *transaction volume* (10,000 queries per second) will eventually overwhelm the database connection pool. To optimize this, I would evolve the architecture depending on the exact requirements:

1. **Batching:** Instead of updating immediately, workers push successful IDs to a Go channel. A background goroutine reads the channel and flushes updates to the database every 1 second using a single array query: `UPDATE notifications SET status = 'SENT' WHERE id = ANY($1)`. This reduces 10,000 transactions down to 1.
2. **Webhooks (The Enterprise Way):** I wouldn't update the status in the worker at all. The worker only knows the email was *accepted* by SendGrid, not delivered. I would expose a webhook endpoint (`/webhooks/sendgrid`). SendGrid batches delivery statuses on their end and posts them to our webhook every few seconds, which then performs the batch update. This completely removes the write-load from the workers and guarantees accurate analytics."

---

## 10. Eliminating Polling Latency & Queue Prioritization
**Question:** *"The Outbox Relay polls every 2 seconds, which adds a 2-second delay to all notifications. How do we bypass this delay for instant notifications, and how do we prioritize critical SMS messages over marketing emails once they reach Redis?"*

**What they are testing:** Postgres Pub/Sub (LISTEN/NOTIFY), Queue Priority/QoS, and knowing when *not* to use a pattern.

**Your Answer:**
"To eliminate the 2-second polling delay, I use **Postgres LISTEN/NOTIFY**. I add a database trigger that broadcasts a signal whenever an outbox row is inserted. The Relay maintains a persistent connection listening for this signal, waking it up instantly (< 1ms) to fetch and push the event to Redis. The 2-second poller is kept running purely as a fallback safety net in case the network blips.

Once the tasks are in Redis, I use **Asynq's Strict Priority Queues** to handle routing. I configure a `critical` queue for SMS and a `default` queue for emails. Even if there are 50,000 emails waiting, Asynq is configured to always drain the `critical` queue first, ensuring OTPs go out instantly.

**The Pragmatic Exception:** For highly critical user-facing events like OTPs, I actually bypass the Outbox entirely. I call Twilio synchronously directly in the HTTP request. If Twilio is down, the API returns a 500, and the user acts as the retry mechanism by clicking 'Resend Code'."
