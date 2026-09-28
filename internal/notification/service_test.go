package notification

import (
	"context"
	"errors"
	"testing"

	"notification-engine/internal/database"
	"notification-engine/internal/outbox"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// --- Mocks ---

// MockNotificationRepo implements Repository for testing
type MockNotificationRepo struct {
	MockGetByIdempotencyKey func(ctx context.Context, key string) (*Notification, error)
	MockCreate              func(ctx context.Context, tx database.DBTX, n *Notification) error
}

func (m *MockNotificationRepo) GetByIdempotencyKey(ctx context.Context, key string) (*Notification, error) {
	return m.MockGetByIdempotencyKey(ctx, key)
}
func (m *MockNotificationRepo) Create(ctx context.Context, tx database.DBTX, n *Notification) error {
	return m.MockCreate(ctx, tx, n)
}

// MockOutboxRepo implements outbox.Repository for testing
type MockOutboxRepo struct {
	MockCreate func(ctx context.Context, tx database.DBTX, n *outbox.Event) error
}

func (m *MockOutboxRepo) Create(ctx context.Context, tx database.DBTX, n *outbox.Event) error {
	return m.MockCreate(ctx, tx, n)
}
func (m *MockOutboxRepo) FetchUnprocessed(ctx context.Context, tx database.DBTX, limit int) ([]*outbox.Event, error) {
	return nil, nil
}
func (m *MockOutboxRepo) MarkProcessed(ctx context.Context, tx database.DBTX, eventIds []uuid.UUID) error {
	return nil
}

// MockUnitOfWork implements database.UnitOfWork for testing
type MockUnitOfWork struct {
	MockExecute func(ctx context.Context, fn func(tx pgx.Tx) error) error
}

func (m *MockUnitOfWork) Execute(ctx context.Context, fn func(tx pgx.Tx) error) error {
	return m.MockExecute(ctx, fn)
}

// --- Tests ---

func TestCreateNotification_IdempotencySuccess(t *testing.T) {
	ctx := context.Background()

	existingNotification := &Notification{
		ID:             uuid.New(),
		IdempotencyKey: "test-key-123",
	}

	// Setup Mocks
	mockNotifRepo := &MockNotificationRepo{
		MockGetByIdempotencyKey: func(ctx context.Context, key string) (*Notification, error) {
			if key == "test-key-123" {
				return existingNotification, nil
			}
			return nil, ErrNotFound
		},
	}

	// Create Service
	svc := NewService(mockNotifRepo, nil, nil)

	// Execute
	req := CreateNotificationRequest{IdempotencyKey: "test-key-123"}
	result, err := svc.CreateNotification(ctx, req)

	// Assert
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result.ID != existingNotification.ID {
		t.Errorf("expected existing notification ID, got %v", result.ID)
	}
}

func TestCreateNotification_CreatesNew(t *testing.T) {
	ctx := context.Background()
	req := CreateNotificationRequest{
		IdempotencyKey: "new-key",
		RecipientID:    "user_1",
		TemplateID:     "welcome_email",
		Payload:        []byte{},
	}

	// Setup Mocks
	mockNotifRepo := &MockNotificationRepo{
		MockGetByIdempotencyKey: func(ctx context.Context, key string) (*Notification, error) {
			return nil, ErrNotFound // Simulate not found so it creates a new one
		},
		MockCreate: func(ctx context.Context, tx database.DBTX, n *Notification) error {
			if n.RecipientID != "user_1" {
				return errors.New("invalid recipient")
			}
			return nil
		},
	}

	mockOutboxRepo := &MockOutboxRepo{
		MockCreate: func(ctx context.Context, tx database.DBTX, n *outbox.Event) error {
			if n.EventType != "NOTIFICATION_CREATED" {
				return errors.New("invalid event type")
			}
			return nil
		},
	}

	mockUoW := &MockUnitOfWork{
		MockExecute: func(ctx context.Context, fn func(tx pgx.Tx) error) error {
			// Simulate the transaction wrapper executing the inner function
			return fn(nil) // passing nil for pgx.Tx in a mock is fine
		},
	}

	// Create Service
	svc := NewService(mockNotifRepo, mockOutboxRepo, mockUoW)

	// Execute
	result, err := svc.CreateNotification(ctx, req)

	// Assert
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if result == nil {
		t.Fatalf("expected a notification to be returned")
	}
	if result.Status != StatusAccepted {
		t.Errorf("expected status %v, got %v", StatusAccepted, result.Status)
	}
}
