package database

import (
	"context"

	"github.com/jackc/pgx/v5"
)

type UnitOfWork interface {
	Execute(ctx context.Context, fn func(tx pgx.Tx) error) error
}
