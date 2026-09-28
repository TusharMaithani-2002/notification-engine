package database

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PgxUnitOfWork struct {
	pool *pgxpool.Pool
}

func NewPgxUnitOfWork(pool *pgxpool.Pool) *PgxUnitOfWork {
	return &PgxUnitOfWork{
		pool: pool,
	}
}

func (u *PgxUnitOfWork) Execute(ctx context.Context, fn func(tx pgx.Tx) error) error {
	tx, err := u.pool.Begin(ctx)

	if err != nil {
		return err
	}

	defer tx.Rollback(ctx)

	if err := fn(tx); err != nil {
		return err
	}

	return tx.Commit(ctx)
}
