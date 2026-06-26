package idempotency

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresStore struct {
	db          *pgxpool.Pool
	serviceName string
}

func NewPostgresStore(db *pgxpool.Pool, serviceName string) (*PostgresStore, error) {
	if db == nil {
		return nil, errors.New("invalid db")
	}
	if serviceName == "" {
		return nil, errors.New("invalid service name")
	}

	return &PostgresStore{
		db:          db,
		serviceName: serviceName,
	}, nil
}

var _ Store = (*PostgresStore)(nil)

func (p *PostgresStore) Init(ctx context.Context) error {
	const query = `
CREATE TABLE IF NOT EXISTS processed_events (
	service_name TEXT NOT NULL,
	event_id TEXT NOT NULL,
	processed_at TIMESTAMPTZ NOT NULL DEFAULT now(),
	UNIQUE (service_name, event_id)
)
`

	if _, err := p.db.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create table: %w", err)
	}

	return nil
}

func (p *PostgresStore) Has(ctx context.Context, eventID string) (bool, error) {
	const query = `
SELECT EXISTS (
	SELECT 1 FROM processed_events
	WHERE service_name = $1 AND event_id = $2
)
`
	var exists bool

	err := p.db.QueryRow(
		ctx,
		query,
		p.serviceName,
		eventID,
	).Scan(&exists)

	if err != nil {
		return false, fmt.Errorf("failed to read row: %w", err)
	}

	return exists, nil
}

func (p *PostgresStore) Mark(ctx context.Context, eventID string) error {
	const query = `
INSERT INTO processed_events (service_name, event_id)
VALUES ($1, $2)
ON CONFLICT (service_name, event_id) DO NOTHING
`
	_, err := p.db.Exec(ctx, query, p.serviceName, eventID)
	if err != nil {
		return fmt.Errorf("failed to insert row: %w", err)
	}

	return nil
}
