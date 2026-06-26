package idempotency

import "context"

type Store interface {
	Has(ctx context.Context, eventID string) (bool, error)
	Mark(ctx context.Context, eventID string) error
}
