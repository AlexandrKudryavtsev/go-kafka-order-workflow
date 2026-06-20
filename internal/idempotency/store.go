package idempotency

type Store interface {
	Has(eventID string) bool
	Mark(eventID string)
}
