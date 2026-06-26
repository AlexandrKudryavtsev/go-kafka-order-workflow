package idempotency

import (
	"context"
	"sync"
)

type MemoryStore struct {
	mx    sync.RWMutex
	store map[string]struct{}
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		mx:    sync.RWMutex{},
		store: make(map[string]struct{}, 0),
	}
}

var _ Store = (*MemoryStore)(nil)

func (m *MemoryStore) Has(ctx context.Context, eventID string) (bool, error) {
	m.mx.RLock()
	defer m.mx.RUnlock()

	_, ok := m.store[eventID]

	return ok, nil
}

func (m *MemoryStore) Mark(ctx context.Context, eventID string) error {
	m.mx.Lock()
	defer m.mx.Unlock()

	m.store[eventID] = struct{}{}

	return nil
}
