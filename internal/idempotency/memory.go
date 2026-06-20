package idempotency

import "sync"

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

func (m *MemoryStore) Has(eventID string) bool {
	m.mx.RLock()
	defer m.mx.RUnlock()

	_, ok := m.store[eventID]

	return ok
}

func (m *MemoryStore) Mark(eventID string) {
	m.mx.Lock()
	defer m.mx.Unlock()

	m.store[eventID] = struct{}{}
}
