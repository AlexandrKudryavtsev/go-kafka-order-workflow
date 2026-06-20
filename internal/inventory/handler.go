package inventory

import (
	"context"
	"sync"
	"time"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/events"
	"github.com/google/uuid"
)

type Handler struct {
	mx     sync.Mutex
	stocks map[string]int
}

func NewHandler() *Handler {
	return &Handler{
		mx: sync.Mutex{},
		stocks: map[string]int{
			"book-1": 10,
			"book-2": 5,
		},
	}
}

func (h *Handler) HandleOrderCreated(ctx context.Context, e events.OrderCreatedEvent) (any, error) {
	h.mx.Lock()
	defer h.mx.Unlock()

	now := time.Now().UTC()
	newEventID := uuid.NewString()

	for _, item := range e.Items {
		if item.Quantity > h.stocks[item.SKU] {
			return events.InventoryRejectedEvent{
				EventID:    newEventID,
				EventType:  events.EventTypeInventoryRejected,
				Version:    1,
				OrderID:    e.OrderID,
				Reason:     "not_enough_stock",
				RejectedAt: now,
			}, nil
		}
	}

	for _, item := range e.Items {
		h.stocks[item.SKU] -= item.Quantity
	}

	return events.InventoryReservedEvent{
		EventID:    newEventID,
		EventType:  events.EventTypeInventoryReserved,
		Version:    1,
		OrderID:    e.OrderID,
		Items:      e.Items,
		Amount:     e.Amount,
		ReservedAt: now,
	}, nil
}
