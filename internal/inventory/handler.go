package inventory

import (
	"context"
	"sync"
	"time"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/event"
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

func (h *Handler) HandleOrderCreated(ctx context.Context, e event.OrderCreatedEvent) (any, error) {
	h.mx.Lock()
	defer h.mx.Unlock()

	now := time.Now().UTC()
	newEventID := uuid.NewString()

	for _, item := range e.Items {
		if item.Quantity > h.stocks[item.SKU] {
			return event.InventoryRejectedEvent{
				EventID:    newEventID,
				EventType:  event.EventTypeInventoryRejected,
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

	return event.InventoryReservedEvent{
		EventID:    newEventID,
		EventType:  event.EventTypeInventoryReserved,
		Version:    1,
		OrderID:    e.OrderID,
		Items:      e.Items,
		ReservedAt: now,
	}, nil
}
