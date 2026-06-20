package shipping

import (
	"context"
	"time"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/events"
	"github.com/google/uuid"
)

type Handler struct {
}

func NewHandler() *Handler {
	return &Handler{}
}

func (h *Handler) HandlePaymentSucceeded(ctx context.Context, event events.PaymentSucceededEvent) (any, error) {
	return events.ShipmentCreatedEvent{
		EventID:    uuid.NewString(),
		EventType:  events.EventTypeShipmentCreated,
		Version:    1,
		OrderID:    event.OrderID,
		ShipmentID: uuid.NewString(),
		CreatedAt:  time.Now().UTC(),
	}, nil
}
