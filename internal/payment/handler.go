package payment

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

const declineThreshold = 1_000

func (h *Handler) HandleInventoryReserved(ctx context.Context, event events.InventoryReservedEvent) (any, error) {
	now := time.Now().UTC()

	if event.Amount > declineThreshold {
		return events.PaymentFailedEvent{
			EventID:   uuid.NewString(),
			EventType: events.EventTypePaymentFailed,
			Version:   1,
			OrderID:   event.OrderID,
			Reason:    "payment_declined",
			FailedAt:  now,
		}, nil
	}

	return events.PaymentSucceededEvent{
		EventID:   uuid.NewString(),
		EventType: events.EventTypePaymentSucceeded,
		Version:   1,
		OrderID:   event.OrderID,
		Amount:    event.Amount,
		PaidAt:    now,
	}, nil
}
