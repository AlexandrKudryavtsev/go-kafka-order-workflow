package payment

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/events"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/idempotency"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/worker"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/kafka"
)

type Processor struct {
	handler *Handler
	store   idempotency.Store
}

func NewProcessor(handler *Handler, store idempotency.Store) *Processor {
	return &Processor{
		handler: handler,
		store:   store,
	}
}

func (p *Processor) Process(ctx context.Context, msg kafka.Message) (worker.Result, error) {
	var event events.InventoryReservedEvent

	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return worker.Result{}, worker.NonRetryable(events.ReasonInvalidJSON, err)
	}

	if event.EventType != events.EventTypeInventoryReserved {
		return worker.Result{Skip: true}, nil
	}

	if event.Version != 1 {
		return worker.Result{}, worker.NonRetryable(
			events.ReasonUnsupportedEventVersion,
			fmt.Errorf("unsupported event version: %d", event.Version),
		)
	}

	has, err := p.store.Has(ctx, event.EventID)
	if err != nil {
		return worker.Result{}, worker.Retryable(events.ReasonIdempotencyStoreFailed, err)
	}

	if has {
		return worker.Result{Skip: true, Duplicate: true}, nil
	}

	out, err := p.handler.HandleInventoryReserved(ctx, event)
	if err != nil {
		return worker.Result{}, worker.Retryable(events.ReasonHandlerFailed, err)
	}

	return worker.Result{
		EventID: event.EventID,
		Key:     event.OrderID,
		OrderID: event.OrderID,
		Event:   out,
	}, nil
}
