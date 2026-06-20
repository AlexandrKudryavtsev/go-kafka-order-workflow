package inventory

import (
	"context"
	"encoding/json"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/events"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/worker"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/kafka"
)

type Processor struct {
	handler *Handler
}

func NewProcessor(handler *Handler) *Processor {
	return &Processor{handler: handler}
}

func (p *Processor) Process(ctx context.Context, msg kafka.Message) (worker.Result, error) {
	var event events.OrderCreatedEvent

	if err := json.Unmarshal(msg.Value, &event); err != nil {
		return worker.Result{}, err
	}

	if event.EventType != events.EventTypeOrderCreated {
		return worker.Result{Skip: true}, nil
	}

	out, err := p.handler.HandleOrderCreated(ctx, event)
	if err != nil {
		return worker.Result{}, err
	}

	return worker.Result{
		Key:   event.OrderID,
		Event: out,
	}, nil
}
