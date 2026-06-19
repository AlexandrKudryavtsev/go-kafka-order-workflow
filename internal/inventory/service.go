package inventory

import (
	"context"
	"encoding/json"
	"errors"
	"os/signal"
	"syscall"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/config"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/event"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/kafka"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/logger"
)

func Run(cfg *config.Config, groupID string) error {
	log := logger.New(cfg.Logger)

	handler := NewHandler()

	consumer := kafka.NewConsumer(cfg.Kafka.Topics.OrderEvents, groupID, cfg.Kafka.Brokers)
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Error("failed to close consumer", "error", err)
		}
	}()

	producer := kafka.NewProducer(cfg.Kafka.Topics.InventoryEvents, cfg.Kafka.Brokers)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("failed to close producer", "error", err)
		}
	}()

	log.Info("creating kafka consumer and producer")

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	for {
		msg, err := consumer.Fetch(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				log.Info("consumer canceled")
				return nil
			}

			log.Error("failed to fetch message", "error", err)
			return err
		}

		var createdEvent event.OrderCreatedEvent
		if err = json.Unmarshal(msg.Value, &createdEvent); err != nil {
			// TODO: write to dlq
			log.Error("failed to unmarshal json", "error", err)

			if err := consumer.Commit(ctx, msg); err != nil {
				log.Error("failed to commit message", "error", err)
				return err
			}

			continue
		}

		if createdEvent.EventType != event.EventTypeOrderCreated {
			log.Info(
				"skip unsupported event",
				"event_type", createdEvent.EventType,
				"event_id", createdEvent.EventID,
			)

			if err := consumer.Commit(ctx, msg); err != nil {
				log.Error("failed to commit message", "error", err)
				return err
			}

			continue
		}

		result, err := handler.HandleOrderCreated(ctx, createdEvent)
		if err != nil {
			// TODO: retry + dlq
			return err
		}

		if err := producer.Write(ctx, createdEvent.OrderID, result); err != nil {
			log.Error("failed to write message", "error", err, "order_id", createdEvent.OrderID)
			return err
		}
		log.Info("published inventory event", "order_id", createdEvent.OrderID)

		if err := consumer.Commit(ctx, msg); err != nil {
			log.Error("failed to commit message", "error", err)
			return err
		}
	}
}
