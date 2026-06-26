package worker

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/events"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/idempotency"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/kafka"
	"github.com/google/uuid"
)

type Result struct {
	EventID string
	Key     string
	Event   any
	Skip    bool
}

type Processor interface {
	Process(ctx context.Context, msg kafka.Message) (Result, error)
}

type Config struct {
	ServiceName      string
	SourceTopic      string
	OutputTopic      string
	DLQTopic         string
	Brokers          []string
	ConsumerGroupID  string
	MaxAttempts      int
	Logger           *slog.Logger
	IdempotencyStore idempotency.Store
}

func Run(ctx context.Context, cfg Config, processor Processor) error {
	if cfg.MaxAttempts <= 0 {
		return errors.New("invalid max attempts")
	}
	if cfg.Logger == nil {
		return errors.New("invalid logger")
	}
	if cfg.IdempotencyStore == nil {
		return errors.New("invalid idempotency store")
	}
	if processor == nil {
		return errors.New("invalid processor")
	}

	log := cfg.Logger.With("service", cfg.ServiceName, "topic", cfg.SourceTopic, "group", cfg.ConsumerGroupID)

	consumer := kafka.NewConsumer(cfg.SourceTopic, cfg.ConsumerGroupID, cfg.Brokers)
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Error("failed to close consumer", "error", err)
		}
	}()
	producer := kafka.NewProducer(cfg.OutputTopic, cfg.Brokers)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("failed to close producer", "error", err)
		}
	}()
	dlqProducer := kafka.NewProducer(cfg.DLQTopic, cfg.Brokers)
	defer func() {
		if err := dlqProducer.Close(); err != nil {
			log.Error("failed to close dlq producer", "error", err)
		}
	}()

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

		handled := false
		var lastError error
		var result Result

		for attempt := 1; attempt <= cfg.MaxAttempts; attempt++ {
			result, err = processor.Process(ctx, msg)
			if err == nil {
				handled = true
				break
			}

			lastError = err
			log.Warn(
				"failed to process event",
				"error", err,
				"attempt", attempt,
				"attempts", cfg.MaxAttempts,
			)
		}

		if !handled {
			dlq := events.DLQEvent{
				EventID:       uuid.NewString(),
				OriginalEvent: string(msg.Value),

				Reason:        "handler_failed",
				Error:         lastError.Error(),
				SourceTopic:   cfg.SourceTopic,
				ConsumerGroup: cfg.ConsumerGroupID,
				Attempts:      cfg.MaxAttempts,
				FailedAt:      time.Now().UTC(),
			}

			if err = dlqProducer.Write(ctx, string(msg.Key), dlq); err != nil {
				log.Error("failed to write dlq", "error", err)
				return err
			}

			log.Info("event published to dlq", "eventId", dlq.EventID)

			if err = consumer.Commit(ctx, msg); err != nil {
				log.Error("failed to commit message", "error", err)
				return err
			}

			continue
		}

		if result.Skip {
			if err = consumer.Commit(ctx, msg); err != nil {
				log.Error("failed to commit message", "error", err)
				return err
			}

			continue
		}

		if err := producer.Write(ctx, result.Key, result.Event); err != nil {
			log.Error("failed to write message", "error", err)
			return err
		}
		log.Info("published event", "topic", cfg.OutputTopic, "key", result.Key)

		if err := cfg.IdempotencyStore.Mark(ctx, result.EventID); err != nil {
			log.Error("failed to mark event as processed", "error", err)
			return err
		}

		if err = consumer.Commit(ctx, msg); err != nil {
			log.Error("failed to commit message", "error", err)
			return err
		}
	}
}
