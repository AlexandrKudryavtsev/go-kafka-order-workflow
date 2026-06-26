package payment

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/config"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/idempotency"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/worker"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/logger"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/postgres"
)

const serviceName = "payment-service"

func Run(cfg *config.Config, groupID string) error {
	log := logger.New(cfg.Logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	db, err := postgres.New(ctx, cfg.Postgres)
	if err != nil {
		return err
	}
	defer db.Close()

	store, err := idempotency.NewPostgresStore(db.Pool(), serviceName)
	if err != nil {
		return err
	}
	if err := store.Init(ctx); err != nil {
		return err
	}
	handler := NewHandler()
	processor := NewProcessor(handler, store)

	return worker.Run(ctx, worker.Config{
		ServiceName:      serviceName,
		SourceTopic:      cfg.Kafka.Topics.InventoryEvents,
		OutputTopic:      cfg.Kafka.Topics.PaymentEvents,
		DLQTopic:         cfg.Kafka.Topics.DeadLetterEvents,
		Brokers:          cfg.Kafka.Brokers,
		ConsumerGroupID:  groupID,
		MaxAttempts:      cfg.Retry.MaxRetries,
		Logger:           log,
		IdempotencyStore: store,
	}, processor)
}
