package shipping

import (
	"context"
	"os/signal"
	"syscall"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/config"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/worker"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/logger"
)

func Run(cfg *config.Config, groupID string) error {
	log := logger.New(cfg.Logger)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	handler := NewHandler()
	processor := NewProcessor(handler)

	return worker.Run(ctx, worker.Config{
		ServiceName:     "shipping-service",
		SourceTopic:     cfg.Kafka.Topics.PaymentEvents,
		OutputTopic:     cfg.Kafka.Topics.ShippingEvents,
		DLQTopic:        cfg.Kafka.Topics.DeadLetterEvents,
		Brokers:         cfg.Kafka.Brokers,
		ConsumerGroupID: groupID,
		MaxAttempts:     cfg.Retry.MaxRetries,
		Logger:          log,
	}, processor)
}
