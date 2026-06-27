package inventory

import (
	"context"
	"encoding/json"
	"net/http"
	"os/signal"
	"syscall"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/config"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/idempotency"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/observability"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/worker"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/httpserver"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/logger"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/postgres"
)

const serviceName = "inventory-service"

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

	metrics := observability.New(serviceName)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /debug/state", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(metrics.State())
	})

	srv := httpserver.New(mux, httpserver.Address(cfg.Observability.Address))
	srv.Start()
	defer func() {
		if err := srv.Shutdown(); err != nil {
			log.Error("failed to shutdown observability server", "error", err)
		}
	}()

	workerErr := make(chan error, 1)
	go func() {
		workerErr <- worker.Run(ctx, worker.Config{
			ServiceName:      serviceName,
			SourceTopic:      cfg.Kafka.Topics.OrderEvents,
			OutputTopic:      cfg.Kafka.Topics.InventoryEvents,
			DLQTopic:         cfg.Kafka.Topics.DeadLetterEvents,
			Brokers:          cfg.Kafka.Brokers,
			ConsumerGroupID:  groupID,
			MaxAttempts:      cfg.Retry.MaxRetries,
			Backoff:          cfg.Retry.Backoff.Duration,
			Logger:           log,
			IdempotencyStore: store,
			Observability:    metrics,
		}, processor)
	}()

	select {
	case err := <-workerErr:
		return err

	case err := <-srv.Notify():
		return err

	case <-ctx.Done():
		return nil
	}
}
