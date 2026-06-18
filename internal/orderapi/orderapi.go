package orderapi

import (
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/config"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/httpserver"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/kafka"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/logger"
)

func Run(cfg *config.Config) error {
	log := logger.New(cfg.Logger)

	mux := http.NewServeMux()

	producer := kafka.NewProducer(cfg.Kafka.Topics.OrderEvents, cfg.Kafka.Brokers)
	defer func() {
		if err := producer.Close(); err != nil {
			log.Error("failed to close producer", "error", err)
		}
	}()

	handler := NewHandler(log, producer)
	handler.Register(mux)

	server := httpserver.New(
		mux,
		httpserver.Address(cfg.HTTP.Address),
		httpserver.ReadTimeout(cfg.HTTP.ReadTimeout.Duration),
		httpserver.WriteTimeout(cfg.HTTP.WriteTimeout.Duration),
		httpserver.IdleTimeout(cfg.HTTP.IdleTimeout.Duration),
		httpserver.ShutdownTimeout(cfg.HTTP.ShutdownTimeout.Duration),
	)
	server.Start()
	log.Info("starting http server", "address", cfg.HTTP.Address)

	notify := server.Notify()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)

	select {
	case err, ok := <-notify:
		if ok && err != nil {
			log.Error("http server error", "error", err)
			return err
		}

		log.Info("http server stopped")
	case sig := <-quit:
		log.Info("received signal", "signal", sig)
		if err := server.Shutdown(); err != nil {
			log.Error("http server shutdown error", "error", err)
			return err
		}

		err, ok := <-notify
		if ok && err != nil {
			log.Error("http server error", "error", err)
			return err
		}

		log.Info("http server stopped")
	}

	return nil
}
