package worker

import (
	"context"
	"log/slog"
	"time"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/kafka"
)

func processWithRetry(
	ctx context.Context,
	log *slog.Logger,
	msg kafka.Message,
	maxAttempts int,
	backoff time.Duration,
	handler Processor,
) (Result, error) {
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		res, lastErr := handler.Process(ctx, msg)
		if lastErr == nil {
			return res, nil
		}

		log.Warn(
			"failed to process event",
			"error", lastErr,
			"attempt", attempt,
			"attempts", maxAttempts,
		)

		if attempt == maxAttempts {
			break
		}

		select {
		case <-ctx.Done():
			return Result{}, ctx.Err()
		case <-time.After(backoff):
		}

		backoff *= 2
	}

	return Result{}, lastErr
}
