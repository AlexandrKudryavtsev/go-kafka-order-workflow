package worker

import (
	"context"
	"errors"
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
	var res Result

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		res, lastErr = handler.Process(ctx, msg)
		if lastErr == nil {
			return res, nil
		}

		log.Warn(
			"failed to process event",
			"error", lastErr,
			"attempt", attempt,
			"attempts", maxAttempts,
		)

		var processingError *ProcessingError

		if errors.As(lastErr, &processingError) &&
			processingError.Kind == NonRetryableError {
			break
		}

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
