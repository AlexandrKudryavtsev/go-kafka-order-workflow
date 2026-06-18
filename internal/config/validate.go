package config

import (
	"errors"
	"fmt"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/logger"
)

func (c *Config) Validate() error {
	// http
	if c.HTTP.Address == "" {
		return errors.New("invalid http address")
	}
	if c.HTTP.ReadTimeout.Duration <= 0 {
		return errors.New("invalid read_timeout")
	}
	if c.HTTP.WriteTimeout.Duration <= 0 {
		return errors.New("invalid write_timeout")
	}
	if c.HTTP.IdleTimeout.Duration <= 0 {
		return errors.New("invalid idle_timeout")
	}
	if c.HTTP.ShutdownTimeout.Duration <= 0 {
		return errors.New("invalid shutdown_timeout")
	}

	// logger
	switch c.Logger.Level {
	case "info", "debug", "warn", "error":
	default:
		return errors.New("invalid logger level")
	}

	switch c.Logger.Format {
	case logger.JSONFormat, logger.TextFormat:
	default:
		return errors.New("invalid logger format")
	}

	// kafka
	if len(c.Kafka.Brokers) == 0 {
		return fmt.Errorf("empty kafka brokers")
	}

	seenBrokers := make(map[string]struct{}, len(c.Kafka.Brokers))
	for i, broker := range c.Kafka.Brokers {
		if broker == "" {
			return fmt.Errorf("broker %d: invalid name", i)
		}
		if _, ok := seenBrokers[broker]; ok {
			return fmt.Errorf("broker %d: duplicate name", i)
		}

		seenBrokers[broker] = struct{}{}
	}

	if c.Kafka.Topics.OrderEvents == "" {
		return errors.New("invalid order_events topic")
	}
	if c.Kafka.Topics.InventoryEvents == "" {
		return errors.New("invalid inventory_events topic")
	}
	if c.Kafka.Topics.PaymentEvents == "" {
		return errors.New("invalid payment_events topic")
	}
	if c.Kafka.Topics.ShippingEvents == "" {
		return errors.New("invalid shipping_events topic")
	}
	if c.Kafka.Topics.DeadLetterEvents == "" {
		return errors.New("invalid dead_letter_events topic")
	}

	// retry
	if c.Retry.MaxRetries <= 0 {
		return errors.New("invalid max_retries")
	}

	return nil
}
