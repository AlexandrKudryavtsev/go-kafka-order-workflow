package config

import (
	"strings"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/logger"
)

func (c *Config) Normalize() {
	c.HTTP.Address = strings.TrimSpace(c.HTTP.Address)

	c.Logger.Format = logger.Format(
		strings.TrimSpace(strings.ToLower(string(c.Logger.Format))),
	)
	c.Logger.Level = strings.TrimSpace(strings.ToLower(c.Logger.Level))
	if c.Logger.Level == "warning" {
		c.Logger.Level = "warn"
	}

	brokers := make([]string, 0, len(c.Kafka.Brokers))
	for _, broker := range c.Kafka.Brokers {
		brokers = append(brokers, strings.TrimSpace(broker))
	}
	c.Kafka.Brokers = brokers
	c.Kafka.Topics.DeadLetterEvents = strings.TrimSpace(c.Kafka.Topics.DeadLetterEvents)
	c.Kafka.Topics.InventoryEvents = strings.TrimSpace(c.Kafka.Topics.InventoryEvents)
	c.Kafka.Topics.OrderEvents = strings.TrimSpace(c.Kafka.Topics.OrderEvents)
	c.Kafka.Topics.PaymentEvents = strings.TrimSpace(c.Kafka.Topics.PaymentEvents)
	c.Kafka.Topics.ShippingEvents = strings.TrimSpace(c.Kafka.Topics.ShippingEvents)

	c.Postgres.Database = strings.TrimSpace(c.Postgres.Database)
	c.Postgres.Host = strings.TrimSpace(c.Postgres.Host)
	c.Postgres.User = strings.TrimSpace(c.Postgres.User)
	c.Postgres.Password = strings.TrimSpace(c.Postgres.Password)
	c.Postgres.SSLMode = strings.ToLower(strings.TrimSpace(c.Postgres.SSLMode))
	if c.Postgres.SSLMode == "" {
		c.Postgres.SSLMode = "disable"
	}
}
