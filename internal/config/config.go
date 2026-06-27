package config

import (
	"fmt"
	"os"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/logger"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/pkg/postgres"
	"gopkg.in/yaml.v3"
)

type Config struct {
	HTTP          HTTPConfig          `yaml:"http"`
	Logger        logger.Config       `yaml:"logger"`
	Kafka         KafkaConfig         `yaml:"kafka"`
	Retry         RetryConfig         `yaml:"retry"`
	Postgres      postgres.Config     `yaml:"postgres"`
	Observability ObservabilityConfig `yaml:"observability"`
}

type HTTPConfig struct {
	Address         string   `yaml:"address"`
	ReadTimeout     Duration `yaml:"read_timeout"`
	WriteTimeout    Duration `yaml:"write_timeout"`
	IdleTimeout     Duration `yaml:"idle_timeout"`
	ShutdownTimeout Duration `yaml:"shutdown_timeout"`
}

type KafkaConfig struct {
	Brokers []string          `yaml:"brokers"`
	Topics  KafkaTopicsConfig `yaml:"topics"`
}

type KafkaTopicsConfig struct {
	OrderEvents      string `yaml:"order_events"`
	InventoryEvents  string `yaml:"inventory_events"`
	PaymentEvents    string `yaml:"payment_events"`
	ShippingEvents   string `yaml:"shipping_events"`
	DeadLetterEvents string `yaml:"dead_letter_events"`
}

type RetryConfig struct {
	MaxRetries int      `yaml:"max_retries"`
	Backoff    Duration `yaml:"backoff"`
}

type ObservabilityConfig struct {
	Address string `yaml:"address"`
}

func New(path string) (*Config, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("failed to open config file: %w", err)
	}
	defer file.Close()

	var cfg Config

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)

	if err = decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("failed to decode config: %w", err)
	}

	return &cfg, nil
}
