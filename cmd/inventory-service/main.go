package main

import (
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/config"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/inventory"
)

func main() {
	configPath := flag.String("config", "./config.yaml", "path to config")
	groupID := flag.String("group-id", "inventory-service", "kafka consumer group id")

	flag.Parse()

	workerGroupID := strings.TrimSpace(*groupID)
	if workerGroupID == "" {
		fmt.Fprintf(os.Stderr, "invalid group-id\n")
		os.Exit(1)
	}

	cfg, err := config.New(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create config: %v\n", err)
		os.Exit(1)
	}

	cfg.Normalize()
	if err = cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid config: %v\n", err)
		os.Exit(1)
	}

	if err = inventory.Run(cfg, workerGroupID); err != nil {
		os.Exit(1)
	}
}
