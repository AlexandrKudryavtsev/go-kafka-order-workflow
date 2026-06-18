package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/config"
	"github.com/AlexandrKudryavtsev/go-kafka-order-workflow/internal/orderapi"
)

func main() {
	configPath := flag.String("config", "./config.yaml", "path to config")
	httpAddress := flag.String("http-address", "", "http address")
	logLevel := flag.String("log-level", "", "logger level")

	flag.Parse()

	cfg, err := config.New(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to create config: %v\n", err)
		os.Exit(1)
	}

	if *httpAddress != "" {
		cfg.HTTP.Address = *httpAddress
	}
	if *logLevel != "" {
		cfg.Logger.Level = *logLevel
	}

	cfg.Normalize()
	if err = cfg.Validate(); err != nil {
		fmt.Fprintf(os.Stderr, "invalid config: %v\n", err)
		os.Exit(1)
	}

	if err = orderapi.Run(cfg); err != nil {
		os.Exit(1)
	}
}
