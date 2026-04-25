package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all configuration for the relay service.
type Config struct {
	Postgres  PostgresConfig
	RabbitMQ  RabbitMQConfig
	Relay     RelayConfig
	LogLevel  string   // ← NOVO
}

type PostgresConfig struct {
	DSN string
}

type RabbitMQConfig struct {
	Host     string
	Port     int
	User     string
	Password string
	VHost    string
	Exchange string
}

type RelayConfig struct {
	BatchSize         int
	PollInterval      time.Duration
	WorkerCount       int
	ProcessingTimeout time.Duration
}

func Load() (*Config, error) {
	var errs []string

	require := func(key string) string {
		v := os.Getenv(key)
		if v == "" {
			errs = append(errs, fmt.Sprintf("missing required env var: %s", key))
		}
		return v
	}

	optionalInt := func(key string, defaultVal int) int {
		v := os.Getenv(key)
		if v == "" {
			return defaultVal
		}
		n, err := strconv.Atoi(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("invalid int for %s: %s", key, v))
			return defaultVal
		}
		return n
	}

	optionalDuration := func(key string, defaultVal time.Duration) time.Duration {
		v := os.Getenv(key)
		if v == "" {
			return defaultVal
		}
		d, err := time.ParseDuration(v)
		if err != nil {
			errs = append(errs, fmt.Sprintf("invalid duration for %s: %s", key, v))
			return defaultVal
		}
		return d
	}

	rabbitPort := optionalInt("RABBITMQ_PORT", 5672)

	cfg := &Config{
		Postgres: PostgresConfig{
			DSN: require("DATABASE_URL"),
		},
		RabbitMQ: RabbitMQConfig{
			Host:     require("RABBITMQ_HOST"),
			Port:     rabbitPort,
			User:     require("RABBITMQ_USER"),
			Password: require("RABBITMQ_PASSWORD"),
			VHost:    getEnvOrDefault("RABBITMQ_VHOST", "/"),
			Exchange: getEnvOrDefault("RABBITMQ_EXCHANGE", "doc_validator.events"),
		},
		Relay: RelayConfig{
			BatchSize:         optionalInt("RELAY_BATCH_SIZE", 100),
			PollInterval:      optionalDuration("RELAY_POLL_INTERVAL", 3*time.Second),
			WorkerCount:       optionalInt("RELAY_WORKER_COUNT", 10),
			ProcessingTimeout: optionalDuration("RELAY_PROCESSING_TIMEOUT", 5*time.Minute),
		},
		LogLevel: getEnvOrDefault("LOG_LEVEL", "info"), // ← NOVO
	}

	if len(errs) > 0 {
		return nil, fmt.Errorf("configuration errors:\n  - %s", joinStrings(errs, "\n  - "))
	}

	return cfg, nil
}

func getEnvOrDefault(key, defaultVal string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return defaultVal
}

func joinStrings(ss []string, sep string) string {
	result := ""
	for i, s := range ss {
		if i > 0 {
			result += sep
		}
		result += s
	}
	return result
}
