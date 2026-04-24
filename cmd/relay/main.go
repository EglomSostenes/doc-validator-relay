package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"go.uber.org/zap"

	"github.com/doc-validator/relay/internal/config"
	"github.com/doc-validator/relay/internal/db"
	"github.com/doc-validator/relay/internal/outbox"
	"github.com/doc-validator/relay/internal/publisher"
)

func main() {
	// Load .env file if present (development convenience, ignored in production
	// where env vars are injected by Docker / the orchestrator).
	_ = godotenv.Load()

	logger := buildLogger()
	defer logger.Sync() //nolint:errcheck

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("invalid configuration", zap.Error(err))
	}

	// Root context cancelled on SIGINT / SIGTERM for graceful shutdown.
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── Database ────────────────────────────────────────────────────────────
	pool, err := db.New(ctx, cfg.Postgres)
	if err != nil {
		logger.Fatal("cannot connect to postgres", zap.Error(err))
	}
	defer pool.Close()
	logger.Info("postgres connected")

	// ── RabbitMQ topology ───────────────────────────────────────────────────
	// Provision is idempotent: safe to run on every startup.
	// topology := publisher.DefaultTopology(cfg.RabbitMQ)
	// if err := provisionWithRetry(cfg.RabbitMQ, topology, logger); err != nil {
		// logger.Fatal("cannot provision rabbitmq topology", zap.Error(err))
	// }
	// logger.Info("rabbitmq topology ready")

	// ── Publisher ───────────────────────────────────────────────────────────
	pub, err := publisher.New(cfg.RabbitMQ, logger)
	if err != nil {
		logger.Fatal("cannot create rabbitmq publisher", zap.Error(err))
	}
	defer pub.Close()
	logger.Info("rabbitmq publisher ready")

	// ── Outbox relay ────────────────────────────────────────────────────────
	processor := outbox.NewProcessor(pub, pool, logger)
	poller := outbox.NewPoller(pool, processor, cfg.Relay, logger)

	logger.Info("doc-validator-relay starting")
	poller.Run(ctx) // blocks until ctx is cancelled

	logger.Info("doc-validator-relay stopped gracefully")
}

// provisionWithRetry retries RabbitMQ provisioning up to 10 times with a
// 3-second back-off. This handles the race between the service container and
// the RabbitMQ container during docker-compose startup.
func provisionWithRetry(cfg config.RabbitMQConfig, topology publisher.Topology, logger *zap.Logger) error {
	const maxAttempts = 10
	const backoff = 3 * time.Second

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		err := publisher.Provision(cfg, topology, logger)
		if err == nil {
			return nil
		}
		logger.Warn("rabbitmq not ready, retrying",
			zap.Int("attempt", attempt),
			zap.Int("max_attempts", maxAttempts),
			zap.Duration("backoff", backoff),
			zap.Error(err),
		)
		time.Sleep(backoff)
	}

	return publisher.Provision(cfg, topology, logger) // final attempt, return real error
}

func buildLogger() *zap.Logger {
	if os.Getenv("RAILS_ENV") == "production" || os.Getenv("GO_ENV") == "production" {
		l, _ := zap.NewProduction()
		return l
	}
	l, _ := zap.NewDevelopment()
	return l
}
