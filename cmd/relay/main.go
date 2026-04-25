package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"go.uber.org/zap"

	"github.com/doc-validator/relay/internal/config"
	"github.com/doc-validator/relay/internal/db"
	"github.com/doc-validator/relay/internal/logger"
	"github.com/doc-validator/relay/internal/outbox"
	"github.com/doc-validator/relay/internal/publisher"
)

func main() {
	// Load .env file if present
	_ = godotenv.Load()

	// Carregar configuração (inclui LOG_LEVEL)
	cfg, err := config.Load()
	if err != nil {
		panic("failed to load config: " + err.Error())
	}

	// Criar logger estruturado baseado no LOG_LEVEL
	log, err := logger.New(cfg.LogLevel)
	if err != nil {
		panic("failed to create logger: " + err.Error())
	}
	defer log.Sync()

	// Root context cancelled on SIGINT / SIGTERM
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// ── Database ────────────────────────────────────────────────────────────
	pool, err := db.New(ctx, cfg.Postgres)
	if err != nil {
		log.Fatal("cannot connect to postgres", zap.Error(err))
	}
	defer pool.Close()
	log.Info("postgres connected")

	// ── RabbitMQ topology (provisionado via Docker definitions.json) ────────
	// (nada a fazer aqui)

	// ── Publisher ───────────────────────────────────────────────────────────
	pub, err := publisher.New(cfg.RabbitMQ, log.Named("publisher"))
	if err != nil {
		log.Fatal("cannot create rabbitmq publisher", zap.Error(err))
	}
	defer pub.Close()
	log.Info("rabbitmq publisher ready")

	// ── Outbox relay ────────────────────────────────────────────────────────
	processor := outbox.NewProcessor(pub, pool, log.Named("processor"))
	poller := outbox.NewPoller(pool, processor, cfg.Relay, log.Named("poller"))

	log.Info("doc-validator-relay starting",
		zap.String("log_level", cfg.LogLevel),
		zap.Int("batch_size", cfg.Relay.BatchSize),
	)
	poller.Run(ctx) // blocks until ctx is cancelled

	log.Info("doc-validator-relay stopped gracefully")
}
