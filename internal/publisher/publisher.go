package publisher

import (
	"context"
	"fmt"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/doc-validator/relay/internal/config"
)

type InfrastructureError struct {
	Cause error
}

func (e *InfrastructureError) Error() string {
	if e.Cause == nil {
		return "rabbitmq infrastructure error"
	}
	return fmt.Sprintf("rabbitmq infrastructure error: %s", e.Cause.Error())
}

func (e *InfrastructureError) Unwrap() error { return e.Cause }

type Publisher struct {
	cfg    config.RabbitMQConfig
	logger *zap.Logger

	mu    sync.Mutex
	conn  *amqp.Connection
	dirty bool
}

func New(cfg config.RabbitMQConfig, logger *zap.Logger) (*Publisher, error) {
	p := &Publisher{cfg: cfg, logger: logger}
	if err := p.connect(); err != nil {
		return nil, err
	}
	return p, nil
}

func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closeUnsafe()
}

func (p *Publisher) Publish(ctx context.Context, routingKey string, body []byte) error {
	start := time.Now()

	p.logger.Debug("publish attempt",
		zap.String("routing_key", routingKey),
		zap.ByteString("body_preview", truncate(body, 200)),
	)

	p.mu.Lock()
	if err := p.ensureConnected(); err != nil {
		p.mu.Unlock()
		p.logger.Error("connection not ready", zap.Error(err))
		return &InfrastructureError{Cause: err}
	}
	conn := p.conn
	p.mu.Unlock()

	ch, err := conn.Channel()
	if err != nil {
		return &InfrastructureError{Cause: fmt.Errorf("open channel: %w", err)}
	}
	defer ch.Close()

	if err := ch.Confirm(false); err != nil {
		return &InfrastructureError{Cause: fmt.Errorf("enable confirms: %w", err)}
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	err = ch.PublishWithContext(
		ctx,
		p.cfg.Exchange,
		routingKey,
		true,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         body,
		},
	)
	if err != nil {
		p.logger.Error("publish failed", zap.Error(err))
		return &InfrastructureError{Cause: fmt.Errorf("publish: %w", err)}
	}

	select {
	case conf, ok := <-confirms:
		if !ok {
			p.logger.Error("confirms channel closed")
			return &InfrastructureError{Cause: fmt.Errorf("confirms channel closed")}
		}
		if !conf.Ack {
			p.logger.Warn("broker nacked message")
			return &InfrastructureError{Cause: fmt.Errorf("broker nacked delivery")}
		}
		p.logger.Info("message published",
			zap.String("routing_key", routingKey),
			zap.Duration("duration_ms", time.Since(start)),
			zap.Uint64("delivery_tag", conf.DeliveryTag),
		)
		return nil
	case <-time.After(5 * time.Second):
		p.logger.Error("publish timeout", zap.Duration("timeout", 5*time.Second))
		return &InfrastructureError{Cause: fmt.Errorf("timeout waiting for ack")}
	case <-ctx.Done():
		p.logger.Warn("publish cancelled", zap.Error(ctx.Err()))
		return &InfrastructureError{Cause: ctx.Err()}
	}
}

func (p *Publisher) connect() error {
	p.logger.Info("connecting to rabbitmq",
		zap.String("host", p.cfg.Host),
		zap.Int("port", p.cfg.Port),
	)

	url := fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		p.cfg.User, p.cfg.Password, p.cfg.Host, p.cfg.Port, p.cfg.VHost,
	)

	conn, err := amqp.DialConfig(url, amqp.Config{
		Heartbeat: 10 * time.Second,
		Dial:      amqp.DefaultDial(10 * time.Second),
	})
	if err != nil {
		p.logger.Error("rabbitmq connection failed", zap.Error(err))
		return err
	}

	p.conn = conn
	p.dirty = false
	p.watchClose(conn)

	p.logger.Info("rabbitmq connected")
	return nil
}

func (p *Publisher) watchClose(conn *amqp.Connection) {
	connClose := conn.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		err, ok := <-connClose
		if !ok {
			return
		}
		p.logger.Error("rabbitmq connection lost",
			zap.Error(err),
			zap.String("reason", err.Reason),
			zap.Int("code", err.Code),
		)
		p.mu.Lock()
		p.dirty = true
		p.mu.Unlock()
	}()
}

func (p *Publisher) ensureConnected() error {
	if !p.dirty && p.conn != nil && !p.conn.IsClosed() {
		return nil
	}
	p.closeUnsafe()
	return p.connect()
}

func (p *Publisher) closeUnsafe() {
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
	p.dirty = false
}

func truncate(b []byte, max int) []byte {
	if len(b) <= max {
		return b
	}
	return append(b[:max], []byte("...")...)
}
