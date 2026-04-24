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

// InfrastructureError signals that the failure is in the messaging layer,
// not in the event payload — triggers infrastructure retry logic.
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

// Publisher manages a single AMQP connection and channel, publishing messages
// with publisher confirms. It is safe for concurrent use.
//
// Zombie connection detection is handled internally via NotifyClose listeners
// registered on every connect. When the broker closes the connection or channel
// unexpectedly (network drop, broker restart, heartbeat timeout), the Publisher
// marks itself as dirty so the next Publish call reconnects cleanly — without
// any knowledge required from callers.
type Publisher struct {
	cfg    config.RabbitMQConfig
	logger *zap.Logger

	mu      sync.Mutex
	conn    *amqp.Connection
	channel *amqp.Channel
}

// New creates a Publisher and establishes the initial connection.
func New(cfg config.RabbitMQConfig, logger *zap.Logger) (*Publisher, error) {
	p := &Publisher{cfg: cfg, logger: logger}
	if err := p.connect(); err != nil {
		return nil, err
	}
	return p, nil
}

// Close shuts down the channel and connection gracefully.
func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closeUnsafe()
}

// Publish sends a message to the exchange with the given routing key.
// It waits for a broker confirm before returning. If the broker or connection
// is unavailable, it returns an InfrastructureError so the caller can apply
// the appropriate retry policy.
func (p *Publisher) Publish(ctx context.Context, routingKey string, body []byte) error {
	// Fail fast if the context is already done before acquiring the lock.
	select {
	case <-ctx.Done():
		return &InfrastructureError{Cause: fmt.Errorf("context cancelled before publish: %w", ctx.Err())}
	default:
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.ensureConnected(); err != nil {
		return &InfrastructureError{Cause: err}
	}

	// Register a confirm listener BEFORE publishing — must be buffered
	// with capacity >= number of messages in flight (1 here).
	confirms := p.channel.NotifyPublish(make(chan amqp.Confirmation, 1))

	err := p.channel.PublishWithContext(
		ctx,
		p.cfg.Exchange,
		routingKey,
		true,  // mandatory — return if no queue matches
		false, // immediate — not supported in RabbitMQ 3+
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         body,
		},
	)
	if err != nil {
		p.resetUnsafe()
		return &InfrastructureError{Cause: fmt.Errorf("publish: %w", err)}
	}

	// Wait for broker acknowledgement with a timeout.
	select {
	case conf, ok := <-confirms:
		if !ok {
			p.resetUnsafe()
			return &InfrastructureError{Cause: fmt.Errorf("confirms channel closed before ack")}
		}
		if !conf.Ack {
			p.resetUnsafe()
			return &InfrastructureError{Cause: fmt.Errorf("broker nacked delivery tag %d", conf.DeliveryTag)}
		}
		return nil
	case <-time.After(5 * time.Second):
		p.resetUnsafe()
		return &InfrastructureError{Cause: fmt.Errorf("timeout waiting for broker confirm")}
	case <-ctx.Done():
		p.resetUnsafe()
		return &InfrastructureError{Cause: fmt.Errorf("context cancelled waiting for confirm: %w", ctx.Err())}
	}
}

// connect opens the AMQP connection and channel, registers NotifyClose
// listeners for zombie detection, and enables publisher confirms.
// Must be called with p.mu held or before the publisher is shared.
func (p *Publisher) connect() error {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		p.cfg.User, p.cfg.Password, p.cfg.Host, p.cfg.Port, p.cfg.VHost,
	)

	// DialConfig with explicit timeouts so a hung broker never blocks forever.
	// Heartbeat: broker and client exchange pings every 10s — if either side
	// misses, the connection is closed and NotifyClose fires automatically.
	conn, err := amqp.DialConfig(url, amqp.Config{
		Heartbeat: 10 * time.Second,
		Dial:      amqp.DefaultDial(10 * time.Second),
	})
	if err != nil {
		return fmt.Errorf("dial rabbitmq: %w", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return fmt.Errorf("open channel: %w", err)
	}

	// Enable publisher confirms on this channel.
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		conn.Close()
		return fmt.Errorf("enable confirms: %w", err)
	}

	p.conn = conn
	p.channel = ch

	// Watch for unexpected closes in background goroutines.
	// When the broker drops the connection or channel (network failure,
	// broker restart, missed heartbeat), these listeners fire and mark
	// the Publisher as dirty. The next Publish call will then reconnect
	// cleanly via ensureConnected — callers never need to know this happened.
	p.watchClose(conn, ch)

	return nil
}

// watchClose starts two background goroutines that listen for unexpected
// connection/channel closes and reset the Publisher state so the next
// Publish call triggers a clean reconnect.
//
// This is the core zombie-connection fix: instead of callers checking if
// the connection is alive, the Publisher proactively detects and clears
// stale state the moment the broker signals a problem.
func (p *Publisher) watchClose(conn *amqp.Connection, ch *amqp.Channel) {
	connClose := conn.NotifyClose(make(chan *amqp.Error, 1))
	chanClose := ch.NotifyClose(make(chan *amqp.Error, 1))

	go func() {
		select {
		case err, ok := <-connClose:
			if !ok {
				// closed cleanly via p.Close() — nothing to do
				return
			}
			p.logger.Warn("rabbitmq connection closed unexpectedly, will reconnect on next publish",
				zap.String("reason", err.Error()),
			)
		case err, ok := <-chanClose:
			if !ok {
				return
			}
			p.logger.Warn("rabbitmq channel closed unexpectedly, will reconnect on next publish",
				zap.String("reason", err.Error()),
			)
		}

		// Mark the publisher as dirty — ensureConnected will reconnect.
		p.mu.Lock()
		p.closeUnsafe()
		p.mu.Unlock()
	}()
}

// ensureConnected reconnects if the connection or channel is closed.
// Must be called with p.mu held.
func (p *Publisher) ensureConnected() error {
	if p.conn != nil && !p.conn.IsClosed() && p.channel != nil {
		return nil
	}

	p.logger.Warn("rabbitmq connection lost, reconnecting")
	p.closeUnsafe()
	return p.connect()
}

// resetUnsafe marks the connection as dirty so the next call reconnects.
// Must be called with p.mu held.
func (p *Publisher) resetUnsafe() {
	p.closeUnsafe()
}

// closeUnsafe tears down channel and connection without acquiring the mutex.
// Must be called with p.mu held.
func (p *Publisher) closeUnsafe() {
	if p.channel != nil {
		p.channel.Close()
		p.channel = nil
	}
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
}
