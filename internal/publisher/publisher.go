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
// Zombie connection detection is handled via NotifyClose listeners registered
// on every connect. When the broker closes the connection or channel
// unexpectedly (network drop, broker restart, heartbeat timeout), the Publisher
// marks itself as dirty so the next Publish call reconnects cleanly — without
// any knowledge required from callers.
//
// FIX: watchClose previously called closeUnsafe() directly from its goroutine,
// racing with in-flight Publish calls. The goroutine now only sets the dirty
// flag; closeUnsafe is only called from ensureConnected, which holds p.mu.
type Publisher struct {
	cfg    config.RabbitMQConfig
	logger *zap.Logger

	mu      sync.Mutex
	conn    *amqp.Connection
	channel *amqp.Channel
	dirty   bool // true when watchClose detected an unexpected close
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
    select {
    case <-ctx.Done():
        return &InfrastructureError{Cause: fmt.Errorf("context cancelled before publish: %w", ctx.Err())}
    default:
    }

    // Fase 1: setup — precisa do mutex para acessar p.channel com segurança.
    p.mu.Lock()
    if err := p.ensureConnected(); err != nil {
        p.mu.Unlock()
        return &InfrastructureError{Cause: err}
    }
    ch := p.channel
    confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))
    err := ch.PublishWithContext(
        ctx, p.cfg.Exchange, routingKey, true, false,
        amqp.Publishing{
            ContentType:  "application/json",
            DeliveryMode: amqp.Persistent,
            Timestamp:    time.Now(),
            Body:         body,
        },
    )
    if err != nil {
        p.resetUnsafe()
        p.mu.Unlock()
        return &InfrastructureError{Cause: fmt.Errorf("publish: %w", err)}
    }
    p.mu.Unlock() // ← libera ANTES de esperar o confirm
    
    // Fase 2: aguarda confirm — sem mutex, outros workers podem publicar em paralelo.
    select {
    case conf, ok := <-confirms:
        if !ok {
            p.mu.Lock()
            p.resetUnsafe()
            p.mu.Unlock()
            return &InfrastructureError{Cause: fmt.Errorf("confirms channel closed before ack")}
        }
        if !conf.Ack {
            p.mu.Lock()
            p.resetUnsafe()
            p.mu.Unlock()
            return &InfrastructureError{Cause: fmt.Errorf("broker nacked delivery tag %d", conf.DeliveryTag)}
        }
        return nil
    case <-time.After(5 * time.Second):
        p.mu.Lock()
        p.resetUnsafe()
        p.mu.Unlock()
        return &InfrastructureError{Cause: fmt.Errorf("timeout waiting for broker confirm")}
    case <-ctx.Done():
        p.mu.Lock()
        p.resetUnsafe()
        p.mu.Unlock()
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
	p.dirty = false

	// Watch for unexpected closes in background goroutines.
	p.watchClose(conn, ch)

	return nil
}

// watchClose starts a background goroutine that listens for unexpected
// connection/channel closes and marks the Publisher as dirty so the next
// Publish call triggers a clean reconnect.
//
// FIX (was): previously called closeUnsafe() directly from this goroutine,
// which raced with in-flight Publish calls. Now it only sets p.dirty = true.
// closeUnsafe is only ever called from ensureConnected (which holds p.mu),
// keeping teardown and reconnect serialised with all publish operations.
func (p *Publisher) watchClose(conn *amqp.Connection, ch *amqp.Channel) {
	connClose := conn.NotifyClose(make(chan *amqp.Error, 1))
	chanClose := ch.NotifyClose(make(chan *amqp.Error, 1))

	go func() {
		var reason string

		select {
		case err, ok := <-connClose:
			if !ok {
				// Closed cleanly via p.Close() — nothing to do.
				return
			}
			reason = err.Error()
		case err, ok := <-chanClose:
			if !ok {
				return
			}
			reason = err.Error()
		}

		p.logger.Warn("rabbitmq connection/channel closed unexpectedly, marking dirty — will reconnect on next publish",
			zap.String("reason", reason),
		)

		// Only set the flag; do NOT call closeUnsafe here.
		// The next Publish call will call ensureConnected (under p.mu),
		// which checks dirty, tears down safely, and reconnects.
		p.mu.Lock()
		p.dirty = true
		p.mu.Unlock()
	}()
}

// ensureConnected reconnects if the connection is closed or marked dirty.
// Must be called with p.mu held.
func (p *Publisher) ensureConnected() error {
	if !p.dirty && p.conn != nil && !p.conn.IsClosed() && p.channel != nil {
		return nil
	}

	p.logger.Warn("rabbitmq reconnecting",
		zap.Bool("dirty_flag", p.dirty),
		zap.Bool("conn_nil", p.conn == nil),
	)

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
	p.dirty = false
}
