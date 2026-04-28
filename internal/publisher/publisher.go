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

// InfrastructureError wraps any RabbitMQ / broker-level failure so the retry
// layer can distinguish it from a business-logic failure.
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

// pooledChannel is a pre-opened AMQP channel that already has publisher
// confirms enabled. It is owned by the pool and must never be closed by
// callers — return it via p.pool <- pc or discard it via pc.ch.Close().
type pooledChannel struct {
	ch       *amqp.Channel
	confirms chan amqp.Confirmation
}

// Publisher manages a single AMQP connection and a fixed-size pool of
// channels (one per worker). The pool size equals cfg.WorkerCount so there
// is always exactly one channel available per in-flight Publish call,
// avoiding both channel-per-call overhead and unbounded growth.
//
// Concurrency model:
//   - mu protects conn, dirty, and all pool mutations (buildPool / drainPool).
//   - The pool itself is a buffered channel used as a semaphore+queue:
//     workers block on <-p.pool until a channel is available.
//   - When the connection is lost (dirty=true), the next Publish call
//     acquires mu, drains and rebuilds the pool, then proceeds normally.
//   - Channels that are in-flight when dirty is set are closed and discarded
//     after use; a replacement is opened if the connection is already healthy
//     again, keeping the pool size at WorkerCount.
type Publisher struct {
	cfg         config.RabbitMQConfig
	workerCount int
	logger      *zap.Logger

	mu    sync.Mutex
	conn  *amqp.Connection
	dirty bool

	// pool is a buffered channel of size workerCount.
	// It doubles as both a semaphore (blocks when all slots are taken) and a
	// FIFO queue (oldest-returned channels are reused first, warming the TCP
	// send buffer).
	pool chan *pooledChannel
}

// New creates a Publisher, establishes the AMQP connection, and fills the
// channel pool. Returns an error if either step fails — the caller should
// treat this as a fatal startup error.
func New(cfg config.RabbitMQConfig, workerCount int, logger *zap.Logger) (*Publisher, error) {
	p := &Publisher{
		cfg:         cfg,
		workerCount: workerCount,
		logger:      logger,
		pool:        make(chan *pooledChannel, workerCount),
	}

	if err := p.connect(); err != nil {
		return nil, err
	}

	if err := p.buildPool(); err != nil {
		// Partial pool — close the connection cleanly before returning.
		p.closeUnsafe()
		return nil, fmt.Errorf("build channel pool: %w", err)
	}

	return p, nil
}

// Close drains the pool, closes all channels, and closes the connection.
// Safe to call multiple times.
func (p *Publisher) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.drainPool()
	p.closeUnsafe()
}

// Publish sends body to the given routingKey and waits for a broker ack.
//
// Flow:
//  1. Ensure the connection (and pool) are healthy, reconnecting if dirty.
//  2. Block until a pooled channel is available (or ctx is cancelled).
//  3. Publish the message.
//  4. Wait for the broker ack / nack (or timeout / ctx cancel).
//  5. Return the channel to the pool on success; discard and attempt to
//     replace it on failure so the pool stays at WorkerCount.
func (p *Publisher) Publish(ctx context.Context, routingKey string, body []byte) error {
	start := time.Now()

	p.logger.Debug("publish attempt",
		zap.String("routing_key", routingKey),
		zap.ByteString("body_preview", truncate(body, 200)),
	)

	// Step 1 — ensure connection and pool are healthy.
	p.mu.Lock()
	if err := p.ensureConnected(); err != nil {
		p.mu.Unlock()
		p.logger.Error("connection not ready", zap.Error(err))
		return &InfrastructureError{Cause: err}
	}
	p.mu.Unlock()

	// Step 2 — acquire a pooled channel.
	// Blocks if all WorkerCount slots are in use, providing natural
	// back-pressure that matches the semaphore in the Poller.
	var pc *pooledChannel
	select {
	case pc = <-p.pool:
	case <-ctx.Done():
		return &InfrastructureError{Cause: ctx.Err()}
	}

	// Step 3 — publish.
	err := pc.ch.PublishWithContext(
		ctx,
		p.cfg.Exchange,
		routingKey,
		true,  // mandatory — broker must route the message
		false, // immediate — not supported in RabbitMQ 3.x+
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         body,
		},
	)
	if err != nil {
		p.logger.Error("publish write failed",
			zap.String("routing_key", routingKey),
			zap.Error(err),
		)
		// Discard the channel — its state after a write error is undefined.
		pc.ch.Close()
		p.replaceChannel()
		return &InfrastructureError{Cause: fmt.Errorf("publish: %w", err)}
	}

	// Step 4 — wait for the broker acknowledgement.
	var publishErr error
	select {
	case conf, ok := <-pc.confirms:
		if !ok {
			publishErr = &InfrastructureError{Cause: fmt.Errorf("confirms channel closed")}
		} else if !conf.Ack {
			publishErr = &InfrastructureError{Cause: fmt.Errorf("broker nacked delivery tag %d", conf.DeliveryTag)}
		}
	case <-time.After(5 * time.Second):
		publishErr = &InfrastructureError{Cause: fmt.Errorf("timeout waiting for broker ack")}
	case <-ctx.Done():
		publishErr = &InfrastructureError{Cause: ctx.Err()}
	}

	// Step 5 — return or discard the channel.
	if publishErr != nil {
		p.logger.Warn("publish failed, discarding channel",
			zap.String("routing_key", routingKey),
			zap.Error(publishErr),
		)
		pc.ch.Close()
		p.replaceChannel()
		return publishErr
	}

	// Happy path — return the healthy channel to the pool for reuse.
	p.pool <- pc

	p.logger.Info("message published",
		zap.String("routing_key", routingKey),
		zap.Duration("duration", time.Since(start)),
	)
	return nil
}

// -------------------------------------------------------------------------
// Internal helpers — all assume the caller manages locking where needed.
// -------------------------------------------------------------------------

// connect dials RabbitMQ and registers the connection-loss watcher.
// Must be called with mu held or during single-threaded initialisation.
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

// watchClose spawns a goroutine that sets dirty=true when the connection is
// lost. The actual reconnection is lazy: it happens on the next Publish call.
func (p *Publisher) watchClose(conn *amqp.Connection) {
	connClose := conn.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		err, ok := <-connClose
		if !ok {
			// Channel closed cleanly (e.g. Publisher.Close was called).
			return
		}
		p.logger.Error("rabbitmq connection lost",
			zap.String("reason", err.Reason),
			zap.Int("code", err.Code),
			zap.Error(err),
		)
		p.mu.Lock()
		p.dirty = true
		p.mu.Unlock()
	}()
}

// ensureConnected reconnects if dirty and rebuilds the channel pool.
// Must be called with mu held.
func (p *Publisher) ensureConnected() error {
	if !p.dirty && p.conn != nil && !p.conn.IsClosed() {
		return nil
	}

	p.logger.Info("reconnecting to rabbitmq")
	p.drainPool()
	p.closeUnsafe()

	if err := p.connect(); err != nil {
		return err
	}

	if err := p.buildPool(); err != nil {
		p.closeUnsafe()
		return fmt.Errorf("rebuild channel pool after reconnect: %w", err)
	}

	return nil
}

// buildPool opens cfg.WorkerCount channels, enables confirms on each, and
// pushes them into p.pool. Must be called with mu held (or during init).
func (p *Publisher) buildPool() error {
	for i := 0; i < p.workerCount; i++ {
		pc, err := p.openPooledChannel()
		if err != nil {
			// Drain whatever was added before the failure so the pool is
			// never in a half-built state.
			p.drainPool()
			return fmt.Errorf("open pool slot %d: %w", i, err)
		}
		p.pool <- pc
	}

	p.logger.Info("channel pool ready", zap.Int("size", p.workerCount))
	return nil
}

// openPooledChannel opens a single AMQP channel and puts it in confirm mode.
// The caller is responsible for adding it to the pool or closing it on error.
// Must be called with mu held (conn must be non-nil and healthy).
func (p *Publisher) openPooledChannel() (*pooledChannel, error) {
	ch, err := p.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}

	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, fmt.Errorf("enable confirms: %w", err)
	}

	return &pooledChannel{
		ch:       ch,
		confirms: ch.NotifyPublish(make(chan amqp.Confirmation, 1)),
	}, nil
}

// drainPool closes and discards all channels currently sitting in the pool.
// Channels that are in-flight (held by a Publish goroutine) are NOT affected
// here — they will be discarded by the Publish caller after the error.
// Must be called with mu held.
func (p *Publisher) drainPool() {
	for {
		select {
		case pc := <-p.pool:
			pc.ch.Close()
		default:
			return
		}
	}
}

// replaceChannel tries to open a new channel and return it to the pool so
// the pool stays at WorkerCount after a discard. It is best-effort: if the
// connection is dirty or already gone, the slot is simply left empty until
// the next ensureConnected call rebuilds the full pool.
func (p *Publisher) replaceChannel() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.dirty || p.conn == nil || p.conn.IsClosed() {
		// The pool will be fully rebuilt on the next ensureConnected call.
		return
	}

	pc, err := p.openPooledChannel()
	if err != nil {
		p.logger.Warn("could not replace discarded channel; pool temporarily undersized",
			zap.Error(err),
		)
		return
	}

	p.pool <- pc
}

// closeUnsafe closes the raw connection without touching the pool.
// Must be called with mu held.
func (p *Publisher) closeUnsafe() {
	if p.conn != nil {
		p.conn.Close()
		p.conn = nil
	}
	p.dirty = false
}

// truncate returns at most max bytes of b, appending "..." if truncated.
func truncate(b []byte, max int) []byte {
	if len(b) <= max {
		return b
	}
	return append(b[:max:max], []byte("...")...)
}
