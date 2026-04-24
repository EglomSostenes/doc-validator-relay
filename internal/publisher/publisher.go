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

// InfrastructureError signals that the failure is in the messaging layer.
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

// Publish agora abre um canal exclusivo por chamada para evitar concorrência no AMQP.
func (p *Publisher) Publish(ctx context.Context, routingKey string, body []byte) error {
	// 1. Garantir conexão saudável
	p.mu.Lock()
	if err := p.ensureConnected(); err != nil {
		p.mu.Unlock()
		return &InfrastructureError{Cause: err}
	}
	conn := p.conn
	p.mu.Unlock()

	// 2. Abrir canal exclusivo para este worker
	ch, err := conn.Channel()
	if err != nil {
		return &InfrastructureError{Cause: fmt.Errorf("open worker channel: %w", err)}
	}
	defer ch.Close()

	// 3. Ativar confirmações (Publisher Confirms)
	if err := ch.Confirm(false); err != nil {
		return &InfrastructureError{Cause: fmt.Errorf("enable confirms: %w", err)}
	}
	confirms := ch.NotifyPublish(make(chan amqp.Confirmation, 1))

	// 4. Publicar com persistência
	err = ch.PublishWithContext(
		ctx,
		p.cfg.Exchange,
		routingKey,
		true,  // mandatory: garante que a msg chegue em alguma fila
		false, // immediate
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			Timestamp:    time.Now(),
			Body:         body,
		},
	)
	if err != nil {
		return &InfrastructureError{Cause: fmt.Errorf("publish: %w", err)}
	}

	// 5. Aguardar ACK do Broker
	select {
	case conf, ok := <-confirms:
		if !ok {
			return &InfrastructureError{Cause: fmt.Errorf("confirms channel closed")}
		}
		if !conf.Ack {
			return &InfrastructureError{Cause: fmt.Errorf("broker nacked delivery")}
		}
		return nil
	case <-time.After(5 * time.Second):
		return &InfrastructureError{Cause: fmt.Errorf("timeout waiting for ack")}
	case <-ctx.Done():
		return &InfrastructureError{Cause: ctx.Err()}
	}
}

func (p *Publisher) connect() error {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		p.cfg.User, p.cfg.Password, p.cfg.Host, p.cfg.Port, p.cfg.VHost,
	)

	conn, err := amqp.DialConfig(url, amqp.Config{
		Heartbeat: 10 * time.Second,
		Dial:      amqp.DefaultDial(10 * time.Second),
	})
	if err != nil {
		return err
	}

	p.conn = conn
	p.dirty = false
	p.watchClose(conn)
	return nil
}

func (p *Publisher) watchClose(conn *amqp.Connection) {
	connClose := conn.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		err, ok := <-connClose
		if !ok {
			return
		}
		p.logger.Warn("rabbitmq connection closed unexpectedly", zap.Error(err))
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
