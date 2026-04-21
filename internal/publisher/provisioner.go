package publisher

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"go.uber.org/zap"

	"github.com/doc-validator/relay/internal/config"
)

// ExchangeDef describes an AMQP exchange to be provisioned.
type ExchangeDef struct {
	Name       string
	Kind       string // "topic", "direct", "fanout", "headers"
	Durable    bool
	AutoDelete bool
	Args       amqp.Table
}

// QueueDef describes an AMQP queue to be provisioned.
type QueueDef struct {
	Name    string
	Durable bool
	Args    amqp.Table
}

// BindingDef describes an exchange→queue binding to be provisioned.
type BindingDef struct {
	Queue      string
	RoutingKey string
	Exchange   string
}

// Topology is the full set of AMQP resources the relay requires.
// It mirrors the JSON definition you provided, but expressed in Go so it is
// versioned with the service code and validated at compile time.
func DefaultTopology(cfg config.RabbitMQConfig) Topology {
	return Topology{
		Exchanges: []ExchangeDef{
			{
				Name:       cfg.Exchange, // "doc_validator.events"
				Kind:       "topic",
				Durable:    true,
				AutoDelete: false,
			},
		},
		Queues: []QueueDef{
			{
				Name:    "doc_validator.main_queue",
				Durable: true,
				Args: amqp.Table{
					"x-dead-letter-exchange":    cfg.Exchange,
					"x-dead-letter-routing-key": "doc_validator.retry",
				},
			},
			{
				Name:    "doc_validator.retry_queue",
				Durable: true,
				Args: amqp.Table{
					"x-dead-letter-exchange":    cfg.Exchange,
					"x-dead-letter-routing-key": "document.retry",
					"x-message-ttl":             int32(30000),
				},
			},
			{
				Name:    "doc_validator.dead_letter_queue",
				Durable: true,
				Args:    amqp.Table{},
			},
		},
		Bindings: []BindingDef{
			{Queue: "doc_validator.main_queue", RoutingKey: "document.#", Exchange: cfg.Exchange},
			{Queue: "doc_validator.retry_queue", RoutingKey: "doc_validator.retry", Exchange: cfg.Exchange},
			{Queue: "doc_validator.dead_letter_queue", RoutingKey: "document.dead_letter", Exchange: cfg.Exchange},
		},
	}
}

// Topology bundles all AMQP resource definitions together.
type Topology struct {
	Exchanges []ExchangeDef
	Queues    []QueueDef
	Bindings  []BindingDef
}

// Provision connects to RabbitMQ and idempotently creates all exchanges, queues,
// and bindings defined in the topology. Safe to call on every startup — AMQP
// declare operations are no-ops when the resource already exists with matching
// parameters.
func Provision(cfg config.RabbitMQConfig, topology Topology, logger *zap.Logger) error {
	url := fmt.Sprintf("amqp://%s:%s@%s:%d/%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.VHost,
	)

	conn, err := amqp.Dial(url)
	if err != nil {
		return fmt.Errorf("provision: dial rabbitmq: %w", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("provision: open channel: %w", err)
	}
	defer ch.Close()

	for _, ex := range topology.Exchanges {
		if err := ch.ExchangeDeclare(ex.Name, ex.Kind, ex.Durable, ex.AutoDelete, false, false, ex.Args); err != nil {
			return fmt.Errorf("provision: declare exchange %q: %w", ex.Name, err)
		}
		logger.Info("exchange ready", zap.String("exchange", ex.Name))
	}

	for _, q := range topology.Queues {
		if _, err := ch.QueueDeclare(q.Name, q.Durable, false, false, false, q.Args); err != nil {
			return fmt.Errorf("provision: declare queue %q: %w", q.Name, err)
		}
		logger.Info("queue ready", zap.String("queue", q.Name))
	}

	for _, b := range topology.Bindings {
		if err := ch.QueueBind(b.Queue, b.RoutingKey, b.Exchange, false, nil); err != nil {
			return fmt.Errorf("provision: bind queue %q → exchange %q (key %q): %w",
				b.Queue, b.Exchange, b.RoutingKey, err)
		}
		logger.Info("binding ready",
			zap.String("queue", b.Queue),
			zap.String("routing_key", b.RoutingKey),
		)
	}

	return nil
}
