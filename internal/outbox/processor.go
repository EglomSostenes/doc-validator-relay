package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/doc-validator/relay/internal/db"
	"github.com/doc-validator/relay/internal/outbox/retry"
)

// Publisher is the interface the Processor uses to send a message.
// Defined here so tests can inject a fake without importing the publisher package.
type Publisher interface {
	Publish(ctx context.Context, routingKey string, body []byte) error
}

// EventStore is the interface the Processor uses to persist results.
type EventStore interface {
	UpdateEvent(ctx context.Context, params db.UpdateParams) error
}

// Processor handles a single outbox event: enriches the payload, publishes it,
// and writes the result (success or failure) back to the database.
type Processor struct {
	publisher Publisher
	store     EventStore
	logger    *zap.Logger
}

// NewProcessor creates a Processor with its dependencies injected.
func NewProcessor(publisher Publisher, store EventStore, logger *zap.Logger) *Processor {
	return &Processor{
		publisher: publisher,
		store:     store,
		logger:    logger,
	}
}

// Process attempts to publish the event and updates its status accordingly.
// It never returns an error — all outcomes are recorded in the database.
// This keeps the Poller loop simple: launch and forget.
func (p *Processor) Process(ctx context.Context, event db.OutboxEvent) {
	payload, err := p.buildPayload(event)
	if err != nil {
		// Malformed payload is a business failure — it will never succeed.
		p.logger.Error("failed to build payload",
			zap.Int64("event_id", event.ID),
			zap.String("event_type", event.EventType),
			zap.Error(err),
		)
		p.applyFailure(ctx, event, fmt.Errorf("build payload: %w", err))
		return
	}

	publishErr := p.publisher.Publish(ctx, event.EventType, payload)
	if publishErr != nil {
		p.logger.Warn("failed to publish event",
			zap.Int64("event_id", event.ID),
			zap.String("event_type", event.EventType),
			zap.Error(publishErr),
		)
		p.applyFailure(ctx, event, publishErr)
		return
	}

	p.markPublished(ctx, event)
}

// buildPayload enriches the raw event payload with relay metadata, mirroring
// Rails' EventPublisher.full_payload.
func (p *Processor) buildPayload(event db.OutboxEvent) ([]byte, error) {
	// event.Payload is already valid JSON from the database.
	var raw map[string]any
	if err := json.Unmarshal(event.Payload, &raw); err != nil {
		return nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	raw["event_id"] = event.ID
	raw["published_at"] = time.Now().UTC().Format(time.RFC3339)

	out, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("marshal enriched payload: %w", err)
	}

	return out, nil
}

// applyFailure selects the appropriate retry handler and writes the result.
func (p *Processor) applyFailure(ctx context.Context, event db.OutboxEvent, err error) {
	handler := retry.For(err)
	params := handler.Handle(event, err)

	if updateErr := p.store.UpdateEvent(ctx, params); updateErr != nil {
		p.logger.Error("failed to persist failure result",
			zap.Int64("event_id", event.ID),
			zap.Error(updateErr),
		)
		return
	}

	if params.Status == db.StatusFailed {
		p.logger.Error("event exhausted all retries",
			zap.Int64("event_id", event.ID),
			zap.String("event_type", event.EventType),
			zap.String("failure_reason", string(*params.FailureReason)),
			zap.Error(err),
		)
	} else {
		p.logger.Warn("event scheduled for retry",
			zap.Int64("event_id", event.ID),
			zap.String("event_type", event.EventType),
			zap.String("failure_reason", string(*params.FailureReason)),
			zap.Error(err),
		)
	}
}

// markPublished updates the event to published status.
func (p *Processor) markPublished(ctx context.Context, event db.OutboxEvent) {
	params := db.UpdateParams{
		ID:     event.ID,
		Status: db.StatusPublished,
	}

	if err := p.store.UpdateEvent(ctx, params); err != nil {
		p.logger.Error("failed to mark event as published",
			zap.Int64("event_id", event.ID),
			zap.Error(err),
		)
		return
	}

	p.logger.Info("event published",
		zap.Int64("event_id", event.ID),
		zap.String("event_type", event.EventType),
	)
}
