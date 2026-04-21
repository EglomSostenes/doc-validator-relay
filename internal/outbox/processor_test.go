package outbox_test

import (
	"context"
	"errors"
	"testing"

	"go.uber.org/zap/zaptest"

	"github.com/doc-validator/relay/internal/db"
	"github.com/doc-validator/relay/internal/outbox"
	"github.com/doc-validator/relay/internal/publisher"
)

// ── Fakes ────────────────────────────────────────────────────────────────────

type fakePublisher struct {
	err error
}

func (f *fakePublisher) Publish(_ context.Context, _ string, _ []byte) error {
	return f.err
}

type fakeStore struct {
	lastParams db.UpdateParams
	err        error
}

func (f *fakeStore) UpdateEvent(_ context.Context, params db.UpdateParams) error {
	f.lastParams = params
	return f.err
}

// ── Tests ────────────────────────────────────────────────────────────────────

func TestProcessor_SuccessfulPublish(t *testing.T) {
	pub := &fakePublisher{err: nil}
	store := &fakeStore{}
	logger := zaptest.NewLogger(t)

	processor := outbox.NewProcessor(pub, store, logger)
	event := db.OutboxEvent{
		ID:        1,
		EventType: "document.approved",
		Payload:   []byte(`{"event":"document.approved","data":{"document_id":42}}`),
	}

	processor.Process(context.Background(), event)

	if store.lastParams.Status != db.StatusPublished {
		t.Errorf("expected status published, got %d", store.lastParams.Status)
	}
	if store.lastParams.ID != 1 {
		t.Errorf("expected event id 1, got %d", store.lastParams.ID)
	}
}

func TestProcessor_InfrastructureFailureAppliesInfraRetry(t *testing.T) {
	pub := &fakePublisher{err: &publisher.InfrastructureError{}}
	store := &fakeStore{}
	logger := zaptest.NewLogger(t)

	processor := outbox.NewProcessor(pub, store, logger)
	event := db.OutboxEvent{
		ID:                       2,
		EventType:                "document.rejected",
		Payload:                  []byte(`{"event":"document.rejected"}`),
		InfrastructureRetryCount: 0,
	}

	processor.Process(context.Background(), event)

	if store.lastParams.Status != db.StatusPending {
		t.Errorf("expected pending after infra failure, got %d", store.lastParams.Status)
	}
	if store.lastParams.InfrastructureRetryCount == nil {
		t.Fatal("expected infrastructure_retry_count to be set")
	}
	if *store.lastParams.FailureReason != db.FailureReasonInfrastructure {
		t.Errorf("expected infrastructure failure reason, got %s", *store.lastParams.FailureReason)
	}
}

func TestProcessor_BusinessFailureAppliesBusinessRetry(t *testing.T) {
	pub := &fakePublisher{err: errors.New("unexpected error")}
	store := &fakeStore{}
	logger := zaptest.NewLogger(t)

	processor := outbox.NewProcessor(pub, store, logger)
	event := db.OutboxEvent{
		ID:        3,
		EventType: "document.created",
		Payload:   []byte(`{"event":"document.created"}`),
		RetryCount: 0,
	}

	processor.Process(context.Background(), event)

	if store.lastParams.Status != db.StatusPending {
		t.Errorf("expected pending after business failure, got %d", store.lastParams.Status)
	}
	if *store.lastParams.FailureReason != db.FailureReasonBusiness {
		t.Errorf("expected business failure reason, got %s", *store.lastParams.FailureReason)
	}
}

func TestProcessor_InvalidPayloadExhaustsAsBusiness(t *testing.T) {
	pub := &fakePublisher{}
	store := &fakeStore{}
	logger := zaptest.NewLogger(t)

	processor := outbox.NewProcessor(pub, store, logger)
	event := db.OutboxEvent{
		ID:        4,
		EventType: "document.created",
		Payload:   []byte(`not valid json`),
		RetryCount: 4, // already at max
	}

	processor.Process(context.Background(), event)

	// Payload error is business; at max retries it should be failed.
	if store.lastParams.Status != db.StatusFailed {
		t.Errorf("expected failed for invalid payload at max retries, got %d", store.lastParams.Status)
	}
}

func TestProcessor_PayloadEnrichedWithEventIDAndPublishedAt(t *testing.T) {
	var capturedBody []byte

	capturingPub := &capturingPublisher{}
	store := &fakeStore{}
	logger := zaptest.NewLogger(t)

	processor := outbox.NewProcessor(capturingPub, store, logger)
	event := db.OutboxEvent{
		ID:        99,
		EventType: "document.approved",
		Payload:   []byte(`{"event":"document.approved"}`),
	}

	processor.Process(context.Background(), event)

	capturedBody = capturingPub.lastBody
	if capturedBody == nil {
		t.Fatal("publisher was not called")
	}

	// The enriched payload must contain event_id and published_at.
	body := string(capturedBody)
	if !contains(body, `"event_id":99`) {
		t.Errorf("payload missing event_id: %s", body)
	}
	if !contains(body, `"published_at"`) {
		t.Errorf("payload missing published_at: %s", body)
	}
}

// ── Helpers ──────────────────────────────────────────────────────────────────

type capturingPublisher struct {
	lastBody []byte
}

func (c *capturingPublisher) Publish(_ context.Context, _ string, body []byte) error {
	c.lastBody = body
	return nil
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsStr(s, substr))
}

func containsStr(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
