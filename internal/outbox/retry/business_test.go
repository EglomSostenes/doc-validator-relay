package retry_test

import (
	"errors"
	"testing"
	"time"

	"github.com/doc-validator/relay/internal/db"
	"github.com/doc-validator/relay/internal/outbox/retry"
)

// businessError simulates a non-infrastructure failure (e.g. JSON parse error).
var businessError = errors.New("payload serialization failed")

func TestBusinessHandler_SchedulesRetry(t *testing.T) {
	handler := retry.For(businessError) // should return BusinessHandler

	event := db.OutboxEvent{
		ID:         1,
		EventType:  "document.created",
		RetryCount: 0,
	}

	params := handler.Handle(event, businessError)

	if params.Status != db.StatusPending {
		t.Errorf("expected status pending, got %d", params.Status)
	}
	if *params.RetryCount != 1 {
		t.Errorf("expected retry_count 1, got %d", *params.RetryCount)
	}
	if params.RetryAfter == nil {
		t.Fatal("expected retry_after to be set")
	}
	// First business retry interval is 1 minute.
	expectedAfter := time.Now().Add(1 * time.Minute)
	diff := params.RetryAfter.Sub(expectedAfter)
	if diff < -2*time.Second || diff > 2*time.Second {
		t.Errorf("retry_after off by %v, expected ~1 minute from now", diff)
	}
	if *params.FailureReason != db.FailureReasonBusiness {
		t.Errorf("expected failure_reason business, got %s", *params.FailureReason)
	}
}

func TestBusinessHandler_ProgressesRetryCount(t *testing.T) {
	handler := retry.For(businessError)

	// Simulate second failure (retry_count already 1).
	event := db.OutboxEvent{ID: 2, RetryCount: 1}
	params := handler.Handle(event, businessError)

	if params.Status != db.StatusPending {
		t.Errorf("expected pending, got %d", params.Status)
	}
	if *params.RetryCount != 2 {
		t.Errorf("expected retry_count 2, got %d", *params.RetryCount)
	}
	// Second interval is 3 minutes.
	expectedAfter := time.Now().Add(3 * time.Minute)
	diff := params.RetryAfter.Sub(expectedAfter)
	if diff < -2*time.Second || diff > 2*time.Second {
		t.Errorf("retry_after off by %v, expected ~3 minutes from now", diff)
	}
}

func TestBusinessHandler_ExhaustsAfterMaxRetries(t *testing.T) {
	handler := retry.For(businessError)

	// MAX_RETRIES is 5. Setting retry_count to 4 means the next call hits the limit.
	event := db.OutboxEvent{ID: 3, RetryCount: 4}
	params := handler.Handle(event, businessError)

	if params.Status != db.StatusFailed {
		t.Errorf("expected status failed, got %d", params.Status)
	}
	if *params.RetryCount != 5 {
		t.Errorf("expected final retry_count 5, got %d", *params.RetryCount)
	}
	if params.RetryAfter != nil {
		t.Error("expected retry_after to be nil on exhaustion")
	}
	if *params.FailureReason != db.FailureReasonBusiness {
		t.Errorf("expected failure_reason business, got %s", *params.FailureReason)
	}
}

func TestBusinessHandler_DoesNotTouchInfraCounters(t *testing.T) {
	handler := retry.For(businessError)
	event := db.OutboxEvent{ID: 4, RetryCount: 0}
	params := handler.Handle(event, businessError)

	if params.InfrastructureRetryCount != nil {
		t.Error("business handler must not touch infrastructure_retry_count")
	}
	if params.InfrastructureRetryAfter != nil {
		t.Error("business handler must not touch infrastructure_retry_after")
	}
}
