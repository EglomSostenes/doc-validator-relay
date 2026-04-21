package retry_test

import (
	"testing"
	"time"

	"github.com/doc-validator/relay/internal/db"
	"github.com/doc-validator/relay/internal/outbox/retry"
	"github.com/doc-validator/relay/internal/publisher"
)

func TestInfrastructureHandler_SelectedForInfrastructureError(t *testing.T) {
	infraErr := &publisher.InfrastructureError{}
	handler := retry.For(infraErr)

	// Verify the correct handler type was selected by checking it uses
	// infrastructure counters, not business counters.
	event := db.OutboxEvent{ID: 1, InfrastructureRetryCount: 0}
	params := handler.Handle(event, infraErr)

	if params.InfrastructureRetryCount == nil {
		t.Fatal("expected infrastructure_retry_count to be set")
	}
	if params.RetryCount != nil {
		t.Error("infrastructure handler must not touch retry_count")
	}
}

func TestInfrastructureHandler_SchedulesRetry(t *testing.T) {
	infraErr := &publisher.InfrastructureError{}
	handler := retry.For(infraErr)

	event := db.OutboxEvent{ID: 2, InfrastructureRetryCount: 0}
	params := handler.Handle(event, infraErr)

	if params.Status != db.StatusPending {
		t.Errorf("expected pending, got %d", params.Status)
	}
	if *params.InfrastructureRetryCount != 1 {
		t.Errorf("expected infra_retry_count 1, got %d", *params.InfrastructureRetryCount)
	}
	if params.InfrastructureRetryAfter == nil {
		t.Fatal("expected infrastructure_retry_after to be set")
	}
	// First infrastructure retry interval is 1 minute.
	expectedAfter := time.Now().Add(1 * time.Minute)
	diff := params.InfrastructureRetryAfter.Sub(expectedAfter)
	if diff < -2*time.Second || diff > 2*time.Second {
		t.Errorf("infrastructure_retry_after off by %v, expected ~1 minute", diff)
	}
	if *params.FailureReason != db.FailureReasonInfrastructure {
		t.Errorf("expected failure_reason infrastructure, got %s", *params.FailureReason)
	}
}

func TestInfrastructureHandler_FourthIntervalIs1Hour(t *testing.T) {
	infraErr := &publisher.InfrastructureError{}
	handler := retry.For(infraErr)

	// retryCount: 3 → nextCount = 4 → índice 3 → 1h (último intervalo antes do exhaust)
	event := db.OutboxEvent{ID: 3, InfrastructureRetryCount: 3}
	params := handler.Handle(event, infraErr)

	if params.Status != db.StatusPending {
		t.Errorf("expected pending at retry 4, got %d", params.Status)
	}
	if params.InfrastructureRetryAfter == nil {
		t.Fatal("expected infrastructure_retry_after to be set")
	}
	expectedAfter := time.Now().Add(1 * time.Hour)
	diff := params.InfrastructureRetryAfter.Sub(expectedAfter)
	if diff < -2*time.Second || diff > 2*time.Second {
		t.Errorf("4th interval should be 1h, off by %v", diff)
	}
}

func TestInfrastructureHandler_ExhaustsAfterMaxRetries(t *testing.T) {
	infraErr := &publisher.InfrastructureError{}
	handler := retry.For(infraErr)

	event := db.OutboxEvent{ID: 4, InfrastructureRetryCount: 4}
	params := handler.Handle(event, infraErr)

	if params.Status != db.StatusFailed {
		t.Errorf("expected failed, got %d", params.Status)
	}
	if *params.InfrastructureRetryCount != 5 {
		t.Errorf("expected final count 5, got %d", *params.InfrastructureRetryCount)
	}
	if params.InfrastructureRetryAfter != nil {
		t.Error("expected infrastructure_retry_after to be nil on exhaustion")
	}
}
