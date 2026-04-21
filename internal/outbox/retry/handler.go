package retry

import (
	"time"

	"github.com/doc-validator/relay/internal/db"
)

// Handler decides what to do with an event after a failed publish attempt.
// Implementations mirror the Rails OutboxRelay::FailureHandler hierarchy.
type Handler interface {
	Handle(event db.OutboxEvent, err error) db.UpdateParams
}

// For returns the correct Handler for a given error, mirroring
// Rails' OutboxRelay::FailureHandler.for(error).
func For(err error) Handler {
	if IsInfrastructureError(err) {
		return newInfrastructureHandler()
	}
	return newBusinessHandler()
}

// base contains the scheduling logic shared between Business and Infrastructure
// handlers. It is not exported — consumers use the concrete types.
type base struct {
	maxRetries     int
	retryIntervals []time.Duration
}

// schedule builds an UpdateParams that keeps the event pending with a future
// retry_after timestamp. It mirrors Rails' FailureHandler#schedule!
func (b *base) schedule(
	event db.OutboxEvent,
	err error,
	nextRetryCount int,
	reason db.FailureReason,
	isInfra bool,
) db.UpdateParams {
	interval := b.retryIntervals[nextRetryCount-1]
	retryAt := time.Now().Add(interval)

	params := db.UpdateParams{
		ID:            event.ID,
		Status:        db.StatusPending,
		FailureReason: &reason,
	}

	if isInfra {
		params.InfrastructureRetryCount = &nextRetryCount
		params.InfrastructureRetryAfter = &retryAt
	} else {
		params.RetryCount = &nextRetryCount
		params.RetryAfter = &retryAt
	}

	return params
}

// exhaust builds an UpdateParams that permanently marks the event as failed.
// It mirrors Rails' FailureHandler#exhaust!
func (b *base) exhaust(
	event db.OutboxEvent,
	nextRetryCount int,
	reason db.FailureReason,
	isInfra bool,
) db.UpdateParams {
	params := db.UpdateParams{
		ID:            event.ID,
		Status:        db.StatusFailed,
		FailureReason: &reason,
	}

	if isInfra {
		params.InfrastructureRetryCount = &nextRetryCount
	} else {
		params.RetryCount = &nextRetryCount
	}

	return params
}
