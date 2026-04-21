package retry

import (
	"errors"
	"time"

	"github.com/doc-validator/relay/internal/db"
	"github.com/doc-validator/relay/internal/publisher"
)

// Business retry intervals mirror Rails' OutboxEvent::RETRY_INTERVALS.
// [ 1.minute, 3.minutes, 5.minutes, 30.minutes, 1.hour ]
var businessRetryIntervals = []time.Duration{
	1 * time.Minute,
	3 * time.Minute,
	5 * time.Minute,
	30 * time.Minute,
	1 * time.Hour,
}

const maxBusinessRetries = 5 // mirrors Rails MAX_RETRIES — deve ser igual a len(businessRetryIntervals)

// BusinessHandler handles failures caused by the event payload or application
// logic (e.g. serialisation errors, unexpected publish failures that are not
// infrastructure-related). Mirrors Rails' OutboxRelay::BusinessFailure.
type BusinessHandler struct {
	base
}

func newBusinessHandler() *BusinessHandler {
	return &BusinessHandler{
		base: base{
			maxRetries:     maxBusinessRetries,
			retryIntervals: businessRetryIntervals,
		},
	}
}

// Handle decides whether to schedule a retry or exhaust the event.
// It uses event.RetryCount (the business retry counter), mirroring
// Rails' use of :retry_count / :retry_after.
func (h *BusinessHandler) Handle(event db.OutboxEvent, err error) db.UpdateParams {
	nextCount := event.RetryCount + 1

	if nextCount >= h.maxRetries {
		return h.exhaust(event, nextCount, db.FailureReasonBusiness, false)
	}

	return h.schedule(event, err, nextCount, db.FailureReasonBusiness, false)
}

// IsInfrastructureError reports whether the error came from the messaging
// infrastructure rather than from business logic.
// Uses errors.As so it correctly unwraps across package boundaries.
func IsInfrastructureError(err error) bool {
	var infraErr *publisher.InfrastructureError
	return errors.As(err, &infraErr)
}
