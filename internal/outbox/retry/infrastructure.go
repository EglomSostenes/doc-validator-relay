package retry

import (
	"time"

	"github.com/doc-validator/relay/internal/db"
)

// Infrastructure retry intervals mirror Rails' OutboxEvent::INFRASTRUCTURE_RETRY_INTERVALS.
// [ 1.minute, 5.minutes, 15.minutes, 1.hour, 6.hours ]
var infrastructureRetryIntervals = []time.Duration{
	1 * time.Minute,
	5 * time.Minute,
	15 * time.Minute,
	1 * time.Hour,
	6 * time.Hour,
}

const maxInfrastructureRetries = 5 // mirrors Rails MAX_INFRASTRUCTURE_RETRIES — deve ser igual a len(infrastructureRetryIntervals)

// InfrastructureHandler handles failures caused by the messaging infrastructure
// (RabbitMQ unavailable, connection dropped, broker nack, etc.).
// Mirrors Rails' OutboxRelay::InfrastructureFailure.
//
// It uses the separate infrastructure_retry_count / infrastructure_retry_after
// columns so that business retries and infrastructure retries are tracked
// independently, exactly as the Rails implementation does.
type InfrastructureHandler struct {
	base
}

func newInfrastructureHandler() *InfrastructureHandler {
	return &InfrastructureHandler{
		base: base{
			maxRetries:     maxInfrastructureRetries,
			retryIntervals: infrastructureRetryIntervals,
		},
	}
}

// Handle decides whether to schedule an infrastructure retry or exhaust the event.
func (h *InfrastructureHandler) Handle(event db.OutboxEvent, err error) db.UpdateParams {
	nextCount := event.InfrastructureRetryCount + 1

	if nextCount >= h.maxRetries {
		return h.exhaust(event, nextCount, db.FailureReasonInfrastructure, true)
	}

	return h.schedule(event, err, nextCount, db.FailureReasonInfrastructure, true)
}
