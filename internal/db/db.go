package db

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/doc-validator/relay/internal/config"
)

// Status mirrors the Rails enum: { pending: 0, published: 1, failed: 2, processing: 3 }
type Status int

const (
	StatusPending    Status = 0
	StatusPublished  Status = 1
	StatusFailed     Status = 2
	StatusProcessing Status = 3
)

// FailureReason mirrors the Rails enum string values.
type FailureReason string

const (
	FailureReasonBusiness       FailureReason = "business"
	FailureReasonInfrastructure FailureReason = "infrastructure"
)

// OutboxEvent mirrors the outbox_events table in the Rails database.
type OutboxEvent struct {
	ID                       int64
	EventType                string
	Payload                  []byte
	Status                   Status
	RetryCount               int
	RetryAfter               *time.Time
	InfrastructureRetryCount int
	InfrastructureRetryAfter *time.Time
	FailureReason            *FailureReason
	CreatedAt                time.Time
	UpdatedAt                time.Time
}

// UpdateParams carries only the fields the relay needs to write back.
type UpdateParams struct {
	ID                       int64
	Status                   Status
	FailureReason            *FailureReason
	RetryCount               *int
	RetryAfter               *time.Time
	InfrastructureRetryCount *int
	InfrastructureRetryAfter *time.Time
}

// Pool wraps pgxpool and exposes only the operations the relay needs.
// Keeping the surface small makes mocking in tests trivial.
type Pool struct {
	pool *pgxpool.Pool
}

// New creates a connection pool. It pings the database before returning
// so startup fails fast if the database is unreachable.
func New(ctx context.Context, cfg config.PostgresConfig) (*Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse postgres DSN: %w", err)
	}

	// Conservative pool settings — the relay is not a web server.
	poolCfg.MaxConns = 10
	poolCfg.MinConns = 2
	poolCfg.MaxConnLifetime = 30 * time.Minute
	poolCfg.MaxConnIdleTime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	return &Pool{pool: pool}, nil
}

// Close releases all pool connections.
func (p *Pool) Close() {
	p.pool.Close()
}

// ClaimBatch atomically selects up to `limit` publishable events and marks them
// as `processing` in a single statement. This is the distributed lock mechanism:
// no two workers (Go or Rails) can claim the same event.
//
// "Publishable" means:
//   - status = pending (0)
//   - retry_after is null or in the past
//   - infrastructure_retry_after is null or in the past
func (p *Pool) ClaimBatch(ctx context.Context, limit int) ([]OutboxEvent, error) {
	query := `
		UPDATE outbox_events
		SET    status = $1, updated_at = NOW()
		WHERE  id IN (
			SELECT id
			FROM   outbox_events
			WHERE  status = $2
			  AND  (retry_after IS NULL OR retry_after <= NOW())
			  AND  (infrastructure_retry_after IS NULL OR infrastructure_retry_after <= NOW())
			ORDER  BY created_at ASC
			LIMIT  $3
			FOR UPDATE SKIP LOCKED
		)
		RETURNING
			id, event_type, payload, status,
			retry_count, retry_after,
			infrastructure_retry_count, infrastructure_retry_after,
			failure_reason, created_at, updated_at
	`

	rows, err := p.pool.Query(ctx, query, StatusProcessing, StatusPending, limit)
	if err != nil {
		return nil, fmt.Errorf("claim batch: %w", err)
	}
	defer rows.Close()

	return scanEvents(rows)
}

// ReclaimStaleProcessing finds events stuck in `processing` for longer than
// `timeout` and resets them to `pending` so they can be retried. This guards
// against crashes that left events in an intermediate state.
func (p *Pool) ReclaimStaleProcessing(ctx context.Context, timeout time.Duration) (int64, error) {
	query := `
		UPDATE outbox_events
		SET    status = $1, updated_at = NOW()
		WHERE  status = $2
		  AND  updated_at < $3
	`

	tag, err := p.pool.Exec(ctx, query, StatusPending, StatusProcessing, time.Now().Add(-timeout))
	if err != nil {
		return 0, fmt.Errorf("reclaim stale processing: %w", err)
	}

	return tag.RowsAffected(), nil
}

// UpdateEvent writes the result of a single publish attempt back to the
// database. Kept for compatibility — internally delegates to UpdateBatch.
func (p *Pool) UpdateEvent(ctx context.Context, params UpdateParams) error {
	return p.UpdateBatch(ctx, []UpdateParams{params})
}

// UpdateBatch writes multiple publish results in a single round-trip using
// UNNEST. This is the primary write path under load — the BufferedEventStore
// accumulates UpdateParams and flushes them here.
//
// The COALESCE preserves the same partial-update semantics as the original
// single-row UpdateEvent: nil pointers leave the column unchanged.
//
// Nil pointer columns are expanded as typed NULL arrays so Postgres can
// resolve the overloaded UNNEST without ambiguity.
func (p *Pool) UpdateBatch(ctx context.Context, params []UpdateParams) error {
	if len(params) == 0 {
		return nil
	}

	// Single-row fast path — avoids UNNEST overhead for the common case
	// where the BufferedEventStore flushes a batch of one (e.g. low traffic).
	if len(params) == 1 {
		return p.updateSingle(ctx, params[0])
	}

	// Expand UpdateParams into typed column arrays for UNNEST.
	ids := make([]int64, len(params))
	statuses := make([]int, len(params))
	failureReasons := make([]*string, len(params))
	retryCounts := make([]*int, len(params))
	retryAfters := make([]*time.Time, len(params))
	infraRetryCounts := make([]*int, len(params))
	infraRetryAfters := make([]*time.Time, len(params))

	for i, p := range params {
		ids[i] = p.ID
		statuses[i] = int(p.Status)
		if p.FailureReason != nil {
			s := string(*p.FailureReason)
			failureReasons[i] = &s
		}
		retryCounts[i] = p.RetryCount
		retryAfters[i] = p.RetryAfter
		infraRetryCounts[i] = p.InfrastructureRetryCount
		infraRetryAfters[i] = p.InfrastructureRetryAfter
	}

	query := `
		UPDATE outbox_events AS o
		SET
			status                     = u.status,
			failure_reason             = u.failure_reason,
			retry_count                = COALESCE(u.retry_count, o.retry_count),
			retry_after                = u.retry_after,
			infrastructure_retry_count = COALESCE(u.infrastructure_retry_count, o.infrastructure_retry_count),
			infrastructure_retry_after = u.infrastructure_retry_after,
			updated_at                 = NOW()
		FROM (
			SELECT
				UNNEST($1::bigint[])      AS id,
				UNNEST($2::int[])         AS status,
				UNNEST($3::text[])        AS failure_reason,
				UNNEST($4::int[])         AS retry_count,
				UNNEST($5::timestamptz[]) AS retry_after,
				UNNEST($6::int[])         AS infrastructure_retry_count,
				UNNEST($7::timestamptz[]) AS infrastructure_retry_after
		) AS u
		WHERE o.id = u.id
	`

	_, err := p.pool.Exec(ctx, query,
		ids,
		statuses,
		failureReasons,
		retryCounts,
		retryAfters,
		infraRetryCounts,
		infraRetryAfters,
	)
	if err != nil {
		return fmt.Errorf("update batch (%d events): %w", len(params), err)
	}

	return nil
}

// updateSingle is the fast path for a batch of one. Identical semantics to
// the original UpdateEvent query — no UNNEST overhead.
func (p *Pool) updateSingle(ctx context.Context, params UpdateParams) error {
	query := `
		UPDATE outbox_events
		SET
			status                     = $2,
			failure_reason             = $3,
			retry_count                = COALESCE($4, retry_count),
			retry_after                = $5,
			infrastructure_retry_count = COALESCE($6, infrastructure_retry_count),
			infrastructure_retry_after = $7,
			updated_at                 = NOW()
		WHERE id = $1
	`

	_, err := p.pool.Exec(ctx, query,
		params.ID,
		params.Status,
		params.FailureReason,
		params.RetryCount,
		params.RetryAfter,
		params.InfrastructureRetryCount,
		params.InfrastructureRetryAfter,
	)
	if err != nil {
		return fmt.Errorf("update event %d: %w", params.ID, err)
	}

	return nil
}

// scanEvents reads pgx rows into OutboxEvent structs.
func scanEvents(rows pgx.Rows) ([]OutboxEvent, error) {
	var events []OutboxEvent

	for rows.Next() {
		var e OutboxEvent
		err := rows.Scan(
			&e.ID, &e.EventType, &e.Payload, &e.Status,
			&e.RetryCount, &e.RetryAfter,
			&e.InfrastructureRetryCount, &e.InfrastructureRetryAfter,
			&e.FailureReason, &e.CreatedAt, &e.UpdatedAt,
		)
		if err != nil {
			return nil, fmt.Errorf("scan outbox event: %w", err)
		}
		events = append(events, e)
	}

	return events, rows.Err()
}
