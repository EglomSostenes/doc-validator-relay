package outbox

import (
	"context"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/doc-validator/relay/internal/db"
)

// BatchStore is the interface the BufferedEventStore uses to flush accumulated
// updates. db.Pool satisfies this interface via UpdateBatch.
type BatchStore interface {
	UpdateBatch(ctx context.Context, params []db.UpdateParams) error
}

// BufferedEventStore implements the EventStore interface used by the Processor.
// Instead of writing each update immediately, it accumulates UpdateParams in a
// buffer and flushes them to the database in a single batch when either:
//   - the buffer reaches batchSize (size-triggered flush), or
//   - the flush ticker fires (time-triggered flush).
//
// This reduces the number of round-trips to Postgres under load from one per
// event to one per batch, while keeping latency bounded by flushInterval so
// events are never stuck in the buffer longer than necessary.
//
// On flush failure the batch is discarded and the events remain in
// "processing" status. ReclaimStaleProcessing will reset them to "pending"
// within ProcessingTimeout / 2, which is the existing recovery mechanism for
// this scenario.
//
// Concurrency model:
//   - mu protects buf. All writes (UpdateEvent) and size-triggered flushes
//     hold mu for the minimum time needed.
//   - The flush goroutine (Run) holds mu only during the buffer swap, not
//     during the actual database write, so writers are never blocked by I/O.
//   - Run must be started before the Processor begins processing events.
//     It blocks until ctx is cancelled, then performs a final flush to drain
//     any remaining updates before returning.
type BufferedEventStore struct {
	store         BatchStore
	batchSize     int
	flushInterval time.Duration
	logger        *zap.Logger

	mu  sync.Mutex
	buf []db.UpdateParams
}

// NewBufferedEventStore creates a BufferedEventStore.
// batchSize should equal cfg.BatchSize; flushInterval should equal cfg.FlushInterval.
func NewBufferedEventStore(
	store BatchStore,
	batchSize int,
	flushInterval time.Duration,
	logger *zap.Logger,
) *BufferedEventStore {
	return &BufferedEventStore{
		store:         store,
		batchSize:     batchSize,
		flushInterval: flushInterval,
		logger:        logger,
		buf:           make([]db.UpdateParams, 0, batchSize),
	}
}

// UpdateEvent appends params to the buffer. If the buffer reaches batchSize,
// a size-triggered flush is performed synchronously before returning.
// Implements the EventStore interface consumed by Processor.
func (b *BufferedEventStore) UpdateEvent(ctx context.Context, params db.UpdateParams) error {
	b.mu.Lock()
	b.buf = append(b.buf, params)
	full := len(b.buf) >= b.batchSize
	b.mu.Unlock()

	if full {
		b.logger.Debug("buffer full, flushing immediately",
			zap.Int("batch_size", b.batchSize),
		)
		b.flush(ctx)
	}

	// Always return nil — failures are logged inside flush and the recovery
	// path (ReclaimStaleProcessing) handles them transparently.
	return nil
}

// Run starts the time-triggered flush loop. It blocks until ctx is cancelled,
// then performs a final flush to drain any buffered updates accumulated between
// the last tick and shutdown. Designed to be launched as a goroutine from main.
func (b *BufferedEventStore) Run(ctx context.Context) {
	ticker := time.NewTicker(b.flushInterval)
	defer ticker.Stop()

	b.logger.Info("buffered event store started",
		zap.Int("batch_size", b.batchSize),
		zap.Duration("flush_interval", b.flushInterval),
	)

	for {
		select {
		case <-ctx.Done():
			// Final flush — drain whatever accumulated since the last tick.
			// Use context.Background() so the write is not aborted by the
			// already-cancelled ctx.
			b.logger.Info("buffered event store shutting down, flushing remaining updates")
			b.flush(context.Background())
			b.logger.Info("buffered event store stopped")
			return

		case <-ticker.C:
			b.flush(ctx)
		}
	}
}

// flush swaps the buffer under the lock (minimising lock contention) and
// writes the snapshot to the database outside the lock so writers are never
// blocked by I/O.
func (b *BufferedEventStore) flush(ctx context.Context) {
	// Swap buffer under lock — writers can append to the new buffer
	// immediately while we flush the snapshot to the database.
	b.mu.Lock()
	if len(b.buf) == 0 {
		b.mu.Unlock()
		return
	}
	snapshot := b.buf
	b.buf = make([]db.UpdateParams, 0, b.batchSize)
	b.mu.Unlock()

	b.logger.Debug("flushing update batch", zap.Int("count", len(snapshot)))

	flushCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := b.store.UpdateBatch(flushCtx, snapshot); err != nil {
		// Batch failed — events remain in "processing" and will be reclaimed
		// by ReclaimStaleProcessing within ProcessingTimeout / 2.
		b.logger.Error("CRITICAL: failed to flush update batch — events will be reclaimed by stale processing recovery",
			zap.Int("count", len(snapshot)),
			zap.Error(err),
		)
		return
	}

	b.logger.Debug("update batch flushed", zap.Int("count", len(snapshot)))
}
