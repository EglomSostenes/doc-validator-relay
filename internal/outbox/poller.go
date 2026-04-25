package outbox

import (
	"context"
	"time"

	"go.uber.org/zap"
	"golang.org/x/sync/semaphore"

	"github.com/doc-validator/relay/internal/config"
	"github.com/doc-validator/relay/internal/db"
)

// Claimer is the interface the Poller uses to fetch events from the database.
type Claimer interface {
	ClaimBatch(ctx context.Context, limit int) ([]db.OutboxEvent, error)
	ReclaimStaleProcessing(ctx context.Context, timeout time.Duration) (int64, error)
}

// Poller runs a continuous loop that claims batches of pending outbox events
// and dispatches each one to a Processor running in a bounded worker pool.
//
// Concurrency model:
//   - A semaphore limits the number of events processed simultaneously.
//   - Each event runs in its own goroutine; the semaphore prevents unbounded growth.
//   - The poll interval is adaptive: if a full batch was returned, the next poll
//     fires immediately (catch-up mode); otherwise it waits the configured interval.
//
// FIX: the original loop used a `default` branch in the outer select, which
// caused poll() to be called in a tight spin whenever reclaimTicker had no
// event ready. Under burst load this fired poll() multiple times before any
// worker had finished, resulting in events stuck in "processing":
//
//   Original (broken):
//     select {
//     case <-ctx.Done():   ...
//     case <-reclaimTicker.C: ...
//     default:             poll() ← spins freely, no back-pressure
//     }
//
//   Fixed: a dedicated pollTicker drives poll cadence. Reset(0) provides the
//   immediate re-poll when a full batch is found, without a spin loop.
type Poller struct {
	claimer   Claimer
	processor *Processor
	cfg       config.RelayConfig
	logger    *zap.Logger
}

// NewPoller constructs a Poller. All dependencies are explicit for testability.
func NewPoller(claimer Claimer, processor *Processor, cfg config.RelayConfig, logger *zap.Logger) *Poller {
	return &Poller{
		claimer:   claimer,
		processor: processor,
		cfg:       cfg,
		logger:    logger,
	}
}

// Run starts the polling loop. It blocks until ctx is cancelled.
// Designed to be launched as a goroutine from main.
func (p *Poller) Run(ctx context.Context) {
	cycle := 0
	p.logger.Info("poller started",
		zap.Int("batch_size", p.cfg.BatchSize),
		zap.Duration("poll_interval", p.cfg.PollInterval),
		zap.Int("worker_count", p.cfg.WorkerCount),
	)

	// A semaphore acting as a fixed-size worker pool.
	sem := semaphore.NewWeighted(int64(p.cfg.WorkerCount))

	// Stale processing reclaim runs on a separate, slower ticker.
	reclaimTicker := time.NewTicker(p.cfg.ProcessingTimeout / 2)
	defer reclaimTicker.Stop()

	// pollTicker drives the poll cadence.
	// Reset(0) is used for immediate re-poll when a full batch is found.
	pollTicker := time.NewTicker(p.cfg.PollInterval)
	defer pollTicker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.logger.Info("poller shutting down, draining in-flight workers")
			// Acquire all semaphore slots to wait for in-flight goroutines.
			// Use context.Background() so the drain is not interrupted by
			// the already-cancelled ctx.
			if err := sem.Acquire(context.Background(), int64(p.cfg.WorkerCount)); err != nil {
				p.logger.Error("failed to drain workers on shutdown", zap.Error(err))
			}
			p.logger.Info("poller stopped")
			return

		case <-reclaimTicker.C:
			p.reclaimStale(ctx)

		case <-pollTicker.C:
			full := p.poll(ctx, sem)
			cycle++
			if cycle%1000 == 0 {
				p.logger.Info("poller health",
					zap.Int("cycle", cycle),
				)
			}
			if full {
				// Full batch returned — there may be more events queued.
				// Reset to fire immediately on the next iteration so we
				// drain the backlog without spinning in a tight loop.
				pollTicker.Reset(0)
			} else {
				// No backlog — restore the normal cadence.
				pollTicker.Reset(p.cfg.PollInterval)
			}
		}
	}
}

// poll claims one batch and dispatches each event to a worker goroutine.
// Returns true if the batch was full (signals a potential backlog).
func (p *Poller) poll(ctx context.Context, sem *semaphore.Weighted) (full bool) {
	events, err := p.claimer.ClaimBatch(ctx, p.cfg.BatchSize)
	if err != nil {
		p.logger.Error("failed to claim batch", zap.Error(err))
		// Back off on database errors to avoid hammering a sick database.
		select {
		case <-ctx.Done():
		case <-time.After(p.cfg.PollInterval):
		}
		return false
	}

	if len(events) == 0 {
		return false
	}

	p.logger.Debug("claimed batch", zap.Int("count", len(events)))

	for _, event := range events {
		event := event // capture for goroutine

		// Acquire a worker slot. This blocks if WorkerCount goroutines are
		// already running, providing natural backpressure.
		if err := sem.Acquire(ctx, 1); err != nil {
			// ctx was cancelled while waiting for a slot.
			return false
		}

		go func() {
			defer sem.Release(1)
			p.processor.Process(ctx, event)
		}()
	}

	return len(events) == p.cfg.BatchSize
}

// reclaimStale finds events stuck in `processing` and resets them to `pending`.
func (p *Poller) reclaimStale(ctx context.Context) {
	n, err := p.claimer.ReclaimStaleProcessing(ctx, p.cfg.ProcessingTimeout)
	if err != nil {
		p.logger.Error("failed to reclaim stale processing events", zap.Error(err))
		return
	}
	if n > 0 {
		p.logger.Warn("reclaimed stale processing events", zap.Int64("count", n))
	}
}
