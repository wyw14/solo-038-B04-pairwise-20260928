package register

import (
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/solo-038/internal/journal"
	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/sensor"
)

// intervalError tracks a failed commit for a specific interval so that a
// successful retry of that same interval can clear exactly that error without
// masking unrelated failures.
type intervalError struct {
	err error
	at  time.Time
}

type Coordinator struct {
	mu        sync.Mutex
	state     *State
	journal   *journal.Store
	released  map[model.JobID]journal.IntervalRecord
	failures  map[string]intervalError
	lastKey   string
	lastError error
}

func NewCoordinator(state *State, store *journal.Store) *Coordinator {
	return &Coordinator{
		state:    state,
		journal:  store,
		released: map[model.JobID]journal.IntervalRecord{},
		failures: map[string]intervalError{},
	}
}

// intervalKey identifies a single register interval independently of the
// operation that commits it and of the commit timestamp, so a failed attempt
// and its retry resolve to the same segment. ClosedAt is deliberately excluded
// because a retry closes the same window at a later instant.
func intervalKey(interval model.Interval) string {
	openedAt := interval.OpenedAt.UTC().UnixNano()
	return fmt.Sprintf("%s|%d|%d|%f|%d", interval.JobID, interval.JobGeneration, interval.RollGeneration, interval.StartMeter, openedAt)
}

func (coordinator *Coordinator) AcceptFrame(frame sensor.CameraFrame, maximumGap time.Duration) error {
	window := coordinator.state.Snapshot()
	if !window.Valid {
		return fmt.Errorf("register window is not active")
	}
	if frame.JobGeneration != window.JobGeneration || frame.PlateGeneration != window.PlateGeneration {
		return fmt.Errorf("camera frame does not belong to active register generation")
	}
	if frame.ReceivedAt.Sub(window.LastAt) > maximumGap {
		coordinator.state.Break()
		return fmt.Errorf("camera gap exceeded register dwell")
	}
	return coordinator.state.Add(frame)
}

func (coordinator *Coordinator) Stable(minimum int) bool {
	window := coordinator.state.Snapshot()
	return window.Valid && window.Samples >= minimum
}

// Release commits one register interval. Release state is derived strictly from
// the commit outcome: a failed persist marks the interval as failed and never
// authorizes advancement; a successful (or idempotent repeat) commit marks the
// interval released and clears that interval's prior failure. The same segment
// is durably released at most once.
func (coordinator *Coordinator) Release(job model.Job, operation model.OperationID, proofs []model.Proof, now time.Time) error {
	window := coordinator.state.Snapshot()
	if !window.Valid || window.Samples == 0 {
		return fmt.Errorf("register window is incomplete")
	}
	interval := model.Interval{JobID: job.ID, JobGeneration: job.Generation, RollGeneration: job.RollGeneration, StartMeter: window.StartMeter, EndMeter: window.LastMeter, OpenedAt: window.StartedAt, ClosedAt: now.UTC()}
	record := journal.IntervalRecord{Interval: interval, Proofs: proofs, Released: false}
	key := intervalKey(interval)

	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()

	// The same segment has already been durably released. A repeated submission
	// must not produce a second commit or a second effective release.
	if existing, ok := coordinator.released[job.ID]; ok && existing.Released && intervalKey(existing.Interval) == key {
		coordinator.clearFailure(key)
		return nil
	}

	if _, err := journal.PersistInterval(coordinator.journal, job, operation, record, now); err != nil {
		coordinator.failures[key] = intervalError{err: err, at: now}
		coordinator.lastKey = key
		coordinator.lastError = err
		return err
	}

	record.Released = true
	coordinator.released[job.ID] = record
	coordinator.clearFailure(key)
	return nil
}

func (coordinator *Coordinator) clearFailure(key string) {
	delete(coordinator.failures, key)
	if coordinator.lastKey != key {
		return
	}
	// The most recent failure was resolved; surface the next outstanding one.
	var nextKey string
	var nextAt time.Time
	var nextErr error
	for candidateKey, failure := range coordinator.failures {
		if nextKey == "" || failure.at.After(nextAt) {
			nextKey = candidateKey
			nextAt = failure.at
			nextErr = failure.err
		}
	}
	coordinator.lastKey = nextKey
	coordinator.lastError = nextErr
}

// CanAdvance reports whether the current generation of the job has a durably
// released interval with no outstanding commit failure for that segment. A
// failed commit for another segment never blocks an independently released one.
func (coordinator *Coordinator) CanAdvance(job model.Job) bool {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	record, ok := coordinator.released[job.ID]
	if !ok || !record.Released || record.Interval.JobGeneration != job.Generation {
		return false
	}
	_, failed := coordinator.failures[intervalKey(record.Interval)]
	return !failed
}

func (coordinator *Coordinator) LastError() error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	return coordinator.lastError
}
