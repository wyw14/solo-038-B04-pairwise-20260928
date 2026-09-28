package register

import (
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/solo-038/internal/journal"
	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/sensor"
)

// intervalIdentity identifies one register segment independent of when its
// commit happened. A retry closes the same segment at a later instant, so
// ClosedAt is deliberately excluded.
type intervalIdentity struct {
	JobID          model.JobID
	JobGeneration  uint64
	RollGeneration uint64
	StartMeter     float64
	EndMeter       float64
	OpenedAt       time.Time
}

func identityOf(interval model.Interval) intervalIdentity {
	return intervalIdentity{
		JobID:          interval.JobID,
		JobGeneration:  interval.JobGeneration,
		RollGeneration: interval.RollGeneration,
		StartMeter:     interval.StartMeter,
		EndMeter:       interval.EndMeter,
		OpenedAt:       interval.OpenedAt.UTC(),
	}
}

type Coordinator struct {
	mu       sync.Mutex
	state    *State
	journal  *journal.Store
	released map[model.JobID]journal.IntervalRecord
	// failures holds the outstanding commit error for each job's segment.
	// It is cleared only when that same segment commits successfully, so a
	// successful retry lifts the block left by its earlier failed attempt
	// while a genuine failure can never be masked into a release.
	failures    map[model.JobID]error
	lastFailure model.JobID
}

func NewCoordinator(state *State, store *journal.Store) *Coordinator {
	return &Coordinator{state: state, journal: store, released: map[model.JobID]journal.IntervalRecord{}, failures: map[model.JobID]error{}}
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

func (coordinator *Coordinator) Release(job model.Job, operation model.OperationID, proofs []model.Proof, now time.Time) error {
	window := coordinator.state.Snapshot()
	if !window.Valid || window.Samples == 0 {
		return fmt.Errorf("register window is incomplete")
	}
	interval := model.Interval{JobID: job.ID, JobGeneration: job.Generation, RollGeneration: job.RollGeneration, StartMeter: window.StartMeter, EndMeter: window.LastMeter, OpenedAt: window.StartedAt, ClosedAt: now.UTC()}
	segment := identityOf(interval)

	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()

	// Retrying (or re-delivering) an already committed segment is a no-op:
	// the segment is released exactly once and produces exactly one durable
	// release regardless of how many attempts surround it.
	if prior, ok := coordinator.released[job.ID]; ok && prior.Released && identityOf(prior.Interval) == segment {
		delete(coordinator.failures, job.ID)
		return nil
	}

	// The persisted record carries the release outcome itself: a durable
	// append means the segment is released; a failed append leaves the
	// segment unreleased and remembers the error for that segment.
	record := journal.IntervalRecord{Interval: interval, Proofs: proofs, Released: true}
	if _, err := journal.PersistInterval(coordinator.journal, job, operation, record, now); err != nil {
		failed := record
		failed.Released = false
		coordinator.released[job.ID] = failed
		coordinator.failures[job.ID] = err
		coordinator.lastFailure = job.ID
		return err
	}
	coordinator.released[job.ID] = record
	delete(coordinator.failures, job.ID)
	return nil
}

func (coordinator *Coordinator) CanAdvance(job model.Job) bool {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	record, ok := coordinator.released[job.ID]
	return ok && record.Released && record.Interval.JobGeneration == job.Generation && coordinator.failures[job.ID] == nil
}

func (coordinator *Coordinator) LastError() error {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if err := coordinator.failures[coordinator.lastFailure]; err != nil {
		return err
	}
	for _, err := range coordinator.failures {
		if err != nil {
			return err
		}
	}
	return nil
}
