package register

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/journal"
	"github.com/wyw14/solo-038/internal/model"
	"github.com/wyw14/solo-038/internal/sensor"
)

func newFailingReleaseHarness(t *testing.T) (*Coordinator, *journal.Store, model.Job) {
	t.Helper()
	store := journal.NewStore(filepath.Join(t.TempDir(), "events.jsonl"))
	state := NewState()
	coordinator := NewCoordinator(state, store)
	now := time.Now().UTC()
	job := model.Job{ID: model.NewJobID(), State: model.JobRegistering, Generation: 1, RollGeneration: 1, CreatedAt: now, UpdatedAt: now}
	if err := state.Start(job.Generation, 1, 0, now); err != nil {
		t.Fatalf("start window: %v", err)
	}
	frame := sensor.CameraFrame{ID: "frame-1", Camera: "register-camera", JobGeneration: job.Generation, PlateGeneration: 1, WebMeter: 1, CapturedAt: now, ReceivedAt: now.Add(time.Millisecond)}
	if err := state.Add(frame); err != nil {
		t.Fatalf("add frame: %v", err)
	}
	return coordinator, store, job
}

func TestFailedCommitDoesNotRelease(t *testing.T) {
	coordinator, _, job := newFailingReleaseHarness(t)
	storeFault := errors.New("disk unavailable")
	coordinator.journal.FailNext(storeFault)

	err := coordinator.Release(job, model.NewOperationID(), nil, time.Now().UTC())
	if !errors.Is(err, storeFault) {
		t.Fatalf("expected commit error %v, got %v", storeFault, err)
	}
	if coordinator.CanAdvance(job) {
		t.Fatal("paper path advanced despite failed commit")
	}
	if coordinator.LastError() == nil {
		t.Fatal("expected the commit failure to remain visible")
	}
}

func TestSuccessfulRetryClearsOldErrorAndReleases(t *testing.T) {
	coordinator, store, job := newFailingReleaseHarness(t)
	coordinator.journal.FailNext(errors.New("disk unavailable"))
	if err := coordinator.Release(job, model.NewOperationID(), nil, time.Now().UTC()); err == nil {
		t.Fatal("expected first commit to fail")
	}

	// Second attempt for the same segment commits durably.
	if err := coordinator.Release(job, model.NewOperationID(), nil, time.Now().UTC().Add(time.Second)); err != nil {
		t.Fatalf("retry commit: %v", err)
	}
	if coordinator.LastError() != nil {
		t.Fatalf("successful retry must clear the old error, got %v", coordinator.LastError())
	}
	if !coordinator.CanAdvance(job) {
		t.Fatal("successful retry must allow the paper path to advance")
	}

	events, err := store.Events()
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("expected exactly one durable commit for the segment, got %d", len(events))
	}
	record, err := model.DecodeEventPayload[journal.IntervalRecord](events[0])
	if err != nil {
		t.Fatalf("decode record: %v", err)
	}
	if !record.Released {
		t.Fatal("durable commit must be recorded as released")
	}
}

func TestRepeatedReleaseOfSameSegmentIsIdempotent(t *testing.T) {
	coordinator, store, job := newFailingReleaseHarness(t)

	first := time.Now().UTC()
	if err := coordinator.Release(job, model.NewOperationID(), nil, first); err != nil {
		t.Fatalf("first commit: %v", err)
	}
	// Redelivered/repeated call for the same window segment, even with a
	// later close time, must not create a second release.
	if err := coordinator.Release(job, model.NewOperationID(), nil, first.Add(time.Second)); err != nil {
		t.Fatalf("idempotent re-release: %v", err)
	}

	events, err := store.Events()
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("same segment must be released exactly once, got %d commits", len(events))
	}
	if !coordinator.CanAdvance(job) {
		t.Fatal("committed segment should remain releasable")
	}
}

func TestFailureOfLaterSegmentDoesNotClearEarlierError(t *testing.T) {
	coordinator, _, job := newFailingReleaseHarness(t)
	coordinator.journal.FailNext(errors.New("disk unavailable"))
	if err := coordinator.Release(job, model.NewOperationID(), nil, time.Now().UTC()); err == nil {
		t.Fatal("expected commit to fail")
	}
	coordinator.journal.FailNext(errors.New("still unavailable"))
	if err := coordinator.Release(job, model.NewOperationID(), nil, time.Now().UTC().Add(time.Second)); err == nil {
		t.Fatal("expected retry to fail as well")
	}
	if coordinator.CanAdvance(job) || coordinator.LastError() == nil {
		t.Fatal("repeated failures must keep the paper path blocked")
	}
}
