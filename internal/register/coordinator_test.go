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

func newReleasedSetup(t *testing.T) (*Coordinator, *journal.Store, model.Job) {
	t.Helper()
	store := journal.NewStore(filepath.Join(t.TempDir(), "events.jsonl"))
	state := NewState()
	coordinator := NewCoordinator(state, store)

	now := time.Now().UTC()
	job := model.Job{ID: model.NewJobID(), State: model.JobPrinting, Generation: 1, RollGeneration: 1, CreatedAt: now, UpdatedAt: now}
	if err := state.Start(job.Generation, 1, 0, now); err != nil {
		t.Fatalf("start window: %v", err)
	}
	frame := sensor.CameraFrame{ID: "frame-1", Camera: "register-camera", JobGeneration: job.Generation, PlateGeneration: 1, WebMeter: 1, Offset: 0.04, CapturedAt: now, ReceivedAt: now.Add(time.Millisecond)}
	if err := state.Add(frame); err != nil {
		t.Fatalf("add frame: %v", err)
	}
	return coordinator, store, job
}

func TestReleaseFailureDoesNotFalselyAdvance(t *testing.T) {
	coordinator, _, job := newReleasedSetup(t)

	coordinator.journal.FailNext(errors.New("disk full"))
	if err := coordinator.Release(job, model.NewOperationID(), nil, time.Now()); err == nil {
		t.Fatal("expected first commit to fail")
	}
	if coordinator.CanAdvance(job) {
		t.Fatal("failed commit must not authorize advancement")
	}
	if coordinator.LastError() == nil {
		t.Fatal("failed commit must record an error")
	}
}

func TestRetrySuccessClearsCorrespondingErrorAndAdvances(t *testing.T) {
	coordinator, store, job := newReleasedSetup(t)

	coordinator.journal.FailNext(errors.New("disk full"))
	if err := coordinator.Release(job, model.NewOperationID(), nil, time.Now()); err == nil {
		t.Fatal("expected first commit to fail")
	}

	if err := coordinator.Release(job, model.NewOperationID(), nil, time.Now()); err != nil {
		t.Fatalf("retry commit should succeed: %v", err)
	}
	if !coordinator.CanAdvance(job) {
		t.Fatal("successful retry must release the web for advancement")
	}
	if coordinator.LastError() != nil {
		t.Fatalf("successful retry must clear the corresponding old error, got %v", coordinator.LastError())
	}

	events, err := store.Events()
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	committed := 0
	for _, event := range events {
		if event.Kind == "interval_committed" {
			committed++
		}
	}
	if committed != 1 {
		t.Fatalf("same segment must be durably released exactly once, got %d commits", committed)
	}
}

func TestRepeatReleaseOfSameSegmentIsIdempotent(t *testing.T) {
	coordinator, store, job := newReleasedSetup(t)

	for attempt := 0; attempt < 2; attempt++ {
		if err := coordinator.Release(job, model.NewOperationID(), nil, time.Now()); err != nil {
			t.Fatalf("release attempt %d: %v", attempt, err)
		}
		if !coordinator.CanAdvance(job) {
			t.Fatalf("release attempt %d must keep advancement authorized", attempt)
		}
	}

	events, err := store.Events()
	if err != nil {
		t.Fatalf("read events: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("repeated release of the same segment must commit once, got %d events", len(events))
	}
}
