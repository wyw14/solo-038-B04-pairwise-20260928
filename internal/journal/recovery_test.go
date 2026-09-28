package journal

import (
	"testing"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

func TestReplayReleasesRetriedSegmentOnce(t *testing.T) {
	now := time.Now().UTC()
	job := model.Job{ID: model.NewJobID(), State: model.JobRegistering, Generation: 1, RollGeneration: 1, CreatedAt: now, UpdatedAt: now}
	interval := model.Interval{JobID: job.ID, JobGeneration: job.Generation, RollGeneration: job.RollGeneration, StartMeter: 0, EndMeter: 1, OpenedAt: now, ClosedAt: now.Add(time.Second)}

	events := []model.Event{}
	jobEvent, err := model.NewEvent(model.NewOperationID(), job, "job_snapshot", job, now)
	if err != nil {
		t.Fatalf("new job event: %v", err)
	}
	events = append(events, jobEvent)
	mustEvent := func(record IntervalRecord) model.Event {
		event, err := model.NewEvent(model.NewOperationID(), job, "interval_committed", record, record.Interval.ClosedAt)
		if err != nil {
			t.Fatalf("new event: %v", err)
		}
		return event
	}
	// Failed first attempt: durable record carries Released=false.
	events = append(events, mustEvent(IntervalRecord{Interval: interval, Released: false}))
	// Successful retry closes the same segment slightly later.
	retry := interval
	retry.ClosedAt = interval.ClosedAt.Add(time.Second)
	events = append(events, mustEvent(IntervalRecord{Interval: retry, Released: true}))
	// Duplicate redelivery of the released segment must not double count.
	events = append(events, mustEvent(IntervalRecord{Interval: retry, Released: true}))

	state, err := Replay(events)
	if err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(state.ReleasedRanges) != 1 {
		t.Fatalf("same segment must produce one effective release, got %d: %+v", len(state.ReleasedRanges), state.ReleasedRanges)
	}
}
