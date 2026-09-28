package journal

import (
	"fmt"
	"sort"
	"time"

	"github.com/wyw14/solo-038/internal/model"
)

type RecoveredState struct {
	Job            model.Job
	Owner          string
	ReleasedRanges []model.Interval
	EventCount     int
}

// releasedSegment identifies a committed segment independently of when it
// closed, so a retried commit of the same segment is released only once.
type releasedSegment struct {
	JobID          model.JobID
	JobGeneration  uint64
	RollGeneration uint64
	StartMeter     float64
	EndMeter       float64
	OpenedAt       time.Time
}

func segmentOf(interval model.Interval) releasedSegment {
	return releasedSegment{JobID: interval.JobID, JobGeneration: interval.JobGeneration, RollGeneration: interval.RollGeneration, StartMeter: interval.StartMeter, EndMeter: interval.EndMeter, OpenedAt: interval.OpenedAt.UTC()}
}

func Replay(events []model.Event) (RecoveredState, error) {
	ordered := append([]model.Event(nil), events...)
	sort.SliceStable(ordered, func(left, right int) bool {
		return ordered[left].At.Before(ordered[right].At)
	})
	state := RecoveredState{}
	released := map[releasedSegment]bool{}
	for _, event := range ordered {
		if err := event.Validate(); err != nil {
			return RecoveredState{}, err
		}
		state.EventCount++
		switch event.Kind {
		case "job_created", "job_snapshot":
			job, err := model.DecodeEventPayload[model.Job](event)
			if err != nil {
				return RecoveredState{}, err
			}
			state.Job = job
		case "web_fence_restored":
			payload, err := model.DecodeEventPayload[map[string]any](event)
			if err != nil {
				return RecoveredState{}, err
			}
			state.Owner, _ = payload["owner"].(string)
		case "interval_committed":
			record, err := model.DecodeEventPayload[IntervalRecord](event)
			if err != nil {
				return RecoveredState{}, err
			}
			segment := segmentOf(record.Interval)
			if record.Released && !released[segment] {
				released[segment] = true
				state.ReleasedRanges = append(state.ReleasedRanges, record.Interval)
			}
		}
	}
	if state.EventCount > 0 && !state.Job.ID.Valid() {
		return RecoveredState{}, fmt.Errorf("journal has no recoverable job snapshot")
	}
	return state, nil
}
