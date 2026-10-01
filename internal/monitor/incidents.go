package monitor

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

type Incident struct {
	ID         uuid.UUID        `json:"id"`
	ChamberID  string           `json:"chamber_id"`
	Severity   string           `json:"severity"`
	Summary    string           `json:"summary"`
	Detail     string           `json:"detail,omitempty"`
	Device     model.DeviceKind `json:"device,omitempty"`
	EventType  model.EventType  `json:"event_type"`
	Sequence   uint64           `json:"sequence"`
	OccurredAt time.Time        `json:"occurred_at"`
}

// IncidentReview derives operator-review items from persisted safety events.
type IncidentReview struct {
	store journal.Store
}

func NewIncidentReview(store journal.Store) *IncidentReview { return &IncidentReview{store: store} }

func (r *IncidentReview) List(ctx context.Context) ([]Incident, error) {
	chambers, err := r.store.Chambers(ctx)
	if err != nil {
		return nil, err
	}
	var result []Incident
	for _, chamberID := range chambers {
		events, err := r.store.Events(ctx, chamberID, 0)
		if err != nil {
			return nil, err
		}
		for _, event := range events {
			severity, summary, include := incidentMeaning(event.Type)
			if !include {
				continue
			}
			incident := Incident{
				ID: event.ID, ChamberID: chamberID, Severity: severity,
				Summary: summary, EventType: event.Type, Sequence: event.Sequence,
				OccurredAt: event.OccurredAt,
			}
			if event.Type == model.EventCommandFailed {
				var command model.DeviceCommand
				if err := event.Decode(&command); err == nil {
					incident.Device = command.Device
					incident.Detail = command.FailureReason
					if command.FailureCode != "" {
						incident.Detail = command.FailureCode + ": " + incident.Detail
					}
					if command.Attempt > 1 {
						incident.Detail = fmt.Sprintf("attempt %d — %s", command.Attempt, incident.Detail)
					}
				}
			}
			result = append(result, incident)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i].OccurredAt.After(result[j].OccurredAt) })
	return result, nil
}

func incidentMeaning(kind model.EventType) (string, string, bool) {
	switch kind {
	case model.EventEmergencyStarted:
		return "critical", "Emergency stop requested", true
	case model.EventEmergencySettled:
		return "notice", "Emergency equipment reached stable state", true
	case model.EventReplayWarning:
		return "warning", "Recovery sequence requires review", true
	case model.EventCommandFailed:
		return "critical", "Device command failed or timed out", true
	case model.EventCommandRetried:
		return "notice", "Failed command safely resent", true
	case model.EventPlanCancelled:
		return "notice", "Convoy plan cancelled and compensated", true
	default:
		return "", "", false
	}
}
