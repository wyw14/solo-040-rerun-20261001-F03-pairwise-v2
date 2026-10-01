package monitor

import (
	"time"

	"github.com/wyw14/cry-96/internal/admission"
	"github.com/wyw14/cry-96/internal/cycle"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/model"
	"github.com/wyw14/cry-96/internal/safety"
	"github.com/wyw14/cry-96/internal/signal"
)

type ChamberView struct {
	ChamberID string                                 `json:"chamber_id"`
	Cycle     *model.TransitCycle                    `json:"cycle,omitempty"`
	Gates     map[model.Direction]model.GatePosition `json:"gates"`
	Queue     []admission.QueueEntry                 `json:"queue"`
	Signals   []model.SignalState                    `json:"signals"`
	// Pending commands are still being received or executed by the device.
	Pending []model.DeviceCommand `json:"pending_commands"`
	// Blocking commands failed or timed out and hold the transit waiting.
	Blocking  []model.DeviceCommand `json:"blocking_commands"`
	Emergency model.EmergencyRecord `json:"emergency"`
	UpdatedAt time.Time             `json:"updated_at"`
}

// Snapshot aggregates current responsibilities without taking ownership.
type Snapshot struct {
	cycles   *cycle.Coordinator
	gates    *gate.State
	commands *gate.Commands
	queue    *admission.Service
	signals  *signal.Service
	safety   *safety.Service
	now      func() time.Time
}

func NewSnapshot(cycles *cycle.Coordinator, gates *gate.State, commands *gate.Commands, queue *admission.Service, signals *signal.Service, safetyService *safety.Service, now func() time.Time) *Snapshot {
	if now == nil {
		now = time.Now
	}
	return &Snapshot{
		cycles: cycles, gates: gates, commands: commands, queue: queue,
		signals: signals, safety: safetyService, now: now,
	}
}

func (s *Snapshot) Chamber(chamberID string) ChamberView {
	view := ChamberView{
		ChamberID: chamberID, Gates: s.gates.Snapshot(chamberID),
		Queue: s.queue.List(chamberID), Signals: s.signals.States(),
		Pending: s.commands.Pending(chamberID), Blocking: s.commands.Blocking(chamberID),
		Emergency: s.safety.Status(chamberID), UpdatedAt: s.now().UTC(),
	}
	if cycleValue, ok := s.cycles.Current(chamberID); ok {
		view.Cycle = &cycleValue
	}
	return view
}

func (s *Snapshot) Chambers(ids []string) []ChamberView {
	result := make([]ChamberView, 0, len(ids))
	for _, chamberID := range ids {
		result = append(result, s.Chamber(chamberID))
	}
	return result
}
