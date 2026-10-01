package cycle

import (
	"context"
	"fmt"

	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Recovery rebuilds coordinator state from partition-safe replay results.
type Recovery struct {
	replayer    *journal.Replayer
	coordinator *Coordinator
}

func NewRecovery(replayer *journal.Replayer, coordinator *Coordinator) *Recovery {
	return &Recovery{replayer: replayer, coordinator: coordinator}
}

func (r *Recovery) RestoreAll(ctx context.Context) (map[string]model.TransitCycle, error) {
	states, err := r.replayer.ReplayAll(ctx)
	if err != nil {
		return nil, err
	}
	restored := make(map[string]model.TransitCycle)
	for chamberID, state := range states {
		cycleValue := state.Snapshot.Cycle
		for _, event := range state.Applied {
			if event.ChamberID != chamberID {
				return nil, fmt.Errorf("event partition mismatch chamber=%s event=%s", chamberID, event.ChamberID)
			}
			switch event.Type {
			case model.EventCycleCreated, model.EventCycleAdvanced:
				var decoded model.TransitCycle
				if err := event.Decode(&decoded); err != nil {
					return nil, err
				}
				if decoded.ChamberID != chamberID {
					return nil, fmt.Errorf("cycle partition mismatch chamber=%s cycle=%s", chamberID, decoded.ChamberID)
				}
				cycleCopy := decoded
				cycleValue = &cycleCopy
			case model.EventCommandQueued, model.EventCommandAccepted, model.EventCommandExecuting,
				model.EventCommandSettled, model.EventCommandFailed, model.EventCommandTimedOut:
				var command model.DeviceCommand
				if err := event.Decode(&command); err != nil {
					return nil, err
				}
				if command.ChamberID != chamberID {
					return nil, fmt.Errorf("command partition mismatch chamber=%s command=%s", chamberID, command.ChamberID)
				}
				// Each command event carries the full post-transition snapshot;
				// Restore keeps the newest one per command ID.
				r.coordinator.gates.Commands().Restore(command)
			}
		}
		if cycleValue != nil {
			r.coordinator.Restore(*cycleValue)
			restored[chamberID] = cycleValue.Clone()
		}
	}
	return restored, nil
}
