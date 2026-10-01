package interlock

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/model"
)

// Coordinator keeps policy evaluation and command reservation in one flow.
type Coordinator struct {
	policy    Policy
	arbiter   *Arbiter
	gates     *gate.Controller
	level     func(string) float64
	emergency func(string) bool
}

func NewCoordinator(policy Policy, arbiter *Arbiter, gates *gate.Controller, level func(string) float64, emergency func(string) bool) *Coordinator {
	return &Coordinator{policy: policy, arbiter: arbiter, gates: gates, level: level, emergency: emergency}
}

func (c *Coordinator) Snapshot(chamberID string) EquipmentSnapshot {
	level := 0.0
	if c.level != nil {
		level = c.level(chamberID)
	}
	emergency := false
	if c.emergency != nil {
		emergency = c.emergency(chamberID)
	}
	return EquipmentSnapshot{
		ChamberID: chamberID, Gates: c.gates.State().Snapshot(chamberID),
		LevelM: level, Emergency: emergency,
	}
}

func (c *Coordinator) RequestOpen(ctx context.Context, cycle model.TransitCycle, direction model.Direction, owner string) (model.DeviceCommand, error) {
	if err := c.policy.ValidateOpen(c.Snapshot(cycle.ChamberID), direction); err != nil {
		return model.DeviceCommand{}, err
	}
	var command model.DeviceCommand
	_, err := c.arbiter.Reserve(cycle.ChamberID, direction, owner, func() error {
		issued, issueErr := c.gates.Request(ctx, cycle, direction, true)
		command = issued
		return issueErr
	})
	return command, err
}

func (c *Coordinator) RequestClose(ctx context.Context, cycle model.TransitCycle, direction model.Direction) (model.DeviceCommand, error) {
	if err := c.policy.ValidateClose(c.Snapshot(cycle.ChamberID), direction); err != nil {
		return model.DeviceCommand{}, err
	}
	return c.gates.Request(ctx, cycle, direction, false)
}

func (c *Coordinator) Release(chamberID, owner string) error {
	if !c.arbiter.Release(chamberID, owner) {
		return fmt.Errorf("reservation owner %q does not control chamber %s", owner, chamberID)
	}
	return nil
}

func (c *Coordinator) Reservation(chamberID string) (Reservation, bool) {
	return c.arbiter.Current(chamberID)
}

func (c *Coordinator) Observe(chamberID string, direction model.Direction, position model.GatePosition, _ time.Time) error {
	return c.gates.Observe(chamberID, direction, position)
}

// RetryCommand safely resends a failed or timed-out command. It enforces the
// safety rules an operator must satisfy before a second attempt is issued:
//   - no emergency interlock active in the chamber,
//   - the failed command belongs to the still-current transit generation,
//   - the command is actually in a failed/timed-out terminal state,
//   - no other attempt of the same intent is already queued/accepted/executing,
//   - for gates, the open/close interlock policy still holds.
//
// The replacement is a brand-new command attempt linked to the original; the
// failed attempt is preserved. The transit stage never moves on a retry.
func (c *Coordinator) RetryCommand(ctx context.Context, cycle model.TransitCycle, commandID uuid.UUID) (model.DeviceCommand, error) {
	command, ok := c.gates.Commands().Get(commandID)
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("command %s not found", commandID)
	}
	if command.ChamberID != cycle.ChamberID || !cycle.IdentityMatches(command.CycleID, command.Generation) {
		return model.DeviceCommand{}, errors.New("command belongs to an inactive transit generation")
	}
	if cycle.Stage.Terminal() {
		return model.DeviceCommand{}, fmt.Errorf("transit is %s, failed commands can no longer be resent", cycle.Stage)
	}
	if !command.State.Failure() {
		return model.DeviceCommand{}, fmt.Errorf("command is %s, only failed or timed-out commands can be resent", command.State)
	}
	if c.emergency != nil && c.emergency(cycle.ChamberID) {
		return model.DeviceCommand{}, fmt.Errorf("interlock emergency active chamber=%s", cycle.ChamberID)
	}
	if c.gates.Commands().HasActiveAttempt(command.RootID) {
		return model.DeviceCommand{}, errors.New("an attempt of this command is already queued or executing")
	}
	if command.IsGate() {
		snapshot := c.Snapshot(cycle.ChamberID)
		direction := model.DirectionUpstream
		if command.Device == model.DeviceDownstreamGate {
			direction = model.DirectionDownstream
		}
		if err := c.policy.ValidateRetry(snapshot, command, direction); err != nil {
			return model.DeviceCommand{}, err
		}
	}
	return c.gates.Retry(ctx, commandID, cycle)
}
