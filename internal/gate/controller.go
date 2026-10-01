package gate

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Controller links physical observations, command lifecycle and the journal.
type Controller struct {
	state    *State
	commands *Commands
	recorder *journal.Recorder
	now      func() time.Time
}

func NewController(state *State, commands *Commands, recorder *journal.Recorder, now func() time.Time) *Controller {
	if now == nil {
		now = time.Now
	}
	return &Controller{state: state, commands: commands, recorder: recorder, now: now}
}

func (c *Controller) Commands() *Commands { return c.commands }

func (c *Controller) Request(ctx context.Context, cycle model.TransitCycle, direction model.Direction, open bool) (model.DeviceCommand, error) {
	if err := ctx.Err(); err != nil {
		return model.DeviceCommand{}, err
	}
	device := model.DeviceUpstreamGate
	if direction == model.DirectionDownstream {
		device = model.DeviceDownstreamGate
	}
	action := "close"
	position := model.GateClosing
	if open {
		action = "open"
		position = model.GateOpening
	}
	command, err := c.commands.Issue(cycle, device, action, 0, c.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if err := c.state.Observe(cycle.ChamberID, direction, position, c.now()); err != nil {
		return model.DeviceCommand{}, err
	}
	_, err = c.recorder.Record(ctx, cycle.ChamberID, model.EventCommandQueued, cycle.ID, cycle.Generation, command)
	return command, err
}

// ApplyFeedback advances one command according to a device report. Accepted and
// executing are recorded as progress only; they never move physical gate
// position to a terminal seat and never advance the transit. Only a settled
// report does that. A failed report records the operator-visible reason.
func (c *Controller) ApplyFeedback(ctx context.Context, feedback model.DeviceFeedback) (model.DeviceCommand, error) {
	if err := ctx.Err(); err != nil {
		return model.DeviceCommand{}, err
	}
	observed := feedback.ObservedAt
	if observed.IsZero() {
		observed = c.now()
	}
	switch feedback.Phase {
	case model.CommandAccepted:
		command, err := c.commands.Accept(feedback.CommandID, observed)
		if err != nil {
			return model.DeviceCommand{}, err
		}
		_, err = c.recorder.Record(ctx, command.ChamberID, model.EventCommandAccepted, command.CycleID, command.Generation, command)
		return command, err
	case model.CommandExecuting:
		command, err := c.commands.Execute(feedback.CommandID, observed)
		if err != nil {
			return model.DeviceCommand{}, err
		}
		_, err = c.recorder.Record(ctx, command.ChamberID, model.EventCommandExecuting, command.CycleID, command.Generation, command)
		return command, err
	case model.CommandFailed:
		reason := feedback.Reason
		if reason == "" {
			reason = "device reported failure without a reason"
		}
		command, err := c.commands.Fail(feedback.CommandID, feedback.Code, reason, observed)
		if err != nil {
			return model.DeviceCommand{}, err
		}
		_, err = c.recorder.Record(ctx, command.ChamberID, model.EventCommandFailed, command.CycleID, command.Generation, command)
		return command, err
	case model.CommandSettled:
		return c.settle(ctx, feedback.CommandID, observed)
	default:
		return model.DeviceCommand{}, errors.New("feedback phase must be accepted, executing, settled or failed")
	}
}

func (c *Controller) settle(ctx context.Context, commandID uuid.UUID, observed time.Time) (model.DeviceCommand, error) {
	command, ok := c.commands.Get(commandID)
	if !ok {
		return model.DeviceCommand{}, errors.New("command not found")
	}
	// The ack identity is taken from the stored command, not device-supplied
	// fields, so a stale device cannot settle a newer generation.
	ack := model.DeviceAck{
		CommandID: command.ID, CycleID: command.CycleID, ChamberID: command.ChamberID,
		Generation: command.Generation, Epoch: command.Epoch, Device: command.Device,
		Settled: true, ObservedAt: observed.UTC(),
	}
	settled, err := c.commands.Settle(ack, c.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	// Only a gate's settled report means a physical gate reached its seat.
	if settled.IsGate() {
		direction := model.DirectionUpstream
		if settled.Device == model.DeviceDownstreamGate {
			direction = model.DirectionDownstream
		}
		position := model.GateClosed
		if settled.Action == "open" {
			position = model.GateOpen
		}
		if err := c.state.Observe(settled.ChamberID, direction, position, c.now()); err != nil {
			return model.DeviceCommand{}, err
		}
	}
	_, err = c.recorder.Record(ctx, settled.ChamberID, model.EventCommandSettled, settled.CycleID, settled.Generation, settled)
	return settled, err
}

// Retry safely resends a failed/timed-out command as a new attempt linked to the
// original. The previous attempt stays terminal; the new one starts queued.
func (c *Controller) Retry(ctx context.Context, commandID uuid.UUID, cycle model.TransitCycle) (model.DeviceCommand, error) {
	if err := ctx.Err(); err != nil {
		return model.DeviceCommand{}, err
	}
	previous, ok := c.commands.Get(commandID)
	if !ok {
		return model.DeviceCommand{}, errors.New("command not found")
	}
	if previous.ChamberID != cycle.ChamberID {
		return model.DeviceCommand{}, errors.New("command belongs to a different chamber")
	}
	command, err := c.commands.Retry(commandID, cycle, c.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if command.IsGate() {
		direction := model.DirectionUpstream
		if command.Device == model.DeviceDownstreamGate {
			direction = model.DirectionDownstream
		}
		position := model.GateClosing
		if command.Action == "open" {
			position = model.GateOpening
		}
		if err := c.state.Observe(command.ChamberID, direction, position, c.now()); err != nil {
			return model.DeviceCommand{}, err
		}
	}
	_, err = c.recorder.Record(ctx, command.ChamberID, model.EventCommandRetried, command.CycleID, command.Generation, command)
	return command, err
}

// TimeoutExpired is invoked by the watchdog. It times out every overdue command
// and journals it as a failure so operators and the incident review see it.
func (c *Controller) TimeoutExpired(ctx context.Context) []model.DeviceCommand {
	var timedOut []model.DeviceCommand
	for _, command := range c.commands.Overdue(c.now()) {
		reason := "device did not accept the command before the receive deadline"
		if command.State.InProgress() {
			reason = "device accepted the command but did not settle before the execution deadline"
		}
		failed, err := c.commands.Timeout(command.ID, reason, c.now())
		if err != nil {
			continue
		}
		if _, err := c.recorder.Record(ctx, failed.ChamberID, model.EventCommandFailed, failed.CycleID, failed.Generation, failed); err != nil {
			continue
		}
		timedOut = append(timedOut, failed)
	}
	return timedOut
}

func (c *Controller) Observe(chamberID string, direction model.Direction, position model.GatePosition) error {
	if chamberID == "" {
		return errors.New("chamber is required")
	}
	return c.state.Observe(chamberID, direction, position, c.now())
}

func (c *Controller) State() *State { return c.state }
