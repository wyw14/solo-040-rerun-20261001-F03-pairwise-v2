package gate

import (
	"context"
	"errors"
	"fmt"
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

// CommandResult reports a feedback application to the caller. Settled commands
// are the only ones that represent physical completion.
type CommandResult struct {
	Command model.DeviceCommand `json:"command"`
	Settled bool                `json:"settled"`
}

// ApplyFeedback drives one command from a device progress report. Receipt and
// execution are recorded as progress; failure and timeout leave the transit
// waiting; only settled completes the instruction and moves the gate position.
func (c *Controller) ApplyFeedback(ctx context.Context, feedback model.DeviceFeedback) (CommandResult, error) {
	if feedback.Kind == "" {
		return CommandResult{}, errors.New("feedback kind is required")
	}
	if !feedback.Kind.Valid() {
		return CommandResult{}, fmt.Errorf("unknown feedback kind %q", feedback.Kind)
	}
	observed := feedback.ObservedAt
	if observed.IsZero() {
		observed = c.now()
	}
	switch feedback.Kind {
	case model.FeedbackReceived:
		command, err := c.commands.Receive(feedback.CommandID, observed)
		if err != nil {
			return CommandResult{}, err
		}
		if err := c.record(ctx, model.EventCommandAccepted, command); err != nil {
			return CommandResult{}, err
		}
		return CommandResult{Command: command}, nil
	case model.FeedbackExecuting:
		command, err := c.commands.Progress(feedback.CommandID, observed)
		if err != nil {
			return CommandResult{}, err
		}
		if err := c.record(ctx, model.EventCommandExecuting, command); err != nil {
			return CommandResult{}, err
		}
		return CommandResult{Command: command}, nil
	case model.FeedbackFailed:
		reason := feedback.Reason
		if reason == "" {
			reason = "device reported failure without a reason"
		}
		command, err := c.commands.Fail(feedback.CommandID, reason, observed)
		if err != nil {
			return CommandResult{}, err
		}
		if err := c.record(ctx, model.EventCommandFailed, command); err != nil {
			return CommandResult{}, err
		}
		return CommandResult{Command: command}, nil
	case model.FeedbackSettled:
		command, err := c.settle(ctx, feedback.CommandID, observed)
		if err != nil {
			return CommandResult{}, err
		}
		return CommandResult{Command: command, Settled: true}, nil
	}
	return CommandResult{}, fmt.Errorf("unhandled feedback kind %q", feedback.Kind)
}

// MarkTimeout is used by the watchdog when a command breaches its deadline.
func (c *Controller) MarkTimeout(ctx context.Context, command model.DeviceCommand) (model.DeviceCommand, error) {
	reason := ""
	switch command.State {
	case model.CommandQueued:
		reason = "device did not accept the command before the receipt deadline"
	case model.CommandAccepted:
		reason = "device accepted but never started execution before the deadline"
	case model.CommandExecuting:
		reason = "device did not settle before the execution deadline"
	default:
		return model.DeviceCommand{}, errors.New("command is not awaiting a deadline")
	}
	timedOut, err := c.commands.Timeout(command.ID, reason, c.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if err := c.record(ctx, model.EventCommandTimedOut, timedOut); err != nil {
		return model.DeviceCommand{}, err
	}
	return timedOut, nil
}

// Retry re-issues a failed or timed-out instruction under the current transit
// generation, linking the new attempt to the original.
func (c *Controller) Retry(ctx context.Context, previousID uuid.UUID, cycle model.TransitCycle) (model.DeviceCommand, error) {
	previous, ok := c.commands.Get(previousID)
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", previousID)
	}
	retry, err := c.commands.Retry(previousID, cycle, c.now())
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if _, err := c.recorder.Record(ctx, retry.ChamberID, model.EventCommandQueued, retry.CycleID, retry.Generation, retry); err != nil {
		return model.DeviceCommand{}, err
	}
	link := map[string]any{
		"original_id": previous.OriginalID(), "retried_id": previous.ID,
		"new_id": retry.ID, "attempt": retry.Attempt,
	}
	if _, err := c.recorder.Record(ctx, retry.ChamberID, model.EventCommandRetried, retry.CycleID, retry.Generation, link); err != nil {
		return model.DeviceCommand{}, err
	}
	return retry, nil
}

func (c *Controller) settle(ctx context.Context, commandID uuid.UUID, observed time.Time) (model.DeviceCommand, error) {
	command, ok := c.commands.Get(commandID)
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", commandID)
	}
	ack := model.DeviceAck{
		CommandID: command.ID, CycleID: command.CycleID, ChamberID: command.ChamberID,
		Generation: command.Generation, Epoch: command.Epoch, Device: command.Device,
		Settled: true, ObservedAt: observed.UTC(),
	}
	settled, err := c.commands.Settle(ack, observed)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	direction := model.DirectionUpstream
	if settled.Device == model.DeviceDownstreamGate {
		direction = model.DirectionDownstream
	}
	if settled.Device == model.DeviceUpstreamGate || settled.Device == model.DeviceDownstreamGate {
		position := model.GateClosed
		if settled.Action == "open" {
			position = model.GateOpen
		}
		if err := c.state.Observe(settled.ChamberID, direction, position, observed); err != nil {
			return model.DeviceCommand{}, err
		}
	}
	if err := c.record(ctx, model.EventCommandSettled, settled); err != nil {
		return model.DeviceCommand{}, err
	}
	return settled, nil
}

// Confirm applies a fully-identified settled acknowledgement (legacy path).
func (c *Controller) Confirm(ctx context.Context, ack model.DeviceAck) (model.DeviceCommand, error) {
	observed := ack.ObservedAt
	if observed.IsZero() {
		observed = c.now()
	}
	return c.settle(ctx, ack.CommandID, observed)
}

func (c *Controller) record(ctx context.Context, kind model.EventType, command model.DeviceCommand) error {
	_, err := c.recorder.Record(ctx, command.ChamberID, kind, command.CycleID, command.Generation, command)
	return err
}

func (c *Controller) Observe(chamberID string, direction model.Direction, position model.GatePosition) error {
	if chamberID == "" {
		return errors.New("chamber is required")
	}
	return c.state.Observe(chamberID, direction, position, c.now())
}

func (c *Controller) State() *State       { return c.state }
func (c *Controller) Commands() *Commands { return c.commands }
