package model

import (
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type DeviceKind string

const (
	DeviceUpstreamGate   DeviceKind = "upstream_gate"
	DeviceDownstreamGate DeviceKind = "downstream_gate"
	DeviceFillValve      DeviceKind = "fill_valve"
	DeviceDrainValve     DeviceKind = "drain_valve"
)

type CommandState string

const (
	CommandQueued    CommandState = "queued"
	CommandAccepted  CommandState = "accepted"
	CommandExecuting CommandState = "executing"
	CommandSettled   CommandState = "settled"
	CommandCancelled CommandState = "cancelled"
	CommandFailed    CommandState = "failed"
	CommandTimedOut  CommandState = "timed_out"
)

func (s CommandState) Terminal() bool {
	return s == CommandSettled || s == CommandCancelled || s == CommandFailed || s == CommandTimedOut
}

// InProgress reports whether the device is still expected to finish the command.
// Acceptance and execution are progress signals, never completion.
func (s CommandState) InProgress() bool {
	return s == CommandQueued || s == CommandAccepted || s == CommandExecuting
}

// allowedTransitions defines the real device lifecycle. A command may jump
// forward (a fast device may settle before the accepted/executing feedback is
// processed) but can never leave a terminal state or move backwards.
var allowedTransitions = map[CommandState]map[CommandState]bool{
	CommandQueued: {
		CommandAccepted: true, CommandExecuting: true, CommandSettled: true,
		CommandFailed: true, CommandTimedOut: true, CommandCancelled: true,
	},
	CommandAccepted: {
		CommandExecuting: true, CommandSettled: true,
		CommandFailed: true, CommandTimedOut: true, CommandCancelled: true,
	},
	CommandExecuting: {
		CommandSettled: true,
		CommandFailed:  true, CommandTimedOut: true, CommandCancelled: true,
	},
}

type DeviceCommand struct {
	ID         uuid.UUID    `json:"id"`
	CycleID    uuid.UUID    `json:"cycle_id"`
	ChamberID  string       `json:"chamber_id"`
	Generation uint64       `json:"generation"`
	Epoch      uint64       `json:"epoch"`
	Device     DeviceKind   `json:"device"`
	Action     string       `json:"action"`
	Target     float64      `json:"target,omitempty"`
	State      CommandState `json:"state"`
	// Attempt is 1 for the original issue and incremented for each safe retry.
	Attempt int `json:"attempt"`
	// AttemptOf links a retried command to the attempt it replaces. Nil on attempt 1.
	AttemptOf *uuid.UUID `json:"attempt_of,omitempty"`
	// Reason carries the device-reported or watchdog-determined failure cause.
	Reason string `json:"reason,omitempty"`
	// AcceptDeadline bounds how long a queued command waits for device receipt.
	AcceptDeadline time.Time `json:"accept_deadline"`
	// SettleDeadline bounds progress once the device has accepted the command.
	SettleDeadline *time.Time `json:"settle_deadline,omitempty"`
	CreatedAt      time.Time  `json:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at"`
	AcceptedAt     *time.Time `json:"accepted_at,omitempty"`
	ExecutingAt    *time.Time `json:"executing_at,omitempty"`
	SettledAt      *time.Time `json:"settled_at,omitempty"`
}

func NewDeviceCommand(cycle TransitCycle, epoch uint64, device DeviceKind, action string, target float64, now time.Time) (DeviceCommand, error) {
	if cycle.ID == uuid.Nil || cycle.ChamberID == "" {
		return DeviceCommand{}, errors.New("command requires a transit cycle")
	}
	if epoch == 0 {
		return DeviceCommand{}, errors.New("command epoch must be positive")
	}
	if device == "" || action == "" {
		return DeviceCommand{}, errors.New("device and action are required")
	}
	stamped := now.UTC()
	return DeviceCommand{
		ID: uuid.New(), CycleID: cycle.ID, ChamberID: cycle.ChamberID,
		Generation: cycle.Generation, Epoch: epoch, Device: device,
		Action: action, Target: target, State: CommandQueued,
		Attempt: 1, CreatedAt: stamped, UpdatedAt: stamped,
	}, nil
}

// RetryOf creates a fresh command for the same device instruction, linked to
// the previous attempt. Identity (new ID, new epoch) is assigned by the
// registry; only the attempt chain is established here.
func RetryOf(previous DeviceCommand, cycle TransitCycle, now time.Time) (DeviceCommand, error) {
	if !previous.State.Terminal() {
		return DeviceCommand{}, errors.New("only a finished command can be retried")
	}
	retry, err := NewDeviceCommand(
		cycle, previous.Epoch, previous.Device, previous.Action, previous.Target, now,
	)
	if err != nil {
		return DeviceCommand{}, err
	}
	origin := previous.OriginalID()
	retry.AttemptOf = &origin
	retry.Attempt = previous.Attempt + 1
	return retry, nil
}

// OriginalID walks the retry chain to the first attempt.
func (c DeviceCommand) OriginalID() uuid.UUID {
	if c.AttemptOf != nil {
		return *c.AttemptOf
	}
	return c.ID
}

func (c *DeviceCommand) SetState(next CommandState, now time.Time) error {
	return c.terminate(next, "", now)
}

// Fail records a terminal state with the cause the operator must see.
func (c *DeviceCommand) Fail(next CommandState, reason string, now time.Time) error {
	if next != CommandFailed && next != CommandTimedOut {
		return fmt.Errorf("state %s is not a failure state", next)
	}
	if reason == "" {
		return errors.New("failure reason is required")
	}
	return c.terminate(next, reason, now)
}

func (c *DeviceCommand) terminate(next CommandState, reason string, now time.Time) error {
	if c.State == next {
		return nil
	}
	if c.State.Terminal() {
		return fmt.Errorf("command %s is already terminal (%s)", c.ID, c.State)
	}
	if next != c.State && !allowedTransitions[c.State][next] {
		return fmt.Errorf("command %s cannot transition %s -> %s", c.ID, c.State, next)
	}
	stamped := now.UTC()
	c.State = next
	c.UpdatedAt = stamped
	switch next {
	case CommandAccepted:
		c.AcceptedAt = &stamped
	case CommandExecuting:
		if c.AcceptedAt == nil {
			c.AcceptedAt = &stamped
		}
		c.ExecutingAt = &stamped
	case CommandSettled:
		c.SettledAt = &stamped
		c.Reason = ""
	case CommandFailed, CommandTimedOut:
		c.Reason = reason
	}
	return nil
}

// MarkAccepted stamps device receipt without changing the settled contract.
func (c *DeviceCommand) MarkAccepted(now time.Time) error {
	return c.SetState(CommandAccepted, now)
}

// MarkExecuting stamps the start of physical actuation.
func (c *DeviceCommand) MarkExecuting(now time.Time) error {
	return c.SetState(CommandExecuting, now)
}

// SetSettleDeadline records when an accepted command must finish.
func (c *DeviceCommand) SetSettleDeadline(deadline time.Time) {
	stamped := deadline.UTC()
	c.SettleDeadline = &stamped
}

// Overdue reports whether the command has breached its receipt or settle deadline.
func (c DeviceCommand) Overdue(now time.Time) bool {
	if !c.State.InProgress() {
		return false
	}
	stamped := now.UTC()
	if c.State == CommandQueued && !c.AcceptDeadline.IsZero() && stamped.After(c.AcceptDeadline) {
		return true
	}
	if (c.State == CommandAccepted || c.State == CommandExecuting) && c.SettleDeadline != nil && stamped.After(*c.SettleDeadline) {
		return true
	}
	return false
}

// CommandTimelineEntry is one observed point in a command's real progress.
type CommandTimelineEntry struct {
	State CommandState `json:"state"`
	At    time.Time    `json:"at"`
	Note  string       `json:"note,omitempty"`
}

// Timeline reconstructs the operator-visible progress from recorded stamps.
func (c DeviceCommand) Timeline() []CommandTimelineEntry {
	entries := []CommandTimelineEntry{{State: CommandQueued, At: c.CreatedAt}}
	if c.AcceptedAt != nil {
		entries = append(entries, CommandTimelineEntry{State: CommandAccepted, At: *c.AcceptedAt})
	}
	if c.ExecutingAt != nil {
		entries = append(entries, CommandTimelineEntry{State: CommandExecuting, At: *c.ExecutingAt})
	}
	if c.State.Terminal() {
		entry := CommandTimelineEntry{State: c.State, At: c.UpdatedAt}
		if c.State == CommandFailed || c.State == CommandTimedOut {
			entry.Note = c.Reason
		}
		entries = append(entries, entry)
	}
	return entries
}

// FeedbackKind is the progress signal reported by the field device.
type FeedbackKind string

const (
	FeedbackReceived  FeedbackKind = "received"
	FeedbackExecuting FeedbackKind = "executing"
	FeedbackFailed    FeedbackKind = "failed"
	FeedbackSettled   FeedbackKind = "settled"
)

func (k FeedbackKind) Valid() bool {
	switch k {
	case FeedbackReceived, FeedbackExecuting, FeedbackFailed, FeedbackSettled:
		return true
	}
	return false
}

// DeviceFeedback is the inbound device report for one command. The device only
// knows the command ID it was given; the registry verifies cycle identity.
type DeviceFeedback struct {
	CommandID  uuid.UUID    `json:"command_id"`
	Kind       FeedbackKind `json:"kind"`
	Reason     string       `json:"reason,omitempty"`
	ObservedAt time.Time    `json:"observed_at"`
}

func (f DeviceFeedback) State() (CommandState, error) {
	switch f.Kind {
	case FeedbackReceived:
		return CommandAccepted, nil
	case FeedbackExecuting:
		return CommandExecuting, nil
	case FeedbackFailed:
		return CommandFailed, nil
	case FeedbackSettled:
		return CommandSettled, nil
	default:
		return "", fmt.Errorf("unknown feedback kind %q", f.Kind)
	}
}

type DeviceAck struct {
	CommandID  uuid.UUID  `json:"command_id"`
	CycleID    uuid.UUID  `json:"cycle_id"`
	ChamberID  string     `json:"chamber_id"`
	Generation uint64     `json:"generation"`
	Epoch      uint64     `json:"epoch"`
	Device     DeviceKind `json:"device"`
	Settled    bool       `json:"settled"`
	ObservedAt time.Time  `json:"observed_at"`
}

func (a DeviceAck) Matches(command DeviceCommand) bool {
	return a.CommandID == command.ID && a.CycleID == command.CycleID &&
		a.Generation == command.Generation && a.Epoch == command.Epoch &&
		a.ChamberID == command.ChamberID && a.Device == command.Device
}

type GatePosition string

const (
	GateOpen    GatePosition = "open"
	GateOpening GatePosition = "opening"
	GateClosed  GatePosition = "closed"
	GateClosing GatePosition = "closing"
)

func (p GatePosition) Sealed() bool { return p == GateClosed }

type LevelSample struct {
	CycleID    uuid.UUID `json:"cycle_id"`
	Generation uint64    `json:"generation"`
	Meters     float64   `json:"meters"`
	ObservedAt time.Time `json:"observed_at"`
}
