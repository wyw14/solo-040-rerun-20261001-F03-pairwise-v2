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

// stateRank orders the progressive lifecycle of a single command attempt.
var stateRank = map[CommandState]int{
	CommandQueued:    0,
	CommandAccepted:  1,
	CommandExecuting: 2,
	CommandSettled:   3,
}

func (s CommandState) Terminal() bool {
	return s == CommandSettled || s == CommandCancelled ||
		s == CommandFailed || s == CommandTimedOut
}

// InProgress reports whether the device has taken the command but has not yet
// reached an outcome. Accepted and executing are never completion.
func (s CommandState) InProgress() bool {
	return s == CommandAccepted || s == CommandExecuting
}

// Failure reports a terminal unsuccessful outcome (device failure or timeout).
func (s CommandState) Failure() bool {
	return s == CommandFailed || s == CommandTimedOut
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
	// Attempt is one-based. The first issue is attempt 1; each safe resend of
	// the same intent creates a new command with attempt = previous+1.
	Attempt uint32 `json:"attempt"`
	// RetryOf links a resend to the command attempt it replaces. Empty for the
	// original issue. Keeps the whole retry chain traceable.
	RetryOf uuid.UUID `json:"retry_of,omitempty"`
	// RootID identifies the original command of a retry chain. It equals ID on
	// the first attempt and is copied forward on every resend.
	RootID uuid.UUID `json:"root_id"`
	// Deadline, when set, is when an unacknowledged/in-progress command is
	// automatically marked timed_out by the watchdog.
	Deadline *time.Time `json:"deadline,omitempty"`
	// FailureReason is a human/operator-readable explanation, populated on
	// failed and timed_out commands.
	FailureReason string `json:"failure_reason,omitempty"`
	// FailureCode is a short stable machine code (e.g. "OBSTRUCTED") so the UI
	// and safety rules can branch without parsing prose.
	FailureCode string         `json:"failure_code,omitempty"`
	Timeline    []CommandEvent `json:"timeline"`
	CreatedAt   time.Time      `json:"created_at"`
	UpdatedAt   time.Time      `json:"updated_at"`
}

// IsGate reports whether the command drives a movable gate whose observed
// position matters to the transit stage machine.
func (c DeviceCommand) IsGate() bool {
	return c.Device == DeviceUpstreamGate || c.Device == DeviceDownstreamGate
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
	id := uuid.New()
	return DeviceCommand{
		ID: id, RootID: id, CycleID: cycle.ID, ChamberID: cycle.ChamberID,
		Generation: cycle.Generation, Epoch: epoch, Device: device,
		Action: action, Target: target, State: CommandQueued, Attempt: 1,
		Timeline:  []CommandEvent{{State: CommandQueued, At: now.UTC()}},
		CreatedAt: now.UTC(), UpdatedAt: now.UTC(),
	}, nil
}

// SetState applies a lifecycle transition. Progressive transitions must move
// forward (queued -> accepted -> executing -> settled); terminal outcomes may
// be reached from any non-terminal state. It rejects backward and repeat
// transitions so a received/executing command cannot silently look complete.
func (c *DeviceCommand) SetState(next CommandState, now time.Time) error {
	return c.record(next, "", "", now)
}

// Fail records a terminal unsuccessful outcome with an operator-visible reason.
func (c *DeviceCommand) Fail(next CommandState, code, reason string, now time.Time) error {
	if !next.Failure() {
		return fmt.Errorf("state %s is not a failure outcome", next)
	}
	if reason == "" {
		return errors.New("failure outcome requires a reason")
	}
	if err := c.record(next, code, reason, now); err != nil {
		return err
	}
	return nil
}

func (c *DeviceCommand) record(next CommandState, code, reason string, now time.Time) error {
	if c.State.Terminal() {
		return fmt.Errorf("command %s is already terminal (%s)", c.ID, c.State)
	}
	if next == c.State {
		return fmt.Errorf("command %s is already %s", c.ID, next)
	}
	if !next.Terminal() {
		currentRank, currentOK := stateRank[c.State]
		nextRank, nextOK := stateRank[next]
		if !currentOK || !nextOK || nextRank != currentRank+1 {
			return fmt.Errorf("illegal command transition %s -> %s", c.State, next)
		}
	}
	c.State = next
	if next.Failure() {
		c.FailureCode = code
		c.FailureReason = reason
	}
	c.UpdatedAt = now.UTC()
	c.Timeline = append(c.Timeline, CommandEvent{State: next, Code: code, Reason: reason, At: c.UpdatedAt})
	return nil
}

// CommandEvent is one entry in a command's lifecycle timeline.
type CommandEvent struct {
	State  CommandState `json:"state"`
	Code   string       `json:"code,omitempty"`
	Reason string       `json:"reason,omitempty"`
	At     time.Time    `json:"at"`
}

// MarkAccepted moves queued -> accepted and (re)starts the execution deadline.
func (c *DeviceCommand) MarkAccepted(deadline time.Time, now time.Time) error {
	if err := c.SetState(CommandAccepted, now); err != nil {
		return err
	}
	deadline = deadline.UTC()
	c.Deadline = &deadline
	return nil
}

// WithDeadline sets the watchdog deadline on a freshly queued command.
func (c *DeviceCommand) WithDeadline(deadline time.Time) {
	deadline = deadline.UTC()
	c.Deadline = &deadline
}

// NewRetryIssue builds the next attempt of a failed/timed-out command. It keeps
// the same physical intent (device/action/target) and carries forward the retry
// chain identity. The new command starts a fresh queued lifecycle.
func NewRetryIssue(previous DeviceCommand, cycle TransitCycle, epoch uint64, now time.Time) (DeviceCommand, error) {
	if !previous.State.Failure() {
		return DeviceCommand{}, fmt.Errorf("command %s is %s, only failed or timed-out commands can be resent", previous.ID, previous.State)
	}
	if !cycle.IdentityMatches(previous.CycleID, previous.Generation) {
		return DeviceCommand{}, errors.New("retry must target the same active transit generation")
	}
	retry, err := NewDeviceCommand(cycle, epoch, previous.Device, previous.Action, previous.Target, now)
	if err != nil {
		return DeviceCommand{}, err
	}
	retry.RetryOf = previous.ID
	retry.RootID = previous.RootID
	retry.Attempt = previous.Attempt + 1
	return retry, nil
}

// DeviceFeedback is the report a device (or device gateway) sends about a
// previously queued command. Phase is one of accepted, executing, settled,
// failed. Timeouts are produced server-side by the watchdog, not devices.
type DeviceFeedback struct {
	CommandID  uuid.UUID    `json:"command_id"`
	Phase      CommandState `json:"phase"`
	Code       string       `json:"code,omitempty"`
	Reason     string       `json:"reason,omitempty"`
	ObservedAt time.Time    `json:"observed_at"`
}

// DeviceAck is a settled confirmation carrying the command's fencing identity.
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

// GatePosition and LevelSample below are unchanged.

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
