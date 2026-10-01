package gate

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

// Default time budgets for a device command. A queued command must be received
// (accepted) within AcceptTimeout; once executing it must settle within
// ExecuteTimeout. The watchdog enforces both.
const (
	DefaultAcceptTimeout  = 30 * time.Second
	DefaultExecuteTimeout = 60 * time.Second
)

// Commands owns command identity, lifecycle and late-ack classification.
type Commands struct {
	mu             sync.RWMutex
	byID           map[uuid.UUID]model.DeviceCommand
	byChamber      map[string][]uuid.UUID
	nextEpoch      map[string]uint64
	acceptTimeout  time.Duration
	executeTimeout time.Duration
}

func NewCommands() *Commands {
	return &Commands{
		byID:           make(map[uuid.UUID]model.DeviceCommand),
		byChamber:      make(map[string][]uuid.UUID),
		nextEpoch:      make(map[string]uint64),
		acceptTimeout:  DefaultAcceptTimeout,
		executeTimeout: DefaultExecuteTimeout,
	}
}

// WithTimeouts overrides the watchdog budgets (zero values keep the default).
func (c *Commands) WithTimeouts(accept, execute time.Duration) *Commands {
	if accept > 0 {
		c.acceptTimeout = accept
	}
	if execute > 0 {
		c.executeTimeout = execute
	}
	return c
}

func epochKey(chamberID string, device model.DeviceKind) string {
	return chamberID + ":" + string(device)
}

func (c *Commands) Issue(cycle model.TransitCycle, device model.DeviceKind, action string, target float64, now time.Time) (model.DeviceCommand, error) {
	return c.issue(cycle, device, action, target, now, c.acceptTimeout)
}

// IssueUnsupervised records a command that is driven internally (e.g. a
// hydraulic-controller valve setpoint) and has no field acknowledgement loop.
// It gets no watchdog deadline, so it cannot generate spurious timeouts; it
// still appears on the command timeline.
func (c *Commands) IssueUnsupervised(cycle model.TransitCycle, device model.DeviceKind, action string, target float64, now time.Time) (model.DeviceCommand, error) {
	return c.issue(cycle, device, action, target, now, 0)
}

func (c *Commands) issue(cycle model.TransitCycle, device model.DeviceKind, action string, target float64, now time.Time, acceptTimeout time.Duration) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := epochKey(cycle.ChamberID, device)
	c.nextEpoch[key]++
	command, err := model.NewDeviceCommand(cycle, c.nextEpoch[key], device, action, target, now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	if acceptTimeout > 0 {
		command.WithDeadline(now.Add(acceptTimeout))
	}
	c.byID[command.ID] = command
	c.byChamber[cycle.ChamberID] = append(c.byChamber[cycle.ChamberID], command.ID)
	return command, nil
}

// Retry issues a fresh attempt for a failed/timed-out command. The original
// attempt is left terminal and linked from the new command.
func (c *Commands) Retry(previousID uuid.UUID, cycle model.TransitCycle, now time.Time) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	previous, ok := c.byID[previousID]
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", previousID)
	}
	key := epochKey(cycle.ChamberID, previous.Device)
	c.nextEpoch[key]++
	command, err := model.NewRetryIssue(previous, cycle, c.nextEpoch[key], now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	command.WithDeadline(now.Add(c.acceptTimeout))
	c.byID[command.ID] = command
	c.byChamber[cycle.ChamberID] = append(c.byChamber[cycle.ChamberID], command.ID)
	return command, nil
}

func (c *Commands) Accept(commandID uuid.UUID, now time.Time) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	command, ok := c.byID[commandID]
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", commandID)
	}
	if err := command.MarkAccepted(now.Add(c.executeTimeout), now); err != nil {
		return model.DeviceCommand{}, err
	}
	c.byID[commandID] = command
	return command, nil
}

func (c *Commands) Execute(commandID uuid.UUID, now time.Time) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	command, ok := c.byID[commandID]
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", commandID)
	}
	if err := command.SetState(model.CommandExecuting, now); err != nil {
		return model.DeviceCommand{}, err
	}
	c.byID[commandID] = command
	return command, nil
}

// Fail records a device-reported terminal failure with an operator-visible
// reason and optional stable code.
func (c *Commands) Fail(commandID uuid.UUID, code, reason string, now time.Time) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	command, ok := c.byID[commandID]
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", commandID)
	}
	if err := command.Fail(model.CommandFailed, code, reason, now); err != nil {
		return model.DeviceCommand{}, err
	}
	c.byID[commandID] = command
	return command, nil
}

// Timeout marks an overdue command timed_out. Used by the watchdog.
func (c *Commands) Timeout(commandID uuid.UUID, reason string, now time.Time) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	command, ok := c.byID[commandID]
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", commandID)
	}
	if err := command.Fail(model.CommandTimedOut, "TIMEOUT", reason, now); err != nil {
		return model.DeviceCommand{}, err
	}
	c.byID[commandID] = command
	return command, nil
}

func (c *Commands) Settle(ack model.DeviceAck, now time.Time) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	command, ok := c.byID[ack.CommandID]
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", ack.CommandID)
	}
	if !ack.Matches(command) {
		return model.DeviceCommand{}, errors.New("ack identity does not match command generation")
	}
	if !ack.Settled {
		return model.DeviceCommand{}, errors.New("device has not settled")
	}
	if err := command.SetState(model.CommandSettled, now); err != nil {
		return model.DeviceCommand{}, err
	}
	c.byID[command.ID] = command
	return command, nil
}

func (c *Commands) CancelChamber(chamberID string, now time.Time) []model.DeviceCommand {
	c.mu.Lock()
	defer c.mu.Unlock()
	var cancelled []model.DeviceCommand
	for _, commandID := range c.byChamber[chamberID] {
		command := c.byID[commandID]
		if command.State.Terminal() {
			continue
		}
		if command.SetState(model.CommandCancelled, now) == nil {
			c.byID[commandID] = command
			cancelled = append(cancelled, command)
		}
	}
	return cancelled
}

func (c *Commands) Pending(chamberID string) []model.DeviceCommand {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var pending []model.DeviceCommand
	for _, commandID := range c.byChamber[chamberID] {
		command := c.byID[commandID]
		if !command.State.Terminal() {
			pending = append(pending, command)
		}
	}
	return pending
}

// List returns every command recorded for a chamber in issue order, including
// terminal attempts, so the UI can render the full timeline and retry chain.
func (c *Commands) List(chamberID string) []model.DeviceCommand {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := c.byChamber[chamberID]
	result := make([]model.DeviceCommand, 0, len(ids))
	for _, commandID := range ids {
		result = append(result, c.byID[commandID])
	}
	return result
}

// Overdue returns non-terminal commands whose watchdog deadline has passed.
func (c *Commands) Overdue(now time.Time) []model.DeviceCommand {
	c.mu.RLock()
	defer c.mu.RUnlock()
	now = now.UTC()
	var overdue []model.DeviceCommand
	for _, command := range c.byID {
		if command.State.Terminal() || command.Deadline == nil {
			continue
		}
		if !now.Before(*command.Deadline) {
			overdue = append(overdue, command)
		}
	}
	return overdue
}

func (c *Commands) Get(commandID uuid.UUID) (model.DeviceCommand, bool) {
	c.mu.RLock()
	command, ok := c.byID[commandID]
	c.mu.RUnlock()
	return command, ok
}

// HasActiveAttempt reports whether any command sharing rootID is still
// non-terminal. Used to block issuing a parallel resend of the same intent.
func (c *Commands) HasActiveAttempt(rootID uuid.UUID) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, command := range c.byID {
		if command.RootID == rootID && !command.State.Terminal() {
			return true
		}
	}
	return false
}
