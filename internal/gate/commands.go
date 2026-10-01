package gate

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

const (
	defaultAcceptTimeout = 30 * time.Second
	defaultSettleTimeout = 90 * time.Second
)

// Commands owns command identity, lifecycle, retry chains and late-ack classification.
type Commands struct {
	mu            sync.RWMutex
	byID          map[uuid.UUID]model.DeviceCommand
	byChamber     map[string][]uuid.UUID
	byCycle       map[uuid.UUID][]uuid.UUID
	chain         map[uuid.UUID][]uuid.UUID
	nextEpoch     map[string]uint64
	acceptTimeout time.Duration
	settleTimeout time.Duration
}

func NewCommands() *Commands {
	return NewCommandsWithTimeouts(defaultAcceptTimeout, defaultSettleTimeout)
}

func NewCommandsWithTimeouts(acceptTimeout, settleTimeout time.Duration) *Commands {
	if acceptTimeout <= 0 {
		acceptTimeout = defaultAcceptTimeout
	}
	if settleTimeout <= 0 {
		settleTimeout = defaultSettleTimeout
	}
	return &Commands{
		byID:          make(map[uuid.UUID]model.DeviceCommand),
		byChamber:     make(map[string][]uuid.UUID),
		byCycle:       make(map[uuid.UUID][]uuid.UUID),
		chain:         make(map[uuid.UUID][]uuid.UUID),
		nextEpoch:     make(map[string]uint64),
		acceptTimeout: acceptTimeout,
		settleTimeout: settleTimeout,
	}
}

func epochKey(chamberID string, device model.DeviceKind) string {
	return chamberID + ":" + string(device)
}

// register stores an already-built command and indexes it by chamber, cycle
// and retry chain. The caller must hold the write lock.
func (c *Commands) register(command model.DeviceCommand) {
	c.byID[command.ID] = command
	c.byChamber[command.ChamberID] = append(c.byChamber[command.ChamberID], command.ID)
	c.byCycle[command.CycleID] = append(c.byCycle[command.CycleID], command.ID)
	origin := command.OriginalID()
	c.chain[origin] = append(c.chain[origin], command.ID)
	key := epochKey(command.ChamberID, command.Device)
	if command.Epoch > c.nextEpoch[key] {
		c.nextEpoch[key] = command.Epoch
	}
}

func (c *Commands) Issue(cycle model.TransitCycle, device model.DeviceKind, action string, target float64, now time.Time) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := epochKey(cycle.ChamberID, device)
	c.nextEpoch[key]++
	command, err := model.NewDeviceCommand(cycle, c.nextEpoch[key], device, action, target, now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	command.AcceptDeadline = now.UTC().Add(c.acceptTimeout)
	c.register(command)
	return command, nil
}

// Receive records that the field device accepted (queued -> accepted) and arms
// the settle deadline. It is progress, not completion.
func (c *Commands) Receive(commandID uuid.UUID, now time.Time) (model.DeviceCommand, error) {
	return c.apply(commandID, func(command *model.DeviceCommand) error {
		if err := command.MarkAccepted(now); err != nil {
			return err
		}
		deadline := now.UTC().Add(c.settleTimeout)
		command.SetSettleDeadline(deadline)
		return nil
	})
}

// Progress records the start of physical actuation (accepted -> executing).
func (c *Commands) Progress(commandID uuid.UUID, now time.Time) (model.DeviceCommand, error) {
	return c.apply(commandID, func(command *model.DeviceCommand) error {
		if err := command.MarkExecuting(now); err != nil {
			return err
		}
		if command.SettleDeadline == nil {
			deadline := now.UTC().Add(c.settleTimeout)
			command.SetSettleDeadline(deadline)
		}
		return nil
	})
}

// Fail records a device-reported failure with the cause shown to the operator.
func (c *Commands) Fail(commandID uuid.UUID, reason string, now time.Time) (model.DeviceCommand, error) {
	return c.apply(commandID, func(command *model.DeviceCommand) error {
		return command.Fail(model.CommandFailed, reason, now)
	})
}

// Timeout records a watchdog-determined deadline breach.
func (c *Commands) Timeout(commandID uuid.UUID, reason string, now time.Time) (model.DeviceCommand, error) {
	return c.apply(commandID, func(command *model.DeviceCommand) error {
		return command.Fail(model.CommandTimedOut, reason, now)
	})
}

func (c *Commands) apply(commandID uuid.UUID, mutate func(*model.DeviceCommand) error) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	command, ok := c.byID[commandID]
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", commandID)
	}
	if err := mutate(&command); err != nil {
		return model.DeviceCommand{}, err
	}
	c.byID[commandID] = command
	return command, nil
}

func (c *Commands) transition(commandID uuid.UUID, state model.CommandState, now time.Time) error {
	_, err := c.apply(commandID, func(command *model.DeviceCommand) error {
		return command.SetState(state, now)
	})
	return err
}

// Accept and Execute are retained for callers that drive the lifecycle without
// re-arming deadlines (e.g. replaying observed progress).
func (c *Commands) Accept(commandID uuid.UUID, now time.Time) error {
	return c.transition(commandID, model.CommandAccepted, now)
}

func (c *Commands) Execute(commandID uuid.UUID, now time.Time) error {
	return c.transition(commandID, model.CommandExecuting, now)
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

// Retry issues a fresh command for the same instruction after a failed or
// timed-out attempt. The new attempt carries a new identity and epoch but is
// linked to the original so the timeline stays coherent.
func (c *Commands) Retry(previousID uuid.UUID, cycle model.TransitCycle, now time.Time) (model.DeviceCommand, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	previous, ok := c.byID[previousID]
	if !ok {
		return model.DeviceCommand{}, fmt.Errorf("unknown command %s", previousID)
	}
	if !previous.State.Terminal() || previous.State == model.CommandSettled || previous.State == model.CommandCancelled {
		return model.DeviceCommand{}, errors.New("only a failed or timed-out command can be retried")
	}
	if !cycle.IdentityMatches(previous.CycleID, previous.Generation) {
		return model.DeviceCommand{}, errors.New("retry must target the active transit generation")
	}
	retry, err := model.RetryOf(previous, cycle, now)
	if err != nil {
		return model.DeviceCommand{}, err
	}
	key := epochKey(cycle.ChamberID, previous.Device)
	c.nextEpoch[key]++
	retry.Epoch = c.nextEpoch[key]
	retry.AcceptDeadline = now.UTC().Add(c.acceptTimeout)
	c.register(retry)
	return retry, nil
}

// Overdue returns in-progress commands that have breached a deadline. The
// watchdog turns these into timed_out terminal states.
func (c *Commands) Overdue(now time.Time) []model.DeviceCommand {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var overdue []model.DeviceCommand
	for _, command := range c.byID {
		if command.Overdue(now) {
			overdue = append(overdue, command)
		}
	}
	return overdue
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

// Pending returns commands the device must still finish. Failed and timed-out
// attempts are excluded: they block the transit but the device is no longer
// working on them.
func (c *Commands) Pending(chamberID string) []model.DeviceCommand {
	c.mu.RLock()
	defer c.mu.RUnlock()
	pending := make([]model.DeviceCommand, 0)
	for _, commandID := range c.byChamber[chamberID] {
		command := c.byID[commandID]
		if command.State.InProgress() {
			pending = append(pending, command)
		}
	}
	return pending
}

// Blocking returns failed or timed-out attempts that hold the transit waiting
// for operator retry.
func (c *Commands) Blocking(chamberID string) []model.DeviceCommand {
	c.mu.RLock()
	defer c.mu.RUnlock()
	blocking := make([]model.DeviceCommand, 0)
	for _, commandID := range c.byChamber[chamberID] {
		command := c.byID[commandID]
		if command.State == model.CommandFailed || command.State == model.CommandTimedOut {
			if !c.resolvedByRetryLocked(command) {
				blocking = append(blocking, command)
			}
		}
	}
	return blocking
}

func (c *Commands) resolvedByRetryLocked(command model.DeviceCommand) bool {
	for _, candidateID := range c.chain[command.OriginalID()] {
		candidate := c.byID[candidateID]
		if candidate.Attempt <= command.Attempt {
			continue
		}
		// A later attempt that is still progressing or has settled removes
		// this failure from the blocking set. A later failed attempt does not.
		if !candidate.State.Terminal() || candidate.State == model.CommandSettled {
			return true
		}
	}
	return false
}

// Attempts returns the full retry chain for one original instruction, in order.
func (c *Commands) Attempts(originalID uuid.UUID) []model.DeviceCommand {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := c.chain[originalID]
	result := make([]model.DeviceCommand, 0, len(ids))
	for _, id := range ids {
		result = append(result, c.byID[id])
	}
	return result
}

func (c *Commands) ForCycle(cycleID uuid.UUID) []model.DeviceCommand {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := c.byCycle[cycleID]
	result := make([]model.DeviceCommand, 0, len(ids))
	for _, id := range ids {
		result = append(result, c.byID[id])
	}
	return result
}

// History returns every command for a chamber in issue order.
func (c *Commands) History(chamberID string) []model.DeviceCommand {
	c.mu.RLock()
	defer c.mu.RUnlock()
	ids := c.byChamber[chamberID]
	result := make([]model.DeviceCommand, 0, len(ids))
	for _, id := range ids {
		result = append(result, c.byID[id])
	}
	return result
}

func (c *Commands) Get(commandID uuid.UUID) (model.DeviceCommand, bool) {
	c.mu.RLock()
	command, ok := c.byID[commandID]
	c.mu.RUnlock()
	return command, ok
}

// Restore rebuilds registry state from a replayed command event. Each command
// event carries the full post-transition snapshot, so an existing entry is
// replaced in place without re-appending it to the chamber/cycle/chain indexes.
func (c *Commands) Restore(command model.DeviceCommand) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.byID[command.ID]; ok {
		if existing.UpdatedAt.After(command.UpdatedAt) {
			return
		}
		c.byID[command.ID] = command
		return
	}
	c.register(command)
}
