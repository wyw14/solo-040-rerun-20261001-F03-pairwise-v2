package cycle

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// Coordinator is the authoritative owner of active transit generations.
type Coordinator struct {
	mu          sync.RWMutex
	current     map[string]model.TransitCycle
	generations map[string]uint64
	gates       *gate.Controller
	hydraulics  *hydraulics.Controller
	recorder    *journal.Recorder
	emergency   func(string) bool
	now         func() time.Time
}

func NewCoordinator(gates *gate.Controller, hydraulicsController *hydraulics.Controller, recorder *journal.Recorder, now func() time.Time) *Coordinator {
	if now == nil {
		now = time.Now
	}
	return &Coordinator{
		current: make(map[string]model.TransitCycle), generations: make(map[string]uint64),
		gates: gates, hydraulics: hydraulicsController, recorder: recorder, now: now,
	}
}

// BindEmergency supplies the emergency-state check after the safety component
// graph has been wired.
func (c *Coordinator) BindEmergency(active func(string) bool) {
	c.mu.Lock()
	c.emergency = active
	c.mu.Unlock()
}

func (c *Coordinator) Start(ctx context.Context, plan model.ConvoyPlan) (model.TransitCycle, error) {
	c.mu.Lock()
	if active, ok := c.current[plan.ChamberID]; ok && !active.Stage.Terminal() {
		c.mu.Unlock()
		return model.TransitCycle{}, errors.New("chamber already has an active transit")
	}
	c.generations[plan.ChamberID]++
	cycleValue, err := model.NewTransitCycle(plan.ChamberID, plan.ID, plan.Direction, c.generations[plan.ChamberID], c.now())
	if err == nil {
		c.current[plan.ChamberID] = cycleValue
	}
	c.mu.Unlock()
	if err != nil {
		return model.TransitCycle{}, err
	}
	if _, err := c.recorder.Record(ctx, plan.ChamberID, model.EventCycleCreated, cycleValue.ID, cycleValue.Generation, cycleValue); err != nil {
		return model.TransitCycle{}, err
	}
	return cycleValue, nil
}

func (c *Coordinator) Current(chamberID string) (model.TransitCycle, bool) {
	c.mu.RLock()
	cycleValue, ok := c.current[chamberID]
	c.mu.RUnlock()
	return cycleValue.Clone(), ok
}

func (c *Coordinator) Cancel(ctx context.Context, chamberID, reason string) (model.TransitCycle, error) {
	c.mu.Lock()
	cycleValue, ok := c.current[chamberID]
	if !ok || cycleValue.Stage.Terminal() {
		c.mu.Unlock()
		return model.TransitCycle{}, errors.New("no cancellable transit")
	}
	if err := cycleValue.Advance(terminalStage(reason), reason, c.now()); err != nil {
		c.mu.Unlock()
		return model.TransitCycle{}, err
	}
	c.current[chamberID] = cycleValue
	c.mu.Unlock()
	_, err := c.recorder.Record(ctx, chamberID, model.EventCycleAdvanced, cycleValue.ID, cycleValue.Generation, cycleValue)
	return cycleValue, err
}

// ApplyCommandFeedback processes a device progress report. Received and
// executing reports update the command timeline only; the transit keeps
// waiting. Failure and timeout likewise leave the stage untouched so the
// operator sees the cause and can retry safely. Only a settled gate command
// advances the transit.
func (c *Coordinator) ApplyCommandFeedback(ctx context.Context, feedback model.DeviceFeedback) (gate.CommandResult, error) {
	command, ok := c.gates.Commands().Get(feedback.CommandID)
	if !ok {
		return gate.CommandResult{}, errors.New("command not found")
	}
	result, err := c.gates.ApplyFeedback(ctx, feedback)
	if err != nil {
		return gate.CommandResult{}, err
	}
	if !result.Settled {
		return result, nil
	}
	if command.Device != model.DeviceUpstreamGate && command.Device != model.DeviceDownstreamGate {
		return result, nil
	}
	return result, c.advanceForSettledGate(ctx, result.Command)
}

// ApplyGateAck keeps the fully-identified acknowledgement path; it applies a
// settled report and advances the transit when the gate finishes.
func (c *Coordinator) ApplyGateAck(ctx context.Context, ack model.DeviceAck) error {
	command, ok := c.gates.Commands().Get(ack.CommandID)
	if !ok {
		return errors.New("command not found")
	}
	if _, err := c.gates.Confirm(ctx, ack); err != nil {
		return err
	}
	if command.Device != model.DeviceUpstreamGate && command.Device != model.DeviceDownstreamGate {
		return nil
	}
	return c.advanceForSettledGate(ctx, command)
}

func (c *Coordinator) advanceForSettledGate(ctx context.Context, command model.DeviceCommand) error {
	c.mu.Lock()
	cycleValue, ok := c.current[command.ChamberID]
	if !ok || cycleValue.Stage.Terminal() || !cycleValue.IdentityMatches(command.CycleID, command.Generation) {
		c.mu.Unlock()
		return errors.New("device acknowledgement belongs to an inactive transit generation")
	}
	next, err := nextAfterGate(cycleValue.Stage)
	if err == nil {
		err = cycleValue.Advance(next, "matching gate command settled", c.now())
	}
	beginLeveling := next == model.StageLeveling
	if err == nil {
		c.current[command.ChamberID] = cycleValue
	}
	c.mu.Unlock()
	if err != nil {
		return err
	}
	if beginLeveling {
		if err := c.hydraulics.BeginLeveling(cycleValue); err != nil {
			return err
		}
	}
	_, err = c.recorder.Record(ctx, command.ChamberID, model.EventCycleAdvanced, cycleValue.ID, cycleValue.Generation, cycleValue)
	return err
}

// RetryCommand safely re-issues a failed or timed-out command. The new attempt
// is linked to the original and the transit stays in its current stage until
// the replacement settles.
func (c *Coordinator) RetryCommand(ctx context.Context, previousID uuid.UUID) (model.DeviceCommand, error) {
	previous, ok := c.gates.Commands().Get(previousID)
	if !ok {
		return model.DeviceCommand{}, errors.New("command not found")
	}
	if previous.State != model.CommandFailed && previous.State != model.CommandTimedOut {
		return model.DeviceCommand{}, errors.New("only a failed or timed-out command can be retried")
	}
	c.mu.RLock()
	cycleValue, active := c.current[previous.ChamberID]
	emergency := c.emergency
	c.mu.RUnlock()
	if !active || cycleValue.Stage.Terminal() {
		return model.DeviceCommand{}, errors.New("command belongs to a transit that is no longer active")
	}
	if !cycleValue.IdentityMatches(previous.CycleID, previous.Generation) {
		return model.DeviceCommand{}, errors.New("command belongs to an inactive transit generation")
	}
	if emergency != nil && emergency(previous.ChamberID) {
		return model.DeviceCommand{}, errors.New("cannot retry while an emergency stop is active")
	}
	return c.gates.Retry(ctx, previousID, cycleValue)
}

// MarkTimeout ages out one overdue command via the controller so the timeout
// is journaled. The transit remains in its waiting stage.
func (c *Coordinator) MarkTimeout(ctx context.Context, command model.DeviceCommand) error {
	_, err := c.gates.MarkTimeout(ctx, command)
	return err
}

func (c *Coordinator) ApplySignalHandoff(ctx context.Context, cycleID uuid.UUID, generation uint64) error {
	c.mu.Lock()
	var chamberID string
	var cycleValue model.TransitCycle
	for id, candidate := range c.current {
		if candidate.IdentityMatches(cycleID, generation) {
			chamberID, cycleValue = id, candidate
			break
		}
	}
	if chamberID == "" {
		c.mu.Unlock()
		return errors.New("signal handoff transit is not current")
	}
	if err := applySignalTransition(&cycleValue); err != nil {
		c.mu.Unlock()
		return err
	}
	cycleValue.UpdatedAt = c.now().UTC()
	c.current[chamberID] = cycleValue
	c.mu.Unlock()
	_, err := c.recorder.Record(ctx, chamberID, model.EventCycleAdvanced, cycleValue.ID, generation, cycleValue)
	return err
}

func (c *Coordinator) Restore(cycleValue model.TransitCycle) {
	c.mu.Lock()
	c.current[cycleValue.ChamberID] = cycleValue
	if c.generations[cycleValue.ChamberID] < cycleValue.Generation {
		c.generations[cycleValue.ChamberID] = cycleValue.Generation
	}
	c.mu.Unlock()
}
