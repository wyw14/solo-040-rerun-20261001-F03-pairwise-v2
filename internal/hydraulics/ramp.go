package hydraulics

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

type RampStep struct {
	Target    float64   `json:"target"`
	IssuedAt  time.Time `json:"issued_at"`
	CommandID uuid.UUID `json:"command_id"`
	Completed bool      `json:"completed"`
}

type RampResult struct {
	Steps     []RampStep `json:"steps"`
	Cancelled bool       `json:"cancelled"`
}

type StepIssuer func(context.Context, model.TransitCycle, float64) (model.DeviceCommand, error)

// Ramp executes each valve target under the caller's lifecycle.
type Ramp struct {
	mu       sync.RWMutex
	interval time.Duration
	history  []RampStep
	now      func() time.Time
}

func NewRamp(interval time.Duration, now func() time.Time) *Ramp {
	if interval <= 0 {
		interval = 20 * time.Millisecond
	}
	if now == nil {
		now = time.Now
	}
	return &Ramp{interval: interval, now: now}
}

func (r *Ramp) Run(ctx context.Context, cycle model.TransitCycle, targets []float64, issue StepIssuer) (RampResult, error) {
	if issue == nil || len(targets) == 0 {
		return RampResult{}, errors.New("ramp requires targets and an issuer")
	}
	result := RampResult{}
	for _, target := range targets {
		if err := ctx.Err(); err != nil {
			result.Cancelled = true
			return result, err
		}
		step := RampStep{Target: target, IssuedAt: r.now().UTC()}
		command, err := issue(ctx, cycle, target)
		if err != nil {
			return result, err
		}
		step.CommandID = command.ID
		step.Completed = true
		result.Steps = append(result.Steps, step)
		r.mu.Lock()
		r.history = append(r.history, step)
		r.mu.Unlock()
		timer := time.NewTimer(r.interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			result.Cancelled = true
			return result, ctx.Err()
		case <-timer.C:
		}
	}
	return result, nil
}

func (r *Ramp) History() []RampStep {
	r.mu.RLock()
	result := append([]RampStep(nil), r.history...)
	r.mu.RUnlock()
	return result
}
