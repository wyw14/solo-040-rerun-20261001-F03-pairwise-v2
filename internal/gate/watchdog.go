package gate

import (
	"context"
	"log/slog"
	"time"

	"github.com/wyw14/cry-96/internal/model"
)

// Watchdog marks in-progress commands as timed_out when they breach their
// receipt or settle deadlines. A timeout is a terminal failure, not progress:
// the transit keeps waiting until an operator retries the instruction.
type Watchdog struct {
	controller *Controller
	interval   time.Duration
	now        func() time.Time
}

func NewWatchdog(controller *Controller, interval time.Duration, now func() time.Time) *Watchdog {
	if interval <= 0 {
		interval = time.Second
	}
	if now == nil {
		now = time.Now
	}
	return &Watchdog{controller: controller, interval: interval, now: now}
}

// Run blocks until the context is cancelled, sweeping overdue commands once
// per interval.
func (w *Watchdog) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.sweep(ctx)
		}
	}
}

func (w *Watchdog) sweep(ctx context.Context) {
	for _, command := range w.controller.Commands().Overdue(w.now()) {
		if _, err := w.controller.MarkTimeout(ctx, command); err != nil {
			slog.Warn("command timeout sweep failed", "command", command.ID, "error", err)
		}
	}
}

// DueOnce exposes a single sweep for callers that want to drive timeouts
// manually (for example tests).
func (w *Watchdog) DueOnce(ctx context.Context) []model.DeviceCommand {
	overdue := w.controller.Commands().Overdue(w.now())
	timedOut := make([]model.DeviceCommand, 0, len(overdue))
	for _, command := range overdue {
		marked, err := w.controller.MarkTimeout(ctx, command)
		if err == nil {
			timedOut = append(timedOut, marked)
		}
	}
	return timedOut
}
