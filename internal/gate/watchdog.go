package gate

import (
	"context"
	"log/slog"
	"time"
)

// Watchdog periodically converts overdue commands into timed_out failures so a
// silent device cannot leave a transit waiting forever. It is a scheduler, not
// an owner: the Controller performs the state change and journaling.
type Watchdog struct {
	controller *Controller
	interval   time.Duration
}

func NewWatchdog(controller *Controller, interval time.Duration) *Watchdog {
	if interval <= 0 {
		interval = time.Second
	}
	return &Watchdog{controller: controller, interval: interval}
}

// Run blocks until ctx is cancelled, sweeping on every tick.
func (w *Watchdog) Run(ctx context.Context) {
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			timedOut := w.controller.TimeoutExpired(ctx)
			for _, command := range timedOut {
				slog.Warn("device command timed out",
					"chamber", command.ChamberID, "command", command.ID,
					"device", command.Device, "state", command.State,
					"reason", command.FailureReason)
			}
		}
	}
}
