package gate

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

func testCycle() model.TransitCycle {
	return model.TransitCycle{
		ID: uuid.New(), ChamberID: "chamber-1", PlanID: uuid.New(),
		Direction: model.DirectionUpstream, Stage: model.StagePlanned, Generation: 1,
	}
}

func newTestController(accept, settle time.Duration) (*Controller, *Commands) {
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, time.Now)
	commands := NewCommandsWithTimeouts(accept, settle)
	state := NewState("chamber-1")
	return NewController(state, commands, recorder, time.Now), commands
}

// Receipt and execution are progress only: neither settles the command.
func TestFeedbackProgressIsNotCompletion(t *testing.T) {
	controller, _ := newTestController(time.Minute, time.Minute)
	ctx := context.Background()
	cycle := testCycle()

	command, err := controller.Request(ctx, cycle, model.DirectionUpstream, true)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	received, err := controller.ApplyFeedback(ctx, model.DeviceFeedback{
		CommandID: command.ID, Kind: model.FeedbackReceived,
	})
	if err != nil {
		t.Fatalf("received: %v", err)
	}
	if received.Settled || received.Command.State != model.CommandAccepted {
		t.Fatalf("received must not settle, got settled=%v state=%s", received.Settled, received.Command.State)
	}

	executing, err := controller.ApplyFeedback(ctx, model.DeviceFeedback{
		CommandID: command.ID, Kind: model.FeedbackExecuting,
	})
	if err != nil {
		t.Fatalf("executing: %v", err)
	}
	if executing.Settled || executing.Command.State != model.CommandExecuting {
		t.Fatalf("executing must not settle, got settled=%v state=%s", executing.Settled, executing.Command.State)
	}

	if pending := controller.Commands().Pending("chamber-1"); len(pending) != 1 {
		t.Fatalf("executing command must remain pending, got %d", len(pending))
	}
}

// A failure records the reason, is terminal and never reports completion.
func TestFeedbackFailureKeepsReason(t *testing.T) {
	controller, _ := newTestController(time.Minute, time.Minute)
	ctx := context.Background()
	command, err := controller.Request(ctx, testCycle(), model.DirectionUpstream, true)
	if err != nil {
		t.Fatalf("request: %v", err)
	}

	failed, err := controller.ApplyFeedback(ctx, model.DeviceFeedback{
		CommandID: command.ID, Kind: model.FeedbackFailed, Reason: "hydraulic overpressure",
	})
	if err != nil {
		t.Fatalf("failure: %v", err)
	}
	if failed.Settled {
		t.Fatal("failed feedback must never settle")
	}
	if failed.Command.State != model.CommandFailed || failed.Command.Reason != "hydraulic overpressure" {
		t.Fatalf("unexpected failed command: %+v", failed.Command)
	}
	if pending := controller.Commands().Pending("chamber-1"); len(pending) != 0 {
		t.Fatalf("failed command must not be pending, got %d", len(pending))
	}
	blocking := controller.Commands().Blocking("chamber-1")
	if len(blocking) != 1 {
		t.Fatalf("failed command must block the transit, got %d", len(blocking))
	}

	// A settled report after failure must be rejected (terminal state).
	if _, err := controller.ApplyFeedback(ctx, model.DeviceFeedback{
		CommandID: command.ID, Kind: model.FeedbackSettled,
	}); err == nil {
		t.Fatal("late settle after failure must be rejected")
	}
}

// Retry issues a linked attempt that clears the blocking flag until it settles.
func TestRetryLinksAttempts(t *testing.T) {
	controller, _ := newTestController(time.Minute, time.Minute)
	ctx := context.Background()
	cycle := testCycle()
	command, err := controller.Request(ctx, cycle, model.DirectionUpstream, true)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if _, err := controller.ApplyFeedback(ctx, model.DeviceFeedback{
		CommandID: command.ID, Kind: model.FeedbackFailed, Reason: "jam",
	}); err != nil {
		t.Fatalf("failure: %v", err)
	}

	retry, err := controller.Retry(ctx, command.ID, cycle)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry.Attempt != 2 {
		t.Fatalf("retry attempt = %d, want 2", retry.Attempt)
	}
	if retry.AttemptOf == nil || *retry.AttemptOf != command.ID {
		t.Fatalf("retry must link to original attempt %s, got %v", command.ID, retry.AttemptOf)
	}
	if retry.ID == command.ID || retry.Epoch == command.Epoch {
		t.Fatal("retry must have a fresh command id and epoch")
	}
	if blocking := controller.Commands().Blocking("chamber-1"); len(blocking) != 0 {
		t.Fatalf("in-flight retry must clear blocking, got %d", len(blocking))
	}

	settled, err := controller.ApplyFeedback(ctx, model.DeviceFeedback{
		CommandID: retry.ID, Kind: model.FeedbackSettled,
	})
	if err != nil {
		t.Fatalf("settle retry: %v", err)
	}
	if !settled.Settled || settled.Command.State != model.CommandSettled {
		t.Fatal("retry must be able to settle")
	}
	chain := controller.Commands().Attempts(command.ID)
	if len(chain) != 2 || chain[0].State != model.CommandFailed || chain[1].State != model.CommandSettled {
		t.Fatalf("attempt chain wrong: %+v", chain)
	}
}

// Retrying an in-progress command is a safety violation and must be refused.
func TestRetryRequiresTerminalFailure(t *testing.T) {
	controller, _ := newTestController(time.Minute, time.Minute)
	ctx := context.Background()
	cycle := testCycle()
	command, err := controller.Request(ctx, cycle, model.DirectionUpstream, true)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if _, err := controller.ApplyFeedback(ctx, model.DeviceFeedback{
		CommandID: command.ID, Kind: model.FeedbackExecuting,
	}); err != nil {
		t.Fatalf("executing: %v", err)
	}
	if _, err := controller.Retry(ctx, command.ID, cycle); err == nil {
		t.Fatal("retry of an executing command must be refused")
	}
}

// The watchdog converts a deadline breach into a timed_out terminal command.
func TestWatchdogTimesOutOverdueCommand(t *testing.T) {
	// Acceptance deadline is in the past immediately after issue.
	controller, _ := newTestController(time.Nanosecond, time.Minute)
	watchdog := NewWatchdog(controller, time.Millisecond, time.Now)
	ctx := context.Background()
	command, err := controller.Request(ctx, testCycle(), model.DirectionUpstream, true)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	time.Sleep(2 * time.Millisecond)

	timedOut := watchdog.DueOnce(ctx)
	if len(timedOut) != 1 {
		t.Fatalf("want 1 timed out command, got %d", len(timedOut))
	}
	stored, _ := controller.Commands().Get(command.ID)
	if stored.State != model.CommandTimedOut || stored.Reason == "" {
		t.Fatalf("watchdog must record timed_out with a reason, got %s %q", stored.State, stored.Reason)
	}
	if pending := controller.Commands().Pending("chamber-1"); len(pending) != 0 {
		t.Fatalf("timed out command must leave pending, got %d", len(pending))
	}
	if blocking := controller.Commands().Blocking("chamber-1"); len(blocking) != 1 {
		t.Fatalf("timed out command must block pending retry, got %d", len(blocking))
	}
}

// A fast device may settle straight from queued without intermediate reports.
func TestDirectSettleFromQueued(t *testing.T) {
	controller, _ := newTestController(time.Minute, time.Minute)
	ctx := context.Background()
	command, err := controller.Request(ctx, testCycle(), model.DirectionUpstream, true)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	settled, err := controller.ApplyFeedback(ctx, model.DeviceFeedback{
		CommandID: command.ID, Kind: model.FeedbackSettled,
	})
	if err != nil {
		t.Fatalf("direct settle: %v", err)
	}
	if !settled.Settled {
		t.Fatal("direct settle must complete the command")
	}
}
