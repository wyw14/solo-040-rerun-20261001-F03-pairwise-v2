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
		CreatedAt: time.Now(), UpdatedAt: time.Now(),
	}
}

func TestCommandLifecycleProgression(t *testing.T) {
	commands := NewCommands()
	cycle := testCycle()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	command, err := commands.Issue(cycle, model.DeviceUpstreamGate, "open", 0, base)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if command.State != model.CommandQueued || command.Attempt != 1 || command.RootID != command.ID {
		t.Fatalf("unexpected initial command: %+v", command)
	}
	if command.Deadline == nil || !command.Deadline.Equal(base.Add(DefaultAcceptTimeout)) {
		t.Fatalf("queued command should carry an accept deadline")
	}

	if _, err := commands.Execute(command.ID, base); err == nil {
		t.Fatal("queued -> executing must be rejected")
	}
	accepted, err := commands.Accept(command.ID, base)
	if err != nil {
		t.Fatalf("accept: %v", err)
	}
	if !accepted.Deadline.Equal(base.Add(DefaultExecuteTimeout)) {
		t.Fatal("accept should (re)start the execution deadline")
	}
	if _, err := commands.Accept(command.ID, base); err == nil {
		t.Fatal("duplicate accept must be rejected")
	}
	if _, err := commands.Execute(command.ID, base.Add(time.Second)); err != nil {
		t.Fatalf("execute: %v", err)
	}

	ack := model.DeviceAck{
		CommandID: command.ID, CycleID: command.CycleID, ChamberID: command.ChamberID,
		Generation: command.Generation, Epoch: command.Epoch, Device: command.Device,
		Settled: true, ObservedAt: base.Add(2 * time.Second),
	}
	settled, err := commands.Settle(ack, base.Add(2*time.Second))
	if err != nil {
		t.Fatalf("settle: %v", err)
	}
	if len(settled.Timeline) != 4 {
		t.Fatalf("timeline should record queued/accepted/executing/settled, got %d entries", len(settled.Timeline))
	}
	if _, err := commands.Execute(command.ID, base.Add(3*time.Second)); err == nil {
		t.Fatal("feedback after terminal settle must be rejected")
	}
}

func TestFailureAndRetryChain(t *testing.T) {
	commands := NewCommands()
	cycle := testCycle()
	base := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)

	first, err := commands.Issue(cycle, model.DeviceUpstreamGate, "close", 0, base)
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	if _, err := commands.Fail(first.ID, "JAM", "", base); err == nil {
		t.Fatal("failure requires an operator-visible reason")
	}
	failed, err := commands.Fail(first.ID, "JAM", "gate stalled", base.Add(time.Second))
	if err != nil {
		t.Fatalf("fail: %v", err)
	}
	if failed.State != model.CommandFailed || failed.FailureReason != "gate stalled" {
		t.Fatalf("unexpected failed command: %+v", failed)
	}

	retry, err := commands.Retry(first.ID, cycle, base.Add(2*time.Second))
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry.Attempt != 2 || retry.RetryOf != first.ID || retry.RootID != first.RootID {
		t.Fatalf("retry chain identity wrong: %+v", retry)
	}
	if retry.State != model.CommandQueued {
		t.Fatalf("retry must start queued, got %s", retry.State)
	}
	// The failed attempt is preserved terminal.
	if got, _ := commands.Get(first.ID); got.State != model.CommandFailed {
		t.Fatalf("original attempt must stay failed, got %s", got.State)
	}
}

func TestWatchdogTimesOutOverdue(t *testing.T) {
	recorder := journal.NewRecorder(journal.NewMemoryStore(), time.Now)
	controller := NewController(NewState("chamber-1"), NewCommands(), recorder, time.Now)
	cycle := testCycle()
	cycle.ChamberID = "chamber-1"

	// A supervised gate command that is never accepted times out at accept.
	command, err := controller.commands.Issue(cycle, model.DeviceUpstreamGate, "open", 0, time.Now().Add(-DefaultAcceptTimeout-time.Second))
	if err != nil {
		t.Fatalf("issue: %v", err)
	}
	timedOut := controller.TimeoutExpired(context.Background())
	if len(timedOut) != 1 || timedOut[0].ID != command.ID {
		t.Fatalf("expected exactly the overdue command to time out, got %+v", timedOut)
	}
	got, _ := controller.commands.Get(command.ID)
	if got.State != model.CommandTimedOut || got.FailureCode != "TIMEOUT" || got.FailureReason == "" {
		t.Fatalf("expected timed_out with reason, got %+v", got)
	}
	// A second sweep must not re-time a terminal command.
	if again := controller.TimeoutExpired(context.Background()); len(again) != 0 {
		t.Fatalf("terminal command timed out twice: %+v", again)
	}

	// An unsupervised valve setpoint has no deadline and must never time out.
	valve, err := controller.commands.IssueUnsupervised(cycle, model.DeviceFillValve, "set_opening", 0.5, time.Now())
	if err != nil {
		t.Fatalf("issue valve: %v", err)
	}
	if valve.Deadline != nil {
		t.Fatal("unsupervised command must not carry a deadline")
	}
	if overdue := controller.commands.Overdue(time.Now().Add(time.Hour)); len(overdue) != 0 {
		t.Fatalf("unsupervised valve must not be overdue: %+v", overdue)
	}
}
