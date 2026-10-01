package cycle

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/gate"
	"github.com/wyw14/cry-96/internal/hydraulics"
	"github.com/wyw14/cry-96/internal/journal"
	"github.com/wyw14/cry-96/internal/model"
)

// After replay, failed commands, reasons and retry chains must be rebuilt.
func TestRecoveryRebuildsCommands(t *testing.T) {
	ctx := context.Background()
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, time.Now)

	build := func() (*Coordinator, *gate.Commands, *gate.Controller) {
		commands := gate.NewCommandsWithTimeouts(time.Minute, time.Minute)
		gateState := gate.NewState("chamber-1")
		gateController := gate.NewController(gateState, commands, recorder, time.Now)
		ramp := hydraulics.NewRamp(time.Millisecond, time.Now)
		hydraulicsController := hydraulics.NewController(commands, recorder, ramp, time.Now)
		coordinator := NewCoordinator(gateController, hydraulicsController, recorder, time.Now)
		replayer := journal.NewReplayer(store)
		recovery := NewRecovery(replayer, coordinator)
		if _, err := recovery.RestoreAll(ctx); err != nil {
			t.Fatalf("restore: %v", err)
		}
		return coordinator, commands, gateController
	}

	coordinator, _, _ := build()
	plan := model.ConvoyPlan{
		ID: uuid.New(), ChamberID: "chamber-1", Direction: model.DirectionUpstream,
		Vessels: []model.Vessel{{ID: uuid.New(), Name: "V", CallSign: "C", LengthM: 10}},
	}
	cycle, err := coordinator.Start(ctx, plan)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	gateController := coordinator.gates
	command, err := gateController.Request(ctx, cycle, model.DirectionUpstream, true)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if _, err := coordinator.ApplyCommandFeedback(ctx, model.DeviceFeedback{
		CommandID: command.ID, Kind: model.FeedbackReceived, ObservedAt: time.Now(),
	}); err != nil {
		t.Fatalf("received: %v", err)
	}
	if _, err := coordinator.ApplyCommandFeedback(ctx, model.DeviceFeedback{
		CommandID: command.ID, Kind: model.FeedbackFailed, Reason: "line break", ObservedAt: time.Now(),
	}); err != nil {
		t.Fatalf("failed: %v", err)
	}
	retry, err := coordinator.RetryCommand(ctx, command.ID)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}

	// Rebuild all in-memory component state purely from the journal.
	_, restoredCommands, _ := build()

	rebuilt, ok := restoredCommands.Get(command.ID)
	if !ok {
		t.Fatal("original command not restored")
	}
	if rebuilt.State != model.CommandFailed || rebuilt.Reason != "line break" {
		t.Fatalf("restored failure wrong: state=%s reason=%q", rebuilt.State, rebuilt.Reason)
	}
	if rebuilt.AcceptedAt == nil {
		t.Fatal("restored command lost its accepted timestamp")
	}
	rebuiltRetry, ok := restoredCommands.Get(retry.ID)
	if !ok {
		t.Fatal("retry command not restored")
	}
	if rebuiltRetry.Attempt != 2 || rebuiltRetry.AttemptOf == nil || *rebuiltRetry.AttemptOf != command.ID {
		t.Fatalf("restored retry chain wrong: attempt=%d of=%v", rebuiltRetry.Attempt, rebuiltRetry.AttemptOf)
	}
	if blocking := restoredCommands.Blocking("chamber-1"); len(blocking) != 0 {
		t.Fatalf("in-flight restored retry should clear blocking, got %d", len(blocking))
	}
	if pending := restoredCommands.Pending("chamber-1"); len(pending) != 1 {
		t.Fatalf("restored retry must remain pending, got %d", len(pending))
	}
	if len(restoredCommands.Attempts(command.ID)) != 2 {
		t.Fatal("restored attempt chain must contain both commands")
	}
}
