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

type harness struct {
	coordinator *Coordinator
	gates       *gate.Controller
	commands    *gate.Commands
	cycle       model.TransitCycle
}

func newHarness(t *testing.T, emergencies map[string]bool) *harness {
	t.Helper()
	store := journal.NewMemoryStore()
	recorder := journal.NewRecorder(store, time.Now)
	commands := gate.NewCommandsWithTimeouts(time.Minute, time.Minute)
	gateState := gate.NewState("chamber-1")
	gateController := gate.NewController(gateState, commands, recorder, time.Now)
	ramp := hydraulics.NewRamp(time.Millisecond, time.Now)
	hydraulicsController := hydraulics.NewController(commands, recorder, ramp, time.Now)
	coordinator := NewCoordinator(gateController, hydraulicsController, recorder, time.Now)
	if emergencies != nil {
		coordinator.BindEmergency(func(chamber string) bool { return emergencies[chamber] })
	}

	plan, err := model.NewConvoyPlan("chamber-1", model.DirectionUpstream,
		[]model.Vessel{{ID: uuid.New(), Name: "V", CallSign: "C", LengthM: 10}}, time.Now())
	if err != nil {
		t.Fatalf("plan: %v", err)
	}
	cycleValue, err := coordinator.Start(context.Background(), plan)
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	return &harness{
		coordinator: coordinator, gates: gateController,
		commands: commands, cycle: cycleValue,
	}
}

func (h *harness) issueOpen(t *testing.T) model.DeviceCommand {
	t.Helper()
	command, err := h.gates.Request(context.Background(), h.cycle, model.DirectionUpstream, true)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	return command
}

func (h *harness) feedback(t *testing.T, id uuid.UUID, kind model.FeedbackKind, reason string) gate.CommandResult {
	t.Helper()
	result, err := h.coordinator.ApplyCommandFeedback(context.Background(), model.DeviceFeedback{
		CommandID: id, Kind: kind, Reason: reason, ObservedAt: time.Now(),
	})
	if err != nil {
		t.Fatalf("feedback %s: %v", kind, err)
	}
	return result
}

func (h *harness) stage() model.TransitStage {
	stage, _ := h.coordinator.Current("chamber-1")
	return stage.Stage
}

// Received and executing must not advance the transit; only settled does.
func TestTransitWaitsUntilSettled(t *testing.T) {
	h := newHarness(t, nil)
	command := h.issueOpen(t)

	h.feedback(t, command.ID, model.FeedbackReceived, "")
	if h.stage() != model.StagePlanned {
		t.Fatalf("transit must stay planned after received, got %s", h.stage())
	}
	h.feedback(t, command.ID, model.FeedbackExecuting, "")
	if h.stage() != model.StagePlanned {
		t.Fatalf("transit must stay planned after executing, got %s", h.stage())
	}
	result := h.feedback(t, command.ID, model.FeedbackSettled, "")
	if !result.Settled {
		t.Fatal("settled feedback must report completion")
	}
	if h.stage() != model.StageAdmitting {
		t.Fatalf("transit must advance after settled, got %s", h.stage())
	}
}

// A failed command leaves the transit waiting; the linked retry carries it home.
func TestFailureHoldsTransitThenRetrySettles(t *testing.T) {
	h := newHarness(t, nil)
	command := h.issueOpen(t)

	h.feedback(t, command.ID, model.FeedbackFailed, "motor overload")
	if h.stage() != model.StagePlanned {
		t.Fatalf("failure must not advance the transit, got %s", h.stage())
	}
	if blocking := h.commands.Blocking("chamber-1"); len(blocking) != 1 {
		t.Fatalf("failed command must block, got %d", len(blocking))
	}

	retry, err := h.coordinator.RetryCommand(context.Background(), command.ID)
	if err != nil {
		t.Fatalf("retry: %v", err)
	}
	if retry.AttemptOf == nil || *retry.AttemptOf != command.ID {
		t.Fatalf("retry must reference original attempt %s", command.ID)
	}
	if h.stage() != model.StagePlanned {
		t.Fatalf("retry issue must not advance the transit, got %s", h.stage())
	}

	h.feedback(t, retry.ID, model.FeedbackReceived, "")
	h.feedback(t, retry.ID, model.FeedbackExecuting, "")
	if h.stage() != model.StagePlanned {
		t.Fatalf("retry progress must not advance the transit, got %s", h.stage())
	}
	h.feedback(t, retry.ID, model.FeedbackSettled, "")
	if h.stage() != model.StageAdmitting {
		t.Fatalf("transit must advance after retry settles, got %s", h.stage())
	}
	if blocking := h.commands.Blocking("chamber-1"); len(blocking) != 0 {
		t.Fatalf("settled retry must clear blocking, got %d", len(blocking))
	}
}

// Retries are forbidden while an emergency stop is active.
func TestRetryBlockedDuringEmergency(t *testing.T) {
	h := newHarness(t, map[string]bool{"chamber-1": true})
	command := h.issueOpen(t)
	h.feedback(t, command.ID, model.FeedbackFailed, "jam")

	if _, err := h.coordinator.RetryCommand(context.Background(), command.ID); err == nil {
		t.Fatal("retry during active emergency must be refused")
	}
}
