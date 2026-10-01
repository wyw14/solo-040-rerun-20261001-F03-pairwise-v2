package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

func openGateHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		direction := model.Direction(chi.URLParam(r, "direction"))
		if !direction.Valid() {
			writeError(w, http.StatusBadRequest, errors.New("invalid gate direction"))
			return
		}
		command, err := runtime.Interlocks.RequestOpen(r.Context(), mustCurrent(runtime, chamberID), direction, "http:"+requestID(r.Context()))
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusAccepted, command)
	}
}

func mustCurrent(runtime *Runtime, chamberID string) model.TransitCycle {
	cycleValue, _ := runtime.Cycles.Current(chamberID)
	return cycleValue
}

type feedbackInput struct {
	Phase      model.CommandState `json:"phase"`
	Code       string             `json:"code,omitempty"`
	Reason     string             `json:"reason,omitempty"`
	ObservedAt time.Time          `json:"observed_at,omitempty"`
}

// commandFeedbackHandler receives a device report about one command. A
// received (accepted) or executing report is progress, not completion; only a
// settled report advances the transit. A failed report must carry the reason
// the operator needs to decide on a safe resend.
func commandFeedbackHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		commandID, err := uuid.Parse(chi.URLParam(r, "commandID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		command, ok := runtime.GateCommands.Get(commandID)
		if !ok || command.ChamberID != chamberID {
			writeError(w, http.StatusNotFound, errors.New("command not found"))
			return
		}
		var input feedbackInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		switch input.Phase {
		case model.CommandAccepted, model.CommandExecuting, model.CommandSettled:
		case model.CommandFailed:
			if input.Reason == "" {
				writeError(w, http.StatusBadRequest, errors.New("failed feedback requires a reason"))
				return
			}
		default:
			writeError(w, http.StatusBadRequest, errors.New("phase must be accepted, executing, settled or failed"))
			return
		}
		feedback := model.DeviceFeedback{
			CommandID: commandID, Phase: input.Phase, Code: input.Code,
			Reason: input.Reason, ObservedAt: input.ObservedAt,
		}
		if feedback.ObservedAt.IsZero() {
			feedback.ObservedAt = runtime.Now().UTC()
		}
		updated, err := runtime.Cycles.ApplyCommandFeedback(r.Context(), feedback)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, updated)
	}
}

// commandRetryHandler safely resends a failed/timed-out command as a new
// attempt linked to the original. Safety rules are enforced by the interlock
// coordinator. The transit stays in its current stage.
func commandRetryHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		commandID, err := uuid.Parse(chi.URLParam(r, "commandID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if _, ok := runtime.GateCommands.Get(commandID); !ok {
			writeError(w, http.StatusNotFound, errors.New("command not found"))
			return
		}
		cycleValue, ok := runtime.Cycles.Current(chamberID)
		if !ok {
			writeError(w, http.StatusConflict, errors.New("active transit not found"))
			return
		}
		command, err := runtime.Interlocks.RetryCommand(r.Context(), cycleValue, commandID)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusCreated, command)
	}
}

// commandsHandler returns the full command timeline for a chamber, including
// terminal and retried attempts, newest last.
func commandsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		writeJSON(w, http.StatusOK, map[string]any{
			"chamber_id": chamberID,
			"commands":   runtime.GateCommands.List(chamberID),
		})
	}
}

func emergencyHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		record, err := runtime.Safety.EmergencyStop(r.Context(), chi.URLParam(r, "chamberID"))
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, record)
	}
}
