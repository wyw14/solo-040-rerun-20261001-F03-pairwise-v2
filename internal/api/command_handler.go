package api

import (
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/wyw14/cry-96/internal/model"
)

type feedbackInput struct {
	Kind       model.FeedbackKind `json:"kind"`
	Reason     string             `json:"reason,omitempty"`
	ObservedAt time.Time          `json:"observed_at,omitempty"`
}

// commandFeedbackHandler accepts real device progress: received, executing,
// failed or settled. Receipt and execution never complete the transit.
func commandFeedbackHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		commandID, err := uuid.Parse(chi.URLParam(r, "commandID"))
		if err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		var input feedbackInput
		if err := decodeJSON(r, &input); err != nil {
			writeError(w, http.StatusBadRequest, err)
			return
		}
		if !input.Kind.Valid() {
			writeError(w, http.StatusBadRequest, errors.New("kind must be one of received, executing, failed, settled"))
			return
		}
		if input.Kind == model.FeedbackFailed && input.Reason == "" {
			writeError(w, http.StatusBadRequest, errors.New("failed feedback requires a reason"))
			return
		}
		command, ok := runtime.GateCommands.Get(commandID)
		if !ok || command.ChamberID != chamberID {
			writeError(w, http.StatusNotFound, errors.New("command not found"))
			return
		}
		feedback := model.DeviceFeedback{
			CommandID: commandID, Kind: input.Kind,
			Reason: input.Reason, ObservedAt: input.ObservedAt,
		}
		if feedback.ObservedAt.IsZero() {
			feedback.ObservedAt = runtime.Now().UTC()
		}
		result, err := runtime.Cycles.ApplyCommandFeedback(r.Context(), feedback)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	}
}

// retryCommandHandler re-issues a failed or timed-out command under the
// current transit generation, linking the new attempt to the original.
func retryCommandHandler(runtime *Runtime) http.HandlerFunc {
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
		retry, err := runtime.Cycles.RetryCommand(r.Context(), commandID)
		if err != nil {
			writeError(w, http.StatusConflict, err)
			return
		}
		writeJSON(w, http.StatusCreated, retry)
	}
}

type attemptSummary struct {
	ID        uuid.UUID          `json:"id"`
	Attempt   int                `json:"attempt"`
	State     model.CommandState `json:"state"`
	Reason    string             `json:"reason,omitempty"`
	UpdatedAt time.Time          `json:"updated_at"`
}

type commandView struct {
	model.DeviceCommand
	Timeline []model.CommandTimelineEntry `json:"timeline"`
	Attempts []attemptSummary             `json:"attempts"`
}

// commandDetailHandler returns one command with its timeline and retry chain.
func commandDetailHandler(runtime *Runtime) http.HandlerFunc {
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
		chain := runtime.GateCommands.Attempts(command.OriginalID())
		summary := make([]attemptSummary, 0, len(chain))
		for _, item := range chain {
			summary = append(summary, attemptSummary{
				ID: item.ID, Attempt: item.Attempt, State: item.State,
				Reason: item.Reason, UpdatedAt: item.UpdatedAt,
			})
		}
		writeJSON(w, http.StatusOK, commandView{
			DeviceCommand: command, Timeline: command.Timeline(), Attempts: summary,
		})
	}
}

// commandsHandler returns the chamber command timeline, including failed and
// retried attempts so operators see true progress rather than only the result.
func commandsHandler(runtime *Runtime) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		chamberID := chi.URLParam(r, "chamberID")
		history := runtime.GateCommands.History(chamberID)
		views := make([]commandView, 0, len(history))
		for _, command := range history {
			chain := runtime.GateCommands.Attempts(command.OriginalID())
			summary := make([]attemptSummary, 0, len(chain))
			for _, item := range chain {
				summary = append(summary, attemptSummary{
					ID: item.ID, Attempt: item.Attempt, State: item.State,
					Reason: item.Reason, UpdatedAt: item.UpdatedAt,
				})
			}
			views = append(views, commandView{
				DeviceCommand: command, Timeline: command.Timeline(), Attempts: summary,
			})
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"chamber_id": chamberID,
			"commands":   views,
			"pending":    runtime.GateCommands.Pending(chamberID),
			"blocking":   runtime.GateCommands.Blocking(chamberID),
		})
	}
}
