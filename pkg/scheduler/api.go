package scheduler

import (
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

// Handler returns the HTTP handler of the API.
func (s *Scheduler) Handler(apiToken string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /v1/runs", s.handleSubmit)
	mux.HandleFunc("GET /v1/runs", s.handleListRuns)
	mux.HandleFunc("GET /v1/runs/{id}", s.handleGetRun)
	mux.HandleFunc("GET /v1/runs/{id}/logs", s.handleLogs)
	mux.HandleFunc("POST /v1/runs/{id}/cancel", s.handleCancel)
	mux.HandleFunc("GET /v1/stats", s.handleStats)
	mux.HandleFunc("GET /healthz", s.handleHealth)

	return authenticate(mux, apiToken)
}

func authenticate(next http.Handler, token string) http.Handler {
	if token == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/healthz" {
			next.ServeHTTP(w, r)
			return
		}
		supplied, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
		if !ok || subtle.ConstantTimeCompare([]byte(supplied), []byte(token)) != 1 {
			writeError(w, http.StatusUnauthorized, "missing or invalid API token")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Scheduler) handleSubmit(w http.ResponseWriter, r *http.Request) {
	body := http.MaxBytesReader(w, r.Body, 1<<20)
	defer body.Close()

	var req RunRequest
	decoder := json.NewDecoder(body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("invalid JSON body: %v", err))
		return
	}

	run, err := s.Submit(&req)
	if err != nil {
		writeSchedulerError(w, err)
		return
	}
	writeJSON(w, http.StatusAccepted, run)
}

func (s *Scheduler) handleListRuns(w http.ResponseWriter, r *http.Request) {
	query := r.URL.Query()
	limit := 100
	if raw := query.Get("limit"); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 {
			limit = parsed
		}
	}

	runs := s.store.List(0)
	filtered := make([]*RunState, 0, len(runs))
	for _, run := range runs {
		if status := query.Get("status"); status != "" && string(run.Status) != status {
			continue
		}
		if repo := query.Get("repo"); repo != "" && run.Repo != repo {
			continue
		}
		if commit := query.Get("commit"); commit != "" && run.Commit != commit {
			continue
		}
		filtered = append(filtered, run)
		if len(filtered) >= limit {
			break
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": filtered})
}

func (s *Scheduler) handleGetRun(w http.ResponseWriter, r *http.Request) {
	run, ok := s.store.Get(r.PathValue("id"))
	if !ok {
		writeError(w, http.StatusNotFound, ErrRunNotFound.Error())
		return
	}
	writeJSON(w, http.StatusOK, run)
}

func (s *Scheduler) handleCancel(w http.ResponseWriter, r *http.Request) {
	err := s.Cancel(r.PathValue("id"))
	if err != nil {
		writeSchedulerError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "cancelling"})
}

func (s *Scheduler) handleStats(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.Stats())
}

func (s *Scheduler) handleHealth(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	draining := s.draining
	s.mu.Unlock()
	if draining {
		writeError(w, http.StatusServiceUnavailable, ErrDraining.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Scheduler) handleLogs(w http.ResponseWriter, r *http.Request) {
	runID := r.PathValue("id")
	if _, ok := s.store.Get(runID); !ok {
		writeError(w, http.StatusNotFound, ErrRunNotFound.Error())
		return
	}

	query := r.URL.Query()
	key := query.Get("job")
	if key == "" {
		logs, err := s.logs.ListJobLogs(runID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"run": runID, "logs": logs})
		return
	}

	// The key is sanitized when building the path, so it can never escape the
	// run directory.
	path := s.logs.JobLogPath(runID, key)
	offset := int64(0)
	if raw := query.Get("offset"); raw != "" {
		if parsed, err := strconv.ParseInt(raw, 10, 64); err == nil && parsed >= 0 {
			offset = parsed
		}
	}
	follow := query.Get("follow") == "1" || query.Get("follow") == "true"
	if err := ServeLog(w, r, path, offset, follow, s.store, runID); err != nil {
		// The headers are already written when streaming, so only report errors
		// that happen before the first byte.
		if errors.Is(err, errRunNotFound) {
			writeError(w, http.StatusNotFound, err.Error())
			return
		}
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func writeSchedulerError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrInvalidRequest):
		writeError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, ErrQueueFull), errors.Is(err, ErrDraining):
		writeError(w, http.StatusServiceUnavailable, err.Error())
	case errors.Is(err, ErrRunNotFound):
		writeError(w, http.StatusNotFound, err.Error())
	case errors.Is(err, ErrRunNotActive):
		writeError(w, http.StatusConflict, err.Error())
	default:
		writeError(w, http.StatusInternalServerError, err.Error())
	}
}

func writeError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, code int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(payload)
}
