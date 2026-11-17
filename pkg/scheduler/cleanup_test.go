package scheduler

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nektos/act/pkg/runner"
)

func newCleanupTestScheduler(t *testing.T, interval, grace time.Duration) *Scheduler {
	t.Helper()
	s, err := New(Config{
		DataDir:           t.TempDir(),
		MaxContainers:     1,
		WorkerCount:       1,
		CleanupInterval:   interval,
		CleanupGrace:      grace,
		CleanupRetryDelay: 5 * time.Millisecond,
		BaseRunnerConfig:  &runner.Config{},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(shutdownCtx)
	})
	return s
}

type cleanupRecorder struct {
	mu    sync.Mutex
	calls [][]string
}

func (r *cleanupRecorder) record(runIDs []string) map[string]bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	copied := append([]string(nil), runIDs...)
	r.calls = append(r.calls, copied)
	result := make(map[string]bool, len(runIDs))
	for _, id := range runIDs {
		result[id] = true
	}
	return result
}

func (r *cleanupRecorder) recorded() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var ids []string
	for _, call := range r.calls {
		ids = append(ids, call...)
	}
	return ids
}

func TestScheduledCleanupRunsAfterGrace(t *testing.T) {
	s := newCleanupTestScheduler(t, 5*time.Millisecond, time.Millisecond)
	recorder := &cleanupRecorder{}
	s.setCleanupResources(recorder.record)

	s.scheduleCleanupAfter("r20260101T000000deadbeef", 0)

	require.Eventually(t, func() bool {
		return len(recorder.recorded()) == 1
	}, 2*time.Second, 5*time.Millisecond)
	assert.Equal(t, []string{"r20260101T000000deadbeef"}, recorder.recorded())
	assert.Eventually(t, func() bool { return s.pendingCleanups() == 0 }, time.Second, 5*time.Millisecond)
}

func TestCleanupIsDeduplicatedAndRetried(t *testing.T) {
	s := newCleanupTestScheduler(t, 5*time.Millisecond, time.Millisecond)

	var mu sync.Mutex
	attempts := 0
	s.setCleanupResources(func(runIDs []string) map[string]bool {
		mu.Lock()
		defer mu.Unlock()
		attempts++
		result := map[string]bool{}
		for _, id := range runIDs {
			// Fail the first attempt, as docker does when a container is still
			// attached to the network or the daemon is busy.
			result[id] = attempts > 1
		}
		return result
	})

	s.scheduleCleanupAfter("rTEST", 0)
	s.scheduleCleanupAfter("rTEST", 0) // must not create a second timer

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts >= 2
	}, 5*time.Second, 10*time.Millisecond)
	assert.Eventually(t, func() bool { return s.pendingCleanups() == 0 }, 2*time.Second, 10*time.Millisecond)
}

func TestPreviousRunDirectoriesAreSweptAtStartup(t *testing.T) {
	dir := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "runs", "rOLD1"), 0o700))
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "runs", "rOLD2"), 0o700))

	s, err := New(Config{
		DataDir:           dir,
		MaxContainers:     1,
		WorkerCount:       1,
		CleanupInterval:   5 * time.Millisecond,
		CleanupGrace:      time.Millisecond,
		CleanupRetryDelay: 5 * time.Millisecond,
		BaseRunnerConfig:  &runner.Config{},
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})

	recorder := &cleanupRecorder{}
	s.setCleanupResources(recorder.record)

	require.Eventually(t, func() bool {
		seen := map[string]bool{}
		for _, id := range recorder.recorded() {
			seen[id] = true
		}
		return seen["rOLD1"] && seen["rOLD2"]
	}, 3*time.Second, 10*time.Millisecond)
}

func TestRemoveRunResourcesSkipsReusedContainers(t *testing.T) {
	s := newCleanupTestScheduler(t, time.Hour, time.Hour)
	s.cfg.BaseRunnerConfig.ReuseContainers = true

	cleaned := s.removeRunResources([]string{"rTEST"})
	assert.Equal(t, map[string]bool{"rTEST": true}, cleaned)
}
