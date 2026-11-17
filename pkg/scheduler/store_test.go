package scheduler

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStoreIsolatesReturnedState(t *testing.T) {
	store := NewStore()
	run := &RunState{ID: "a", Status: StatusQueued, Jobs: []*JobState{{Key: "j", Status: StatusQueued}}}
	require.NoError(t, store.Create(run))

	got, ok := store.Get("a")
	require.True(t, ok)
	got.Status = StatusSuccess
	got.Jobs[0].Status = StatusSuccess

	again, ok := store.Get("a")
	require.True(t, ok)
	assert.Equal(t, StatusQueued, again.Status)
	assert.Equal(t, StatusQueued, again.Jobs[0].Status)
}

func TestStoreConcurrentUpdates(t *testing.T) {
	store := NewStore()
	require.NoError(t, store.Create(&RunState{
		ID:   "a",
		Jobs: []*JobState{{Key: "j0"}, {Key: "j1"}, {Key: "j2"}, {Key: "j3"}},
	}))

	var wg sync.WaitGroup
	for _, key := range []string{"j0", "j1", "j2", "j3"} {
		key := key
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 50; i++ {
				store.SetJob("a", key, func(job *JobState) { job.Status = StatusRunning })
				_ = store.List(0)
				_ = store.JobsRunning()
			}
		}()
	}
	wg.Wait()

	run, ok := store.Get("a")
	require.True(t, ok)
	for _, job := range run.Jobs {
		assert.Equal(t, StatusRunning, job.Status)
	}
}

func TestStorePruneKeepsActiveRuns(t *testing.T) {
	store := NewStore()
	for _, id := range []string{"done1", "active", "done2"} {
		require.NoError(t, store.Create(&RunState{ID: id}))
	}
	store.Finish("done1", StatusSuccess, nil)
	store.Finish("done2", StatusSuccess, nil)

	store.Prune(2)

	assert.Equal(t, 2, store.Len())
	_, ok := store.Get("active")
	assert.True(t, ok, "active run must never be pruned")
}

func TestValidateWorkdir(t *testing.T) {
	root := t.TempDir()
	outside := t.TempDir()
	nested := filepath.Join(root, "repo")
	require.NoError(t, os.MkdirAll(nested, 0o755))

	resolved, err := validateWorkdir(nested, []string{root})
	require.NoError(t, err)
	assert.Equal(t, nested, resolved)

	_, err = validateWorkdir(nested, []string{outside})
	assert.Error(t, err, "workdir outside the allowed roots must be rejected")

	_, err = validateWorkdir("relative/path", []string{root})
	assert.Error(t, err)

	_, err = validateWorkdir(filepath.Join(root, "missing"), []string{root})
	assert.Error(t, err)
}

func TestResolveWorkflowsPath(t *testing.T) {
	root := t.TempDir()
	wf := filepath.Join(root, ".github", "workflows")
	require.NoError(t, os.MkdirAll(wf, 0o755))

	resolved, err := resolveWorkflowsPath(root, "")
	require.NoError(t, err)
	assert.Equal(t, wf, resolved)

	_, err = resolveWorkflowsPath(root, "../outside")
	assert.Error(t, err)
}
