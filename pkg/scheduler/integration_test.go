//go:build !(WITHOUT_DOCKER || !(linux || darwin || windows || netbsd))

package scheduler

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nektos/act/pkg/runner"
)

const integrationImage = "node:22-slim"

// TestSchedulerIntegration runs three concurrent requests of the same workdir
// against the real Docker daemon and checks the isolation and the container cap.
func TestSchedulerIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping docker integration test in short mode")
	}
	dockerAvailable(t)
	imageAvailable(t, integrationImage)

	workdir := t.TempDir()
	workflowDir := filepath.Join(workdir, ".github", "workflows")
	require.NoError(t, os.MkdirAll(workflowDir, 0o755))
	workflow, err := os.ReadFile(filepath.Join("testdata", "concurrent", "workflow.yml"))
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(filepath.Join(workflowDir, "ci.yml"), workflow, 0o644))

	baseConfig := &runner.Config{
		Platforms:      map[string]string{"ubuntu-latest": integrationImage},
		ForcePull:      false,
		AutoRemove:     true,
		ActionCacheDir: t.TempDir(),
		LogOutput:      false,
	}

	s, err := New(Config{
		DataDir:             t.TempDir(),
		MaxContainers:       2,
		MaxQueuedRuns:       10,
		MaxUnitsPerRun:      50,
		AllowedWorkdirRoots: []string{workdir},
		BaseRunnerConfig:    baseConfig,
		NoCacheServer:       true,
		RunTimeout:          10 * time.Minute,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		_ = s.Shutdown(ctx)
	})

	runs := make([]*RunState, 0, 3)
	for i := 0; i < 3; i++ {
		run, err := s.Submit(&RunRequest{
			Workdir:   workdir,
			Repo:      "example/concurrent",
			Commit:    "deadbeef",
			EventName: "push",
		})
		require.NoError(t, err)
		runs = append(runs, run)
	}

	// Watch how many containers of these runs run at the same time. Containers
	// of other act processes on the machine are ignored.
	prefixes := make([]string, 0, len(runs))
	for _, run := range runs {
		prefixes = append(prefixes, "act-"+run.ID)
	}
	var maxRunning atomic.Int32
	stopWatch := make(chan struct{})
	var watchWG sync.WaitGroup
	watchWG.Add(1)
	go func() {
		defer watchWG.Done()
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopWatch:
				return
			case <-ticker.C:
				out, err := exec.Command("docker", "ps", "--format", "{{.Names}}").Output()
				if err != nil {
					continue
				}
				count := int32(0)
				for _, name := range strings.Split(strings.TrimSpace(string(out)), "\n") {
					for _, prefix := range prefixes {
						if strings.HasPrefix(name, prefix) {
							count++
							break
						}
					}
				}
				for {
					old := maxRunning.Load()
					if count <= old || maxRunning.CompareAndSwap(old, count) {
						break
					}
				}
			}
		}
	}()

	for _, run := range runs {
		finished := waitRun(t, s, run.ID)
		assert.Equalf(t, StatusSuccess, finished.Status, "run %s failed: %s", run.ID, finished.Error)
		// first, second and three matrix instances
		assert.Len(t, finished.Jobs, 5)
	}

	close(stopWatch)
	watchWG.Wait()

	assert.LessOrEqual(t, maxRunning.Load(), int32(2), "more containers than --max-containers ran at the same time")
	assert.GreaterOrEqual(t, maxRunning.Load(), int32(1), "the watcher did not observe any container")

	// Every run must have used its own container names and must have cleaned up.
	for _, run := range runs {
		out, err := exec.Command("docker", "ps", "-a", "--filter", "name=act-"+run.ID, "--format", "{{.Names}}").Output()
		require.NoError(t, err)
		assert.Empty(t, strings.TrimSpace(string(out)), "run %s left containers behind", run.ID)
	}
}

func dockerAvailable(t *testing.T) {
	t.Helper()
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker is not installed")
	}
	if err := exec.Command("docker", "info", "--format", "{{.ServerVersion}}").Run(); err != nil {
		t.Skip("docker daemon is not reachable")
	}
}

func imageAvailable(t *testing.T, image string) {
	t.Helper()
	if err := exec.Command("docker", "image", "inspect", image).Run(); err != nil {
		t.Skipf("image %s is not available locally", image)
	}
}
