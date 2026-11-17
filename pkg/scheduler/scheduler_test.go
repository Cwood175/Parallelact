package scheduler

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nektos/act/pkg/common"
	"github.com/nektos/act/pkg/runner"
)

type fakeEngine struct {
	prepare func(ctx context.Context, req *RunRequest, cfg *runner.Config) (*ExecutionPlan, error)
}

func (e *fakeEngine) Prepare(ctx context.Context, req *RunRequest, cfg *runner.Config) (*ExecutionPlan, error) {
	return e.prepare(ctx, req, cfg)
}

func newTestScheduler(t *testing.T, cfg Config) (*Scheduler, string) {
	t.Helper()

	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, ".github", "workflows"), 0o755))

	if cfg.DataDir == "" {
		cfg.DataDir = t.TempDir()
	}
	if len(cfg.AllowedWorkdirRoots) == 0 {
		cfg.AllowedWorkdirRoots = []string{root}
	}
	if cfg.BaseRunnerConfig == nil {
		cfg.BaseRunnerConfig = &runner.Config{}
	}
	cfg.NoCacheServer = true
	// Unit tests have no containers, skip the docker volume cleanup.
	cfg.SharedToolCache = true

	s, err := New(cfg)
	require.NoError(t, err)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.Shutdown(ctx)
	})

	return s, root
}

func testUnit(key, jobID string, exec common.Executor) *JobUnit {
	return &JobUnit{
		Key:      key,
		Workflow: "wf",
		JobID:    jobID,
		Exec:     exec,
	}
}

func staticEngine(stages ...*ExecutionStage) Engine {
	return &fakeEngine{prepare: func(context.Context, *RunRequest, *runner.Config) (*ExecutionPlan, error) {
		return &ExecutionPlan{
			Stages:   stages,
			Finalize: func() (map[string]bool, error) { return nil, nil },
		}, nil
	}}
}

func stageWith(name string, maxParallel int, units ...*JobUnit) *ExecutionStage {
	return &ExecutionStage{
		Name: name,
		Expand: func(context.Context) ([]*JobGroup, error) {
			return []*JobGroup{{MaxParallel: maxParallel, Units: units}}, nil
		},
	}
}

func submit(t *testing.T, s *Scheduler, root string) *RunState {
	t.Helper()
	run, err := s.Submit(&RunRequest{Workdir: root, EventName: "push"})
	require.NoError(t, err)
	return run
}

func waitRun(t *testing.T, s *Scheduler, id string) *RunState {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		run, ok := s.Store().Get(id)
		require.True(t, ok)
		if run.Status.Terminal() {
			return run
		}
		time.Sleep(5 * time.Millisecond)
	}
	run, _ := s.Store().Get(id)
	t.Fatalf("run %s did not finish, state: %+v", id, run)
	return nil
}

func TestStageSequencing(t *testing.T) {
	s, root := newTestScheduler(t, Config{MaxContainers: 4})

	var finishedFirstStage atomic.Int32
	var stageTwoSawBoth atomic.Int32

	stageOne := stageWith("one", 2,
		testUnit("wf/a/0", "a", func(context.Context) error {
			time.Sleep(20 * time.Millisecond)
			finishedFirstStage.Add(1)
			return nil
		}),
		testUnit("wf/b/0", "b", func(context.Context) error {
			time.Sleep(40 * time.Millisecond)
			finishedFirstStage.Add(1)
			return nil
		}),
	)
	stageTwo := stageWith("two", 1,
		testUnit("wf/c/0", "c", func(context.Context) error {
			if finishedFirstStage.Load() == 2 {
				stageTwoSawBoth.Store(1)
			}
			return nil
		}),
	)
	s.SetEngine(staticEngine(stageOne, stageTwo))

	run := submit(t, s, root)
	finished := waitRun(t, s, run.ID)

	assert.Equal(t, StatusSuccess, finished.Status)
	assert.Equal(t, int32(1), stageTwoSawBoth.Load(), "the second stage must start after the first one finished")
}

func TestGroupMaxParallel(t *testing.T) {
	s, root := newTestScheduler(t, Config{MaxContainers: 10})

	var running, maxRunning atomic.Int32
	units := make([]*JobUnit, 0, 6)
	for i := 0; i < 6; i++ {
		i := i
		units = append(units, testUnit(fmt.Sprintf("wf/j%d/0", i), fmt.Sprintf("j%d", i), func(context.Context) error {
			current := running.Add(1)
			for {
				old := maxRunning.Load()
				if current <= old || maxRunning.CompareAndSwap(old, current) {
					break
				}
			}
			time.Sleep(20 * time.Millisecond)
			running.Add(-1)
			return nil
		}))
	}
	s.SetEngine(staticEngine(stageWith("one", 2, units...)))

	run := submit(t, s, root)
	finished := waitRun(t, s, run.ID)

	assert.Equal(t, StatusSuccess, finished.Status)
	assert.LessOrEqual(t, maxRunning.Load(), int32(2), "group max-parallel must be honoured")
	assert.Len(t, finished.Jobs, 6)
}

func TestGlobalContainerLimit(t *testing.T) {
	s, root := newTestScheduler(t, Config{MaxContainers: 3, WorkerCount: 10})

	var running, maxRunning atomic.Int32
	engine := &fakeEngine{prepare: func(_ context.Context, _ *RunRequest, cfg *runner.Config) (*ExecutionPlan, error) {
		units := make([]*JobUnit, 0, 6)
		for i := 0; i < 6; i++ {
			i := i
			units = append(units, testUnit(fmt.Sprintf("wf/j%d/0", i), fmt.Sprintf("j%d", i), func(ctx context.Context) error {
				if err := cfg.JobSlotLimiter.Acquire(ctx); err != nil {
					return err
				}
				defer cfg.JobSlotLimiter.Release()

				current := running.Add(1)
				for {
					old := maxRunning.Load()
					if current <= old || maxRunning.CompareAndSwap(old, current) {
						break
					}
				}
				time.Sleep(20 * time.Millisecond)
				running.Add(-1)
				return nil
			}))
		}
		return &ExecutionPlan{
			Stages:   []*ExecutionStage{stageWith("one", 6, units...)},
			Finalize: func() (map[string]bool, error) { return nil, nil },
		}, nil
	}}
	s.SetEngine(engine)

	run := submit(t, s, root)
	finished := waitRun(t, s, run.ID)

	assert.Equal(t, StatusSuccess, finished.Status)
	assert.LessOrEqual(t, maxRunning.Load(), int32(3), "the global container limit must be honoured")
	assert.Equal(t, int32(3), maxRunning.Load(), "the scheduler should use all available slots")
}

func TestCrossRunParallelism(t *testing.T) {
	s, root := newTestScheduler(t, Config{MaxContainers: 2, WorkerCount: 8})

	var bothStarted sync.WaitGroup
	bothStarted.Add(2)
	started := make(chan struct{}, 2)

	engine := &fakeEngine{prepare: func(context.Context, *RunRequest, *runner.Config) (*ExecutionPlan, error) {
		return &ExecutionPlan{
			Stages: []*ExecutionStage{stageWith("one", 1, testUnit("wf/a/0", "a", func(context.Context) error {
				started <- struct{}{}
				bothStarted.Done()
				waitCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				select {
				case <-started:
					// wait for the other run to be running as well
				case <-waitCtx.Done():
					return errors.New("runs did not overlap")
				}
				return nil
			}))},
			Finalize: func() (map[string]bool, error) { return nil, nil },
		}, nil
	}}
	s.SetEngine(engine)

	runA := submit(t, s, root)
	runB := submit(t, s, root)

	// Both runs must have a job running at the same time.
	done := make(chan struct{})
	go func() {
		bothStarted.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("jobs of two runs did not run concurrently")
	}

	assert.Equal(t, StatusSuccess, waitRun(t, s, runA.ID).Status)
	assert.Equal(t, StatusSuccess, waitRun(t, s, runB.ID).Status)
}

func TestCancelRun(t *testing.T) {
	s, root := newTestScheduler(t, Config{MaxContainers: 2})

	var secondStageRan atomic.Bool
	blocking := make(chan struct{})
	stageOne := stageWith("one", 1, testUnit("wf/a/0", "a", func(ctx context.Context) error {
		close(blocking)
		<-ctx.Done()
		return ctx.Err()
	}))
	stageTwo := stageWith("two", 1, testUnit("wf/b/0", "b", func(context.Context) error {
		secondStageRan.Store(true)
		return nil
	}))
	s.SetEngine(staticEngine(stageOne, stageTwo))

	run := submit(t, s, root)

	select {
	case <-blocking:
	case <-time.After(5 * time.Second):
		t.Fatal("job did not start")
	}

	require.NoError(t, s.Cancel(run.ID))
	finished := waitRun(t, s, run.ID)

	assert.Equal(t, StatusCancelled, finished.Status)
	assert.False(t, secondStageRan.Load(), "later stages must not run after a cancellation")
	require.Len(t, finished.Jobs, 1)
	assert.Equal(t, StatusCancelled, finished.Jobs[0].Status)
	assert.Equal(t, "failure", finished.Jobs[0].Result)
}

func TestRunTimeout(t *testing.T) {
	s, root := newTestScheduler(t, Config{MaxContainers: 2})
	s.SetEngine(staticEngine(stageWith("one", 1, testUnit("wf/a/0", "a", func(ctx context.Context) error {
		<-ctx.Done()
		return ctx.Err()
	}))))

	run, err := s.Submit(&RunRequest{Workdir: root, EventName: "push", TimeoutSeconds: 1})
	require.NoError(t, err)

	finished := waitRun(t, s, run.ID)
	assert.Equal(t, StatusFailure, finished.Status)
	assert.Contains(t, finished.Error, "deadline")
}

func TestJobFailureIsReported(t *testing.T) {
	s, root := newTestScheduler(t, Config{MaxContainers: 4})
	engine := &fakeEngine{prepare: func(context.Context, *RunRequest, *runner.Config) (*ExecutionPlan, error) {
		return &ExecutionPlan{
			Stages: []*ExecutionStage{stageWith("one", 2,
				testUnit("wf/good/0", "good", func(context.Context) error { return nil }),
				testUnit("wf/bad/0", "bad", func(context.Context) error { return errors.New("boom") }),
			)},
			Finalize: func() (map[string]bool, error) {
				return map[string]bool{failedJobKey("wf", "bad"): true}, errors.New("job(s) failed")
			},
		}, nil
	}}
	s.SetEngine(engine)

	run := submit(t, s, root)
	finished := waitRun(t, s, run.ID)

	assert.Equal(t, StatusFailure, finished.Status)
	statuses := map[string]Status{}
	results := map[string]string{}
	for _, job := range finished.Jobs {
		statuses[job.JobID] = job.Status
		results[job.JobID] = job.Result
	}
	assert.Equal(t, StatusSuccess, statuses["good"])
	assert.Equal(t, StatusFailure, statuses["bad"])
	assert.Equal(t, "success", results["good"])
	assert.Equal(t, "failure", results["bad"])
}

func TestAdmissionValidation(t *testing.T) {
	s, root := newTestScheduler(t, Config{MaxContainers: 1, MaxQueuedRuns: 1})
	s.SetEngine(staticEngine(stageWith("one", 1, testUnit("wf/a/0", "a", func(context.Context) error { return nil }))))

	_, err := s.Submit(&RunRequest{Workdir: "/does/not/exist"})
	assert.ErrorIs(t, err, ErrInvalidRequest)

	_, err = s.Submit(&RunRequest{Workdir: root, SecretRefs: []string{"NOPE"}})
	assert.ErrorIs(t, err, ErrInvalidRequest)

	_, err = s.Submit(&RunRequest{Workdir: root, EventJSON: []byte("not json")})
	assert.ErrorIs(t, err, ErrInvalidRequest)

	// The second concurrent run must be rejected because MaxQueuedRuns is 1.
	block := make(chan struct{})
	s.SetEngine(staticEngine(stageWith("one", 1, testUnit("wf/a/0", "a", func(ctx context.Context) error {
		select {
		case <-block:
		case <-ctx.Done():
		}
		return nil
	}))))

	first := submit(t, s, root)
	time.Sleep(20 * time.Millisecond)
	_, err = s.Submit(&RunRequest{Workdir: root})
	assert.ErrorIs(t, err, ErrQueueFull)
	close(block)
	waitRun(t, s, first.ID)
}

func TestJobLogsAreWritten(t *testing.T) {
	s, root := newTestScheduler(t, Config{MaxContainers: 1})
	s.SetEngine(staticEngine(stageWith("one", 1, testUnit("wf/a/0", "a", func(ctx context.Context) error {
		common.Logger(ctx).Infof("hello from the job")
		return nil
	}))))

	run := submit(t, s, root)
	finished := waitRun(t, s, run.ID)
	require.Equal(t, StatusSuccess, finished.Status)

	logs, err := s.logs.ListJobLogs(run.ID)
	require.NoError(t, err)
	require.Len(t, logs, 1)

	content, err := os.ReadFile(s.logs.JobLogPath(run.ID, finished.Jobs[0].Key))
	require.NoError(t, err)
	assert.Contains(t, string(content), "hello from the job")
	assert.Contains(t, string(content), "wf/a/0")
}

func TestStats(t *testing.T) {
	s, root := newTestScheduler(t, Config{MaxContainers: 2})
	s.SetEngine(staticEngine(stageWith("one", 1, testUnit("wf/a/0", "a", func(context.Context) error { return nil }))))

	run := submit(t, s, root)
	waitRun(t, s, run.ID)

	stats := s.Stats()
	assert.Equal(t, 1, stats.RunsTotal)
	assert.Equal(t, 1, stats.RunsSucceeded)
	assert.Equal(t, 2, stats.ContainersMax)
	assert.Equal(t, int64(0), stats.ContainersUsed)
}

func TestShutdownIsBoundedWhenAJobIgnoresCancellation(t *testing.T) {
	s, root := newTestScheduler(t, Config{MaxContainers: 1})

	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})

	started := make(chan struct{})
	s.SetEngine(staticEngine(stageWith("one", 1, testUnit("wf/a/0", "a", func(context.Context) error {
		close(started)
		<-release // deliberately ignores the cancellation
		return nil
	}))))

	run := submit(t, s, root)
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("job did not start")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	begin := time.Now()
	err := s.Shutdown(ctx)
	elapsed := time.Since(begin)

	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, elapsed, 10*time.Second, "Shutdown must not wait for a job that ignores cancellation")

	close(release)
	// Let the worker finish the task before the scheduler cleanup runs.
	time.Sleep(50 * time.Millisecond)
	_ = run
}
