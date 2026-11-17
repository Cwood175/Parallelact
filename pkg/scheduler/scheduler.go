package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/nektos/act/pkg/artifactcache"
	"github.com/nektos/act/pkg/artifacts"
	"github.com/nektos/act/pkg/common"
	"github.com/nektos/act/pkg/runner"
)

var (
	// ErrDraining is returned when the scheduler is shutting down.
	ErrDraining = errors.New("scheduler is shutting down")
	// ErrQueueFull is returned when too many runs are queued or running.
	ErrQueueFull = errors.New("run queue is full")
	// ErrInvalidRequest is returned for requests that fail validation.
	ErrInvalidRequest = errors.New("invalid run request")
	// ErrRunNotFound is returned for unknown runs.
	ErrRunNotFound = errors.New("run not found")
	// ErrRunNotActive is returned when cancelling a finished run.
	ErrRunNotActive = errors.New("run is not active")
)

// Scheduler accepts run requests and executes their job instances on a shared
// pool of up to MaxContainers containers.
type Scheduler struct {
	cfg    Config
	store  *Store
	engine Engine
	slots  *Semaphore
	queue  *Queue
	logs   *LogStore

	ctx    context.Context
	cancel context.CancelFunc

	seq atomic.Uint64

	mu        sync.Mutex
	handles   map[string]*runHandle
	admitted  int
	draining  bool
	workers   sync.WaitGroup
	runWG     sync.WaitGroup
	artifactC context.CancelFunc

	cleanupState
}

type runHandle struct {
	cancel    context.CancelFunc
	jobCancel context.CancelFunc
	jobCtx    context.Context
}

// New creates a scheduler. The returned scheduler must be closed with Shutdown.
func New(cfg Config) (*Scheduler, error) {
	if cfg.DataDir == "" {
		return nil, fmt.Errorf("data dir is required")
	}
	if cfg.MaxContainers < 1 {
		cfg.MaxContainers = 20
	}
	if cfg.WorkerCount < 1 {
		// More workers than containers cannot increase concurrency, and tying
		// the pool size to the host core count made it explode on big hosts.
		cfg.WorkerCount = cfg.MaxContainers + 8
	}
	if cfg.MaxQueuedRuns < 1 {
		cfg.MaxQueuedRuns = 100
	}
	if cfg.MaxUnitsPerRun < 1 {
		cfg.MaxUnitsPerRun = 200
	}
	if len(cfg.AllowedWorkdirRoots) == 0 {
		wd, err := os.Getwd()
		if err != nil {
			return nil, fmt.Errorf("failed to determine the current directory: %w", err)
		}
		cfg.AllowedWorkdirRoots = []string{wd}
	}
	if cfg.BaseRunnerConfig == nil {
		return nil, fmt.Errorf("base runner config is required")
	}
	for _, dir := range []string{cfg.DataDir, cfg.DataDir + "/runs", cfg.DataDir + "/artifacts"} {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			return nil, fmt.Errorf("failed to create %s: %w", dir, err)
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	s := &Scheduler{
		cfg:     cfg,
		store:   NewStore(),
		engine:  NewRunnerEngine(),
		slots:   NewSemaphore(cfg.MaxContainers),
		queue:   NewQueue(),
		logs:    NewLogStore(cfg.DataDir, cfg.LogStdout),
		ctx:     ctx,
		cancel:  cancel,
		handles: map[string]*runHandle{},
	}
	s.initCleanup()

	if cfg.ArtifactServerPath != "" {
		artifactC, err := artifacts.ServeE(ctx,
			cfg.ArtifactServerPath, cfg.ArtifactServerAddr, cfg.ArtifactServerPort, cfg.ArtifactServerAuthKey)
		if err != nil {
			cancel()
			return nil, err
		}
		s.artifactC = artifactC
	}

	for i := 0; i < cfg.WorkerCount; i++ {
		s.workers.Add(1)
		go s.workerLoop()
	}
	s.startCleanup()

	return s, nil
}

// SetEngine replaces the engine, used by tests.
func (s *Scheduler) SetEngine(engine Engine) {
	s.engine = engine
}

// Submit validates and admits a run request. The run itself is executed
// asynchronously.
//
//nolint:gocyclo
func (s *Scheduler) Submit(req *RunRequest) (*RunState, error) {
	if req == nil {
		return nil, fmt.Errorf("%w: empty request", ErrInvalidRequest)
	}

	workdir, err := validateWorkdir(req.Workdir, s.cfg.AllowedWorkdirRoots)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidRequest, err)
	}
	req.Workdir = workdir

	workflows, err := resolveWorkflowsPath(workdir, req.WorkflowsPath)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrInvalidRequest, err)
	}
	req.WorkflowsPath = workflows

	if len(req.EventJSON) > 0 {
		var probe map[string]any
		if err := json.Unmarshal(req.EventJSON, &probe); err != nil {
			return nil, fmt.Errorf("%w: eventJSON must be a JSON object: %s", ErrInvalidRequest, err)
		}
	}

	for name := range req.Env {
		if name == "GITHUB_RUN_ID" || name == "GITHUB_RUN_NUMBER" {
			return nil, fmt.Errorf("%w: %s is managed by the scheduler", ErrInvalidRequest, name)
		}
	}

	secrets, err := s.selectSecrets(req.SecretRefs)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		return nil, ErrDraining
	}
	if s.admitted >= s.cfg.MaxQueuedRuns {
		s.mu.Unlock()
		return nil, ErrQueueFull
	}
	s.admitted++
	s.mu.Unlock()

	run := &RunState{
		ID:          newRunID(),
		GitHubRunID: nextGitHubRunID(),
		Repo:        req.Repo,
		Commit:      req.Commit,
		Workdir:     req.Workdir,
		Event:       eventName(req),
		Status:      StatusQueued,
		CreatedAt:   time.Now(),
		Jobs:        []*JobState{},
	}
	if err := s.store.Create(run); err != nil {
		s.releaseAdmission()
		return nil, err
	}
	s.recordRunID(run.ID)

	timeout := s.cfg.RunTimeout
	if req.TimeoutSeconds > 0 {
		timeout = time.Duration(req.TimeoutSeconds) * time.Second
	}

	var runCtx context.Context
	var runCancel context.CancelFunc
	if timeout > 0 {
		runCtx, runCancel = context.WithTimeout(s.ctx, timeout)
	} else {
		runCtx, runCancel = context.WithCancel(s.ctx)
	}
	jobCtx, jobCancel := context.WithCancel(s.ctx)
	handle := &runHandle{
		cancel: func() {
			runCancel()
			jobCancel()
		},
		jobCancel: jobCancel,
		jobCtx:    jobCtx,
	}

	s.mu.Lock()
	s.handles[run.ID] = handle
	s.mu.Unlock()

	reqCopy := *req
	reqCopy.Secrets = secrets

	s.runWG.Add(1)
	go s.runRun(runCtx, handle, &reqCopy, run)

	return run.clone(), nil
}

func (s *Scheduler) selectSecrets(refs []string) (map[string]string, error) {
	if len(refs) == 0 {
		secrets := make(map[string]string, len(s.cfg.Secrets))
		for k, v := range s.cfg.Secrets {
			secrets[k] = v
		}
		return secrets, nil
	}
	secrets := make(map[string]string, len(refs))
	for _, ref := range refs {
		value, ok := s.cfg.Secrets[ref]
		if !ok {
			return nil, fmt.Errorf("%w: unknown secret reference %q", ErrInvalidRequest, ref)
		}
		secrets[ref] = value
	}
	return secrets, nil
}

func (s *Scheduler) runRun(ctx context.Context, handle *runHandle, req *RunRequest, run *RunState) {
	defer s.runWG.Done()

	ctx = common.WithJobCancelContext(ctx, handle.jobCtx)

	s.store.Update(run.ID, func(st *RunState) {
		st.Status = StatusRunning
		st.StartedAt = time.Now()
	})

	runLogger, err := s.logs.RunLogger(run.ID, nil)
	if err == nil {
		ctx = common.WithLogger(ctx, runLogger.WithFields(log.Fields{"run": run.ID}))
	}
	logger := common.Logger(ctx)

	logger.Infof("Starting run %s (workdir=%s, event=%s)", run.ID, req.Workdir, eventName(req))

	closeCache := func() {}
	cacheURL := ""
	if !s.cfg.NoCacheServer {
		var cacheErr error
		closeCache, cacheURL, cacheErr = s.startCacheServer(run.ID)
		if cacheErr != nil {
			logger.Warnf("Cache server disabled for run %s: %v", run.ID, cacheErr)
			closeCache = func() {}
			cacheURL = ""
		} else {
			defer closeCache()
		}
	}

	cfg, err := s.buildRunConfig(req, run, cacheURL)
	if err != nil {
		s.finishRun(run.ID, handle, err, nil)
		return
	}

	plan, err := s.engine.Prepare(ctx, req, cfg)
	if err != nil {
		s.finishRun(run.ID, handle, err, nil)
		return
	}

	runErr := s.executeStages(ctx, run.ID, req, plan, cfg)
	failed := map[string]bool{}
	if runErr == nil {
		var finalizeErr error
		failed, finalizeErr = plan.Finalize()
		runErr = finalizeErr
	}

	s.finishRun(run.ID, handle, runErr, failed)
}

// executeStages runs the stages of a plan sequentially and returns the first
// error. Jobs of one stage are fanned out to the global queue.
//
//nolint:gocyclo
func (s *Scheduler) executeStages(ctx context.Context, runID string, req *RunRequest, plan *ExecutionPlan, cfg *runner.Config) error {
	logger := common.Logger(ctx)
	units := 0

	for _, stage := range plan.Stages {
		if err := ctx.Err(); err != nil {
			return err
		}

		groups, err := stage.Expand(ctx)
		if err != nil {
			return fmt.Errorf("failed to expand stage %s: %w", stage.Name, err)
		}

		var wg sync.WaitGroup
		for _, group := range groups {
			maxParallel := group.MaxParallel
			if maxParallel < 1 {
				maxParallel = 1
			}
			gate := make(chan struct{}, maxParallel)

			for _, unit := range group.Units {
				units++
				if units > s.cfg.MaxUnitsPerRun {
					return fmt.Errorf("run has more than %d job instances", s.cfg.MaxUnitsPerRun)
				}
				s.store.AddJobs(runID, []*JobState{{
					Key:      unit.Key,
					Workflow: unit.Workflow,
					JobID:    unit.JobID,
					Matrix:   unit.Matrix,
					Status:   StatusQueued,
				}})

				select {
				case gate <- struct{}{}:
				case <-ctx.Done():
					s.store.SetJob(runID, unit.Key, func(job *JobState) {
						job.Status = StatusCancelled
						job.Result = "failure"
						job.FinishedAt = time.Now()
						job.Error = ctx.Err().Error()
					})
					continue
				}

				wg.Add(1)
				unit := unit
				task := &Task{
					RunID:    runID,
					Unit:     unit,
					Config:   cfg,
					Ctx:      ctx,
					Priority: req.Priority,
					Seq:      s.seq.Add(1),
					start: func() {
						s.store.SetJob(runID, unit.Key, func(job *JobState) {
							job.Status = StatusRunning
							job.StartedAt = time.Now()
						})
					},
					finish: func(err error) {
						<-gate
						s.completeJob(runID, unit, err)
						wg.Done()
					},
				}
				if !s.queue.Push(task) {
					<-gate
					wg.Done()
					s.store.SetJob(runID, unit.Key, func(job *JobState) {
						job.Status = StatusCancelled
						job.Result = "failure"
						job.FinishedAt = time.Now()
					})
				}
			}
		}

		logger.Debugf("Run %s: waiting for %d unit(s) of stage %s", runID, units, stage.Name)
		wg.Wait()

		if err := ctx.Err(); err != nil {
			return err
		}
	}

	logger.Infof("Run %s: all %d job instance(s) finished", runID, units)
	return nil
}

func (s *Scheduler) completeJob(runID string, unit *JobUnit, err error) {
	s.store.SetJob(runID, unit.Key, func(job *JobState) {
		job.FinishedAt = time.Now()
		switch {
		case err == nil:
			job.Status = StatusSuccess
			job.Result = "success"
		case errors.Is(err, context.Canceled):
			job.Status = StatusCancelled
			job.Result = "failure"
			job.Error = err.Error()
		default:
			job.Status = StatusFailure
			job.Result = "failure"
			job.Error = err.Error()
		}
	})
}

func (s *Scheduler) finishRun(runID string, handle *runHandle, runErr error, failed map[string]bool) {
	for key := range failed {
		s.store.Update(runID, func(run *RunState) {
			for _, job := range run.Jobs {
				if failedJobKey(job.Workflow, job.JobID) != key {
					continue
				}
				if job.Status != StatusQueued && job.Status != StatusRunning {
					// A matrix leg that already finished keeps its own result;
					// the group failure must not overwrite a successful leg.
					continue
				}
				job.Status = StatusFailure
				job.Result = "failure"
				if job.Error == "" {
					job.Error = "job failed"
				}
			}
		})
	}

	status := StatusSuccess
	if runErr != nil {
		status = StatusFailure
		if errors.Is(runErr, context.Canceled) {
			status = StatusCancelled
		}
	}
	s.store.Finish(runID, status, runErr)

	if run, ok := s.store.Get(runID); ok {
		runLogger, err := s.logs.RunLogger(runID, nil)
		if err == nil {
			runLogger.Infof("Run %s finished with status %s (%s)", runID, status, run.FinishedAt.Sub(run.StartedAt).Round(time.Millisecond))
		}
	}

	if handle != nil {
		handle.cancel()
	}
	// The job executor removes its own container. This is the safety net for
	// runs that were cancelled while a container was being created, or whose
	// cleanup chain was skipped (client timeout, docker error, panic).
	s.scheduleCleanup(runID)

	s.mu.Lock()
	delete(s.handles, runID)
	s.mu.Unlock()
	s.releaseAdmission()
	s.store.Prune(s.cfg.KeepRuns)
}

func (s *Scheduler) releaseAdmission() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.admitted > 0 {
		s.admitted--
	}
}

func (s *Scheduler) workerLoop() {
	defer s.workers.Done()
	for {
		task, err := s.queue.Pop(context.Background())
		if err != nil {
			return
		}
		s.runTask(task)
	}
}

func (s *Scheduler) runTask(task *Task) {
	if err := task.Ctx.Err(); err != nil {
		task.finish(err)
		return
	}
	if task.start != nil {
		task.start()
	}
	if task.Unit.JobResult != nil {
		task.resultStart = task.Unit.JobResult()
	}

	err := s.executeUnit(task)
	if err == nil && task.Unit.JobResult != nil && task.resultStart != "failure" {
		if task.Unit.JobResult() == "failure" {
			err = fmt.Errorf("job %s failed", task.Unit.JobID)
		}
	}
	task.finish(err)
}

func (s *Scheduler) executeUnit(task *Task) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("job panicked: %v", recovered)
		}
	}()

	file, openErr := s.logs.OpenJobLog(task.RunID, task.Unit.Key)
	if openErr != nil {
		return openErr
	}

	factory := &jobLogFactory{
		store:       s.logs,
		runID:       task.RunID,
		key:         task.Unit.Key,
		jobID:       task.Unit.JobID,
		file:        file,
		stdout:      s.cfg.LogStdout,
		json:        s.cfg.BaseRunnerConfig.JSONLogger,
		prefixJobID: s.cfg.BaseRunnerConfig.LogPrefixJobID,
	}

	ctx := runner.WithJobLoggerFactory(task.Ctx, factory)
	// Give scheduler level job messages (image pulls etc.) a logger that writes
	// to the same file. The runner creates its own logger for the steps.
	secretsCfg := task.Config
	if secretsCfg == nil {
		secretsCfg = s.cfg.BaseRunnerConfig
	}
	ctx = runner.WithJobLogger(ctx, task.Unit.JobID, task.Unit.Key, secretsCfg, nil, task.Unit.Matrix)
	return task.Unit.Exec(ctx)
}

// buildRunConfig clones the base config and applies the request specific
// values, including a unique run id for container names and artifacts.
func (s *Scheduler) buildRunConfig(req *RunRequest, run *RunState, cacheURL string) (*runner.Config, error) {
	cfg := s.cfg.BaseRunnerConfig.Clone()
	cfg.Workdir = req.Workdir
	cfg.EventName = eventName(req)
	cfg.EventJSON = string(req.EventJSON)
	cfg.EventPath = ""
	cfg.Secrets = req.Secrets
	cfg.RunID = run.ID
	cfg.JobSlotLimiter = s.slots
	cfg.ConcurrentJobs = s.cfg.MaxContainers
	if !s.cfg.SharedToolCache {
		cfg.ToolCacheVolume = "act-toolcache-" + run.ID
	}
	if s.cfg.PerRunActionCache {
		cfg.ActionCacheDir = runDir(s.cfg.DataDir, run.ID) + "/actions"
	}

	env := make(map[string]string, len(cfg.Env)+len(req.Env)+4)
	for k, v := range cfg.Env {
		env[k] = v
	}
	for k, v := range req.Env {
		env[k] = v
	}
	env["GITHUB_RUN_ID"] = strconv.FormatInt(run.GitHubRunID, 10)
	env["GITHUB_RUN_NUMBER"] = strconv.FormatInt(run.GitHubRunID, 10)
	if cacheURL != "" {
		env["ACTIONS_CACHE_URL"] = cacheURL
	}
	if req.Repo != "" {
		if _, ok := env["GITHUB_REPOSITORY"]; !ok {
			env["GITHUB_REPOSITORY"] = req.Repo
		}
	}
	if req.Commit != "" {
		if _, ok := env["GITHUB_SHA"]; !ok {
			env["GITHUB_SHA"] = req.Commit
		}
	}
	cfg.Env = env

	cfg.Vars = mergeStringMaps(cfg.Vars, req.Vars)
	cfg.Inputs = mergeStringMaps(cfg.Inputs, req.Inputs)

	cfg.Matrix = make(map[string]map[string]bool, len(req.Matrix))
	for key, values := range req.Matrix {
		allowed := make(map[string]bool, len(values))
		for _, value := range values {
			allowed[value] = true
		}
		cfg.Matrix[key] = allowed
	}

	if cfg.ArtifactServerPath != "" {
		cfg.ArtifactServerPath = s.cfg.ArtifactServerPath
		cfg.ArtifactServerAddr = s.cfg.ArtifactServerAddr
		cfg.ArtifactServerPort = s.cfg.ArtifactServerPort
		cfg.ArtifactServerAuthKey = s.cfg.ArtifactServerAuthKey
	}

	return cfg, nil
}

func mergeStringMaps(base, override map[string]string) map[string]string {
	merged := make(map[string]string, len(base)+len(override))
	for k, v := range base {
		merged[k] = v
	}
	for k, v := range override {
		merged[k] = v
	}
	return merged
}

// startCacheServer starts a private cache server for a run. The act cache
// storage is not namespaced by repository, so sharing one server between runs
// would leak caches across them.
func (s *Scheduler) startCacheServer(runID string) (func(), string, error) {
	dir := runDir(s.cfg.DataDir, runID) + "/cache"
	handler, err := artifactcache.StartHandler(dir, "", "", 0, common.Logger(s.ctx))
	if err != nil {
		return func() {}, "", err
	}
	return func() { _ = handler.Close() }, handler.ExternalURL() + "/", nil
}

// Cancel cancels a queued or running run.
func (s *Scheduler) Cancel(runID string) error {
	s.mu.Lock()
	handle, ok := s.handles[runID]
	s.mu.Unlock()
	if !ok {
		if _, exists := s.store.Get(runID); !exists {
			return ErrRunNotFound
		}
		return ErrRunNotActive
	}
	handle.cancel()
	s.store.Update(runID, func(run *RunState) {
		if run.Status == StatusQueued {
			run.Status = StatusCancelled
			run.FinishedAt = time.Now()
			run.Error = context.Canceled.Error()
		}
	})
	return nil
}

// Store exposes the run state store.
func (s *Scheduler) Store() *Store {
	return s.store
}

// Stats returns a snapshot of the scheduler counters.
func (s *Scheduler) Stats() Stats {
	counts := s.store.Count()
	return Stats{
		RunsTotal:      s.store.Len(),
		RunsQueued:     counts[StatusQueued],
		RunsRunning:    counts[StatusRunning],
		RunsSucceeded:  counts[StatusSuccess],
		RunsFailed:     counts[StatusFailure],
		RunsCancelled:  counts[StatusCancelled],
		JobsQueued:     s.store.JobsQueued(),
		JobsRunning:    s.store.JobsRunning(),
		ContainersUsed: s.slots.InUse(),
		ContainersMax:  s.slots.Capacity(),
		QueueLength:    s.queue.Len(),
	}
}

// Shutdown stops accepting runs, cancels the active ones and waits for them.
func (s *Scheduler) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.draining {
		s.mu.Unlock()
		return nil
	}
	s.draining = true
	s.mu.Unlock()

	s.cancel()

	done := make(chan struct{})
	go func() {
		s.runWG.Wait()
		close(done)
	}()

	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
	}

	s.queue.Close()
	workersDone := make(chan struct{})
	go func() {
		s.workers.Wait()
		close(workersDone)
	}()
	select {
	case <-workersDone:
	case <-ctx.Done():
		if err == nil {
			err = ctx.Err()
		}
		// Jobs may still be blocked in a docker operation that ignores the
		// cancellation. Leave them alone but make sure their containers do not
		// outlive the daemon.
		s.cleanupActiveRuns()
	}

	s.stopCleanup()
	if s.artifactC != nil {
		s.artifactC()
	}
	if closeErr := s.logs.Close(); closeErr != nil && err == nil {
		err = closeErr
	}
	return err
}
