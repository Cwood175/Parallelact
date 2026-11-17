package scheduler

import (
	"encoding/json"
	"time"

	"github.com/nektos/act/pkg/runner"
)

// Status is the state of a run or of one of its jobs.
type Status string

const (
	StatusQueued    Status = "queued"
	StatusRunning   Status = "running"
	StatusSuccess   Status = "success"
	StatusFailure   Status = "failure"
	StatusCancelled Status = "cancelled"
	StatusSkipped   Status = "skipped"
)

// Terminal reports whether the status will not change anymore.
func (s Status) Terminal() bool {
	switch s {
	case StatusSuccess, StatusFailure, StatusCancelled, StatusSkipped:
		return true
	}
	return false
}

// RunRequest is one unit of work accepted by the scheduler. The code to run is
// taken from Workdir, which must be an existing checkout on the scheduler host.
type RunRequest struct {
	// Repo and Commit are metadata only: the scheduler never clones anything.
	Repo   string `json:"repo,omitempty"`
	Commit string `json:"commit,omitempty"`

	// Workdir is the absolute path of the checkout to run.
	Workdir string `json:"workdir"`
	// WorkflowsPath is relative to Workdir and defaults to .github/workflows/.
	WorkflowsPath string `json:"workflowsPath,omitempty"`
	// EventName defaults to "push".
	EventName string          `json:"event,omitempty"`
	EventJSON json.RawMessage `json:"eventJSON,omitempty"`
	// JobID runs a single job, like `act -j`.
	JobID string `json:"job,omitempty"`
	// Matrix filters matrix combinations, like `act --matrix key:value`.
	Matrix map[string][]string `json:"matrix,omitempty"`

	Env        map[string]string `json:"env,omitempty"`
	Vars       map[string]string `json:"vars,omitempty"`
	Inputs     map[string]string `json:"inputs,omitempty"`
	SecretRefs []string          `json:"secretRefs,omitempty"`

	TimeoutSeconds int `json:"timeoutSeconds,omitempty"`
	Priority       int `json:"priority,omitempty"`

	NoRecurse bool `json:"noRecurse,omitempty"`
	Strict    bool `json:"strict,omitempty"`

	// Secrets is filled by the scheduler from SecretRefs, never by clients.
	Secrets map[string]string `json:"-"`
}

// JobState is the public state of one job instance.
type JobState struct {
	Key      string                 `json:"key"`
	Workflow string                 `json:"workflow"`
	JobID    string                 `json:"jobId"`
	Matrix   map[string]interface{} `json:"matrix,omitempty"`
	Status   Status                 `json:"status"`
	// Result is the rule-only binary outcome exposed to callers.  It is
	// derived solely from the runner's terminal job status; category
	// classification (environment vs code) is added by the scoring layer.
	Result     string    `json:"result,omitempty"`
	StartedAt  time.Time `json:"startedAt,omitempty"`
	FinishedAt time.Time `json:"finishedAt,omitempty"`
	Error      string    `json:"error,omitempty"`
}

func (j *JobState) clone() *JobState {
	if j == nil {
		return nil
	}
	c := *j
	if j.Matrix != nil {
		c.Matrix = make(map[string]interface{}, len(j.Matrix))
		for k, v := range j.Matrix {
			c.Matrix[k] = v
		}
	}
	return &c
}

// RunState is the public state of a run.
type RunState struct {
	ID          string      `json:"id"`
	GitHubRunID int64       `json:"-"`
	Repo        string      `json:"repo,omitempty"`
	Commit      string      `json:"commit,omitempty"`
	Workdir     string      `json:"workdir"`
	Event       string      `json:"event"`
	Status      Status      `json:"status"`
	CreatedAt   time.Time   `json:"createdAt"`
	StartedAt   time.Time   `json:"startedAt,omitempty"`
	FinishedAt  time.Time   `json:"finishedAt,omitempty"`
	Error       string      `json:"error,omitempty"`
	Jobs        []*JobState `json:"jobs"`
}

func (r *RunState) clone() *RunState {
	if r == nil {
		return nil
	}
	c := *r
	c.Jobs = make([]*JobState, 0, len(r.Jobs))
	for _, j := range r.Jobs {
		c.Jobs = append(c.Jobs, j.clone())
	}
	return &c
}

// Stats is a snapshot of the scheduler counters.
type Stats struct {
	RunsTotal      int   `json:"runsTotal"`
	RunsQueued     int   `json:"runsQueued"`
	RunsRunning    int   `json:"runsRunning"`
	RunsSucceeded  int   `json:"runsSucceeded"`
	RunsFailed     int   `json:"runsFailed"`
	RunsCancelled  int   `json:"runsCancelled"`
	JobsQueued     int   `json:"jobsQueued"`
	JobsRunning    int   `json:"jobsRunning"`
	ContainersUsed int64 `json:"containersUsed"`
	ContainersMax  int   `json:"containersMax"`
	QueueLength    int   `json:"queueLength"`
}

// Config configures a Scheduler.
type Config struct {
	// DataDir holds logs, caches and artifacts of the runs.
	DataDir string
	// MaxContainers is the global number of concurrently running job containers.
	MaxContainers int
	// WorkerCount is the number of goroutines executing jobs.
	WorkerCount int
	// MaxQueuedRuns bounds how many runs may be queued or running at once.
	MaxQueuedRuns int
	// MaxUnitsPerRun bounds the number of job instances of a single run.
	MaxUnitsPerRun int
	// RunTimeout is the default timeout of a run, zero means no timeout.
	RunTimeout time.Duration
	// AllowedWorkdirRoots limits which directories may be executed.
	AllowedWorkdirRoots []string
	// Secrets available to runs, requests reference them by name.
	Secrets map[string]string
	// BaseRunnerConfig is cloned for every run.
	BaseRunnerConfig *runner.Config
	// LogStdout mirrors job logs to the daemon's stdout.
	LogStdout bool
	// SharedToolCache makes all runs share the default toolcache volume.
	SharedToolCache bool
	// PerRunActionCache gives every run its own action cache directory.
	PerRunActionCache bool
	// Artifact server settings, the daemon runs a single server for all runs.
	ArtifactServerPath    string
	ArtifactServerAddr    string
	ArtifactServerPort    string
	ArtifactServerAuthKey []byte
	// NoCacheServer disables the per run cache server.
	NoCacheServer bool
	// KeepRuns is the number of finished runs kept in memory, 0 keeps all.
	KeepRuns int
	// CleanupInterval is how often the janitor looks for due cleanups.
	CleanupInterval time.Duration
	// CleanupGrace delays the janitor after a run finished so the job
	// executor's own docker cleanup can complete first.
	CleanupGrace time.Duration
	// CleanupRetryDelay is the wait before a failed cleanup is retried.
	CleanupRetryDelay time.Duration
	// CleanupOrphans enables the periodic sweep for act-* containers, volumes,
	// networks and act-built images that do not belong to a live run.
	CleanupOrphans bool
	// CleanupOrphanAge is the minimum age of an orphan before it is removed.
	CleanupOrphanAge time.Duration
	// CleanupOrphanInterval is the sweep period.
	CleanupOrphanInterval time.Duration
}
