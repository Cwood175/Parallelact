package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"runtime"

	docker_container "github.com/moby/moby/api/types/container"
	log "github.com/sirupsen/logrus"

	"github.com/nektos/act/pkg/common"
	"github.com/nektos/act/pkg/model"
)

// Runner provides capabilities to run GitHub actions
type Runner interface {
	NewPlanExecutor(plan *model.Plan) common.Executor
	ExpandRun(ctx context.Context, run *model.Run) (*JobExpansion, error)
	NewJobExecutor(ctx context.Context, inst JobInstance, maxJobNameLen int) common.Executor
}

// JobSlotLimiter limits how many jobs may run at the same time. It is
// implemented by the scheduler so that every job container counts towards a
// global limit, including matrix instances and reusable workflow child jobs.
type JobSlotLimiter interface {
	Acquire(ctx context.Context) error
	Release()
}

// Config contains the config for a new runner
type Config struct {
	Actor                              string                       // the user that triggered the event
	Workdir                            string                       // path to working directory
	ActionCacheDir                     string                       // path used for caching action contents
	ActionOfflineMode                  bool                         // when offline, use caching action contents
	BindWorkdir                        bool                         // bind the workdir to the job container
	EventName                          string                       // name of event to run
	EventPath                          string                       // path to JSON file to use for event.json in containers
	DefaultBranch                      string                       // name of the main branch for this repository
	ReuseContainers                    bool                         // reuse containers to maintain state
	ForcePull                          bool                         // force pulling of the image, even if already present
	ForceRebuild                       bool                         // force rebuilding local docker image action
	LogOutput                          bool                         // log the output from docker run
	JSONLogger                         bool                         // use json or text logger
	LogPrefixJobID                     bool                         // switches from the full job name to the job id
	Env                                map[string]string            // env for containers
	Inputs                             map[string]string            // manually passed action inputs
	Secrets                            map[string]string            // list of secrets
	Vars                               map[string]string            // list of vars
	Token                              string                       // GitHub token
	InsecureSecrets                    bool                         // switch hiding output when printing to terminal
	Platforms                          map[string]string            // list of platforms
	Privileged                         bool                         // use privileged mode
	UsernsMode                         string                       // user namespace to use
	ContainerArchitecture              string                       // Desired OS/architecture platform for running containers
	ContainerDaemonSocket              string                       // Path to Docker daemon socket
	ContainerOptions                   string                       // Options for the job container
	ContainerOptionsPolicy             string                       // auto, restrict, or allow; see restrictWorkflowContainerOptions
	UseGitIgnore                       bool                         // controls if paths in .gitignore should not be copied into container, default true
	GitHubInstance                     string                       // GitHub instance to use, default "github.com"
	ContainerCapAdd                    []string                     // list of kernel capabilities to add to the containers
	ContainerCapDrop                   []string                     // list of kernel capabilities to remove from the containers
	AutoRemove                         bool                         // controls if the container is automatically removed upon workflow completion
	ArtifactServerPath                 string                       // the path where the artifact server stores uploads
	ArtifactServerAddr                 string                       // the address the artifact server binds to
	ArtifactServerPort                 string                       // the port the artifact server binds to
	ArtifactServerAuthKey              []byte                       // key passed to artifacts.Serve; signs ACTIONS_RUNTIME_TOKEN
	NoSkipCheckout                     bool                         // do not skip actions/checkout
	RemoteName                         string                       // remote name in local git repo config
	ReplaceGheActionWithGithubCom      []string                     // Use actions from GitHub Enterprise instance to GitHub
	ReplaceGheActionTokenWithGithubCom string                       // Token of private action repo on GitHub.
	Matrix                             map[string]map[string]bool   // Matrix config to run
	ContainerNetworkMode               docker_container.NetworkMode // the network mode of job containers (the value of --network)
	ActionCache                        ActionCache                  // Use a custom ActionCache Implementation
	ConcurrentJobs                     int                          // Number of max concurrent jobs
	JobCPUs                            int                          // CPU budget of a job container, 0 means unlimited
	RunID                              string                       // unique per run, used in container and volume names
	ToolCacheVolume                    string                       // name of the toolcache volume, defaults to "act-toolcache"
	EventJSON                          string                       // inline event payload, takes precedence over EventPath
	JobSlotLimiter                     JobSlotLimiter               // nil means unlimited
}

// Clone returns a copy of the config with all maps deep-copied, so that a
// daemon can hand out one config per run. The action cache and the job slot
// limiter are intentionally shared.
func (config *Config) Clone() *Config {
	if config == nil {
		return nil
	}
	clone := *config
	clone.Env = cloneStringMap(config.Env)
	clone.Inputs = cloneStringMap(config.Inputs)
	clone.Secrets = cloneStringMap(config.Secrets)
	clone.Vars = cloneStringMap(config.Vars)
	clone.Platforms = cloneStringMap(config.Platforms)
	clone.Matrix = make(map[string]map[string]bool, len(config.Matrix))
	for k, v := range config.Matrix {
		values := make(map[string]bool, len(v))
		for name, allowed := range v {
			values[name] = allowed
		}
		clone.Matrix[k] = values
	}
	clone.ContainerCapAdd = append([]string(nil), config.ContainerCapAdd...)
	clone.ContainerCapDrop = append([]string(nil), config.ContainerCapDrop...)
	clone.ReplaceGheActionWithGithubCom = append([]string(nil), config.ReplaceGheActionWithGithubCom...)
	return &clone
}

func cloneStringMap(in map[string]string) map[string]string {
	if in == nil {
		return nil
	}
	out := make(map[string]string, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func (config *Config) GetConcurrentJobs() int {
	if config.ConcurrentJobs >= 1 {
		return config.ConcurrentJobs
	}

	ncpu := runtime.NumCPU()
	log.Debugf("Detected CPUs: %d", ncpu)
	// Do not let the host core count decide how much work runs at once. On a
	// large host this used to spawn one job per core, and memory hungry test
	// runners sized their worker pools by the same number. The per job CPU
	// budget is the natural upper bound for the default concurrency as well.
	if config.JobCPUs > 0 && ncpu > config.JobCPUs {
		ncpu = config.JobCPUs
	}
	if ncpu > 1 {
		return ncpu
	}
	return 1
}

type caller struct {
	runContext *RunContext
}

type runnerImpl struct {
	config    *Config
	eventJSON string
	caller    *caller // the job calling this runner (caller of a reusable workflow)
}

// New Creates a new Runner
func New(runnerConfig *Config) (Runner, error) {
	runner := &runnerImpl{
		config: runnerConfig,
	}

	return runner.configure()
}

func (runner *runnerImpl) configure() (Runner, error) {
	runner.eventJSON = "{}"
	if runner.config.EventJSON != "" {
		runner.eventJSON = runner.config.EventJSON
	} else if runner.config.EventPath != "" {
		log.Debugf("Reading event.json from %s", runner.config.EventPath)
		eventJSONBytes, err := os.ReadFile(runner.config.EventPath)
		if err != nil {
			return nil, err
		}
		runner.eventJSON = string(eventJSONBytes)
	} else if len(runner.config.Inputs) != 0 {
		eventMap := map[string]map[string]string{
			"inputs": runner.config.Inputs,
		}
		eventJSON, err := json.Marshal(eventMap)
		if err != nil {
			return nil, err
		}
		runner.eventJSON = string(eventJSON)
	}
	return runner, nil
}

// JobInstance is one runnable instance of a job: the job itself expanded with
// one matrix combination. It maps to exactly one job container.
type JobInstance struct {
	Run     *model.Run
	Matrix  map[string]interface{}
	Index   int    // 0-based index of the matrix combination
	Count   int    // number of matrix combinations of this run
	Name    string // run context name, suffixed with "-N" when Count > 1
	NameLen int    // length of the decorated run context name

	rc *RunContext
}

// Result returns the result of this matrix leg (not the matrix group result).
func (inst JobInstance) Result() string {
	if inst.rc == nil {
		return ""
	}
	return inst.rc.LegResult()
}

// JobExpansion is the result of expanding a run into its matrix instances.
type JobExpansion struct {
	Run         *model.Run
	Instances   []JobInstance
	MaxParallel int
}

// ExpandRun expands the matrix of a run without executing anything. It must be
// called exactly once per run and before any of its instances start, because it
// mutates the job strategy and the step IDs, which all instances share.
//
//nolint:gocyclo
func (runner *runnerImpl) ExpandRun(ctx context.Context, run *model.Run) (*JobExpansion, error) {
	job := run.Job()
	if job == nil {
		return nil, fmt.Errorf("job %s not found", run.JobID)
	}

	log.Debugf("Job.Name: %v", job.Name)
	log.Debugf("Job.RawNeeds: %v", job.RawNeeds)
	log.Debugf("Job.RawRunsOn: %v", job.RawRunsOn)
	log.Debugf("Job.Env: %v", job.Env)
	log.Debugf("Job.If: %v", job.If)
	for step := range job.Steps {
		if nil != job.Steps[step] {
			log.Debugf("Job.Steps: %v", job.Steps[step].String())
		}
	}
	log.Debugf("Job.TimeoutMinutes: %v", job.TimeoutMinutes)
	log.Debugf("Job.Services: %v", job.Services)
	log.Debugf("Job.Strategy: %v", job.Strategy)
	log.Debugf("Job.RawContainer: %v", job.RawContainer)
	log.Debugf("Job.Defaults.Run.Shell: %v", job.Defaults.Run.Shell)
	log.Debugf("Job.Defaults.Run.WorkingDirectory: %v", job.Defaults.Run.WorkingDirectory)
	log.Debugf("Job.Outputs: %v", job.GetOutputs())
	log.Debugf("Job.Uses: %v", job.Uses)
	log.Debugf("Job.With: %v", job.With)
	// log.Debugf("Job.RawSecrets: %v", job.RawSecrets)
	log.Debugf("Job.Result: %v", job.GetResult())

	if job.Strategy != nil {
		log.Debugf("Job.Strategy.FailFast: %v", job.Strategy.FailFast)
		log.Debugf("Job.Strategy.MaxParallel: %v", job.Strategy.MaxParallel)
		log.Debugf("Job.Strategy.FailFastString: %v", job.Strategy.FailFastString)
		log.Debugf("Job.Strategy.MaxParallelString: %v", job.Strategy.MaxParallelString)
		log.Debugf("Job.Strategy.RawMatrix: %v", job.Strategy.RawMatrix)

		strategyRc := runner.newRunContext(ctx, run, nil)
		if err := strategyRc.NewExpressionEvaluator(ctx).EvaluateYamlNode(ctx, &job.Strategy.RawMatrix); err != nil {
			log.Errorf("Error while evaluating matrix: %v", err)
		}
	}

	var matrixes []map[string]interface{}
	if m, err := job.GetMatrixes(); err != nil {
		log.Errorf("Error while get job's matrix: %v", err)
	} else {
		log.Debugf("Job Matrices: %v", m)
		log.Debugf("Runner Matrices: %v", runner.config.Matrix)
		matrixes = selectMatrixes(m, runner.config.Matrix)
	}
	log.Debugf("Final matrix after applying user inclusions '%v'", matrixes)

	// Give every step a stable ID before any instance of this job runs: the job
	// model is shared by all matrix instances, so assigning the IDs lazily while
	// running would race.
	for i, step := range job.Steps {
		if step != nil && step.ID == "" {
			step.ID = fmt.Sprintf("%d", i)
		}
	}

	maxParallel := 4
	if job.Strategy != nil {
		maxParallel = job.Strategy.MaxParallel
	}

	if len(matrixes) < maxParallel {
		maxParallel = len(matrixes)
	}

	expansion := &JobExpansion{
		Run:         run,
		Instances:   make([]JobInstance, 0, len(matrixes)),
		MaxParallel: maxParallel,
	}

	for i, matrix := range matrixes {
		rc := runner.newRunContext(ctx, run, matrix)
		rc.JobName = rc.Name
		if len(matrixes) > 1 {
			rc.Name = fmt.Sprintf("%s-%d", rc.Name, i+1)
		}
		expansion.Instances = append(expansion.Instances, JobInstance{
			Run:     run,
			Matrix:  matrix,
			Index:   i,
			Count:   len(matrixes),
			Name:    rc.Name,
			NameLen: len(rc.String()),
			rc:      rc,
		})
	}

	return expansion, nil
}

// NewJobExecutor returns the executor of a single job instance. maxJobNameLen
// pads the displayed job name the same way a whole stage does.
func (runner *runnerImpl) NewJobExecutor(_ context.Context, inst JobInstance, maxJobNameLen int) common.Executor {
	rc := inst.rc
	if rc == nil {
		return common.NewErrorExecutor(fmt.Errorf("job instance %s was not expanded", inst.Name))
	}

	return func(ctx context.Context) error {
		jobName := fmt.Sprintf("%-*s", maxJobNameLen, rc.String())
		executor, err := rc.Executor()

		if err != nil {
			return err
		}

		return executor(common.WithJobErrorContainer(WithJobLogger(ctx, rc.Run.JobID, jobName, rc.Config, rc.Masks, inst.Matrix)))
	}
}

// NewPlanExecutor ...
func (runner *runnerImpl) NewPlanExecutor(plan *model.Plan) common.Executor {
	maxJobNameLen := 0

	stagePipeline := make([]common.Executor, 0)
	log.Debugf("Plan Stages: %v", plan.Stages)

	for i := range plan.Stages {
		stage := plan.Stages[i]
		stagePipeline = append(stagePipeline, func(ctx context.Context) error {
			log.Debugf("Stages Runs: %v", stage.Runs)

			// Expand the whole stage before building any executor, so that all
			// instances of the stage share the same name width.
			expansions := make([]*JobExpansion, 0, len(stage.Runs))
			for _, run := range stage.Runs {
				expansion, err := runner.ExpandRun(ctx, run)
				if err != nil {
					log.Errorf("Error while expanding job %s: %v", run.String(), err)
					continue
				}
				for _, inst := range expansion.Instances {
					if inst.NameLen > maxJobNameLen {
						maxJobNameLen = inst.NameLen
					}
				}
				expansions = append(expansions, expansion)
			}

			pipeline := make([]common.Executor, 0, len(expansions))
			for _, expansion := range expansions {
				stageExecutor := make([]common.Executor, 0, len(expansion.Instances))
				for _, inst := range expansion.Instances {
					inst := inst
					stageExecutor = append(stageExecutor, func(ctx context.Context) error {
						return runner.NewJobExecutor(ctx, inst, maxJobNameLen)(ctx)
					})
				}
				pipeline = append(pipeline, common.NewParallelExecutor(expansion.MaxParallel, stageExecutor...))
			}

			log.Debugf("PlanExecutor concurrency: %d", runner.config.GetConcurrentJobs())
			return common.NewParallelExecutor(runner.config.GetConcurrentJobs(), pipeline...)(ctx)
		})
	}

	return common.NewPipelineExecutor(stagePipeline...).Then(handleFailure(plan))
}

func handleFailure(plan *model.Plan) common.Executor {
	return func(_ context.Context) error {
		for _, stage := range plan.Stages {
			for _, run := range stage.Runs {
				if run.Job().GetResult() == "failure" {
					return fmt.Errorf("Job '%s' failed", run.String())
				}
			}
		}
		return nil
	}
}

func selectMatrixes(originalMatrixes []map[string]interface{}, targetMatrixValues map[string]map[string]bool) []map[string]interface{} {
	matrixes := make([]map[string]interface{}, 0)
	for _, original := range originalMatrixes {
		flag := true
		for key, val := range original {
			if allowedVals, ok := targetMatrixValues[key]; ok {
				valToString := fmt.Sprintf("%v", val)
				if _, ok := allowedVals[valToString]; !ok {
					flag = false
				}
			}
		}
		if flag {
			matrixes = append(matrixes, original)
		}
	}
	return matrixes
}

func (runner *runnerImpl) newRunContext(ctx context.Context, run *model.Run, matrix map[string]interface{}) *RunContext {
	rc := &RunContext{
		Config:      runner.config,
		Run:         run,
		EventJSON:   runner.eventJSON,
		StepResults: make(map[string]*model.StepResult),
		Matrix:      matrix,
		Masks:       &MaskList{},
		caller:      runner.caller,
	}
	rc.ExprEval = rc.NewExpressionEvaluator(ctx)
	rc.Name = rc.ExprEval.Interpolate(ctx, run.String())

	return rc
}
