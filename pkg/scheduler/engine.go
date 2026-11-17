package scheduler

import (
	"context"
	"strconv"

	"github.com/nektos/act/pkg/common"
	"github.com/nektos/act/pkg/model"
	"github.com/nektos/act/pkg/runner"
)

// Engine turns a run request into an executable plan. It is an interface so
// that the scheduling logic can be tested without Docker.
type Engine interface {
	// Prepare parses the workflows and builds the stage sequence. It must not
	// execute any job.
	Prepare(ctx context.Context, req *RunRequest, cfg *runner.Config) (*ExecutionPlan, error)
}

// ExecutionPlan is the engine independent form of a model.Plan.
type ExecutionPlan struct {
	Stages []*ExecutionStage
	// Finalize inspects the results after all stages ran. It returns the
	// failed jobs keyed by "workflow\x00jobID" and an aggregate error.
	Finalize func() (map[string]bool, error)
}

// ExecutionStage expands its jobs when it becomes runnable, i.e. after the
// previous stage finished.
type ExecutionStage struct {
	// Name is only used for logging.
	Name   string
	Expand func(ctx context.Context) ([]*JobGroup, error)
}

// JobGroup is the set of instances of one job (matrix expansion).
type JobGroup struct {
	MaxParallel int
	Units       []*JobUnit
}

// JobUnit is a single job instance; executing it creates exactly one container.
type JobUnit struct {
	Key      string
	Workflow string
	JobID    string
	Matrix   map[string]interface{}
	Exec     common.Executor
	// JobResult returns the job result tracked by the runner ("", "success" or
	// "failure"). The runner swallows step failures, so it is the only way to
	// know whether the job failed. Optional.
	JobResult func() string
}

// RunnerEngine runs real GitHub workflows with act.
type RunnerEngine struct{}

func NewRunnerEngine() Engine {
	return &RunnerEngine{}
}

// Prepare implements Engine.
func (e *RunnerEngine) Prepare(ctx context.Context, req *RunRequest, cfg *runner.Config) (*ExecutionPlan, error) {
	planner, err := model.NewWorkflowPlanner(workflowsPath(req), req.NoRecurse, req.Strict)
	if err != nil {
		return nil, err
	}

	plan, err := planFor(planner, req)
	if err != nil {
		return nil, err
	}
	if plan == nil || len(plan.Stages) == 0 {
		return nil, errNoStages
	}

	actRunner, err := runner.New(cfg)
	if err != nil {
		return nil, err
	}

	execPlan := &ExecutionPlan{}
	// maxJobNameLen is shared by all stages on purpose: the CLI pads the names
	// of a stage with the longest name seen in that stage and keeps growing it.
	maxJobNameLen := 0

	for _, stage := range plan.Stages {
		stage := stage
		name := ""
		if len(stage.Runs) > 0 {
			name = stage.Runs[0].String()
		}

		execPlan.Stages = append(execPlan.Stages, &ExecutionStage{
			Name: name,
			Expand: func(ctx context.Context) ([]*JobGroup, error) {
				expansions := make([]*runner.JobExpansion, 0, len(stage.Runs))
				for _, run := range stage.Runs {
					expansion, err := actRunner.ExpandRun(ctx, run)
					if err != nil {
						return nil, err
					}
					for _, inst := range expansion.Instances {
						if inst.NameLen > maxJobNameLen {
							maxJobNameLen = inst.NameLen
						}
					}
					expansions = append(expansions, expansion)
				}

				groups := make([]*JobGroup, 0, len(expansions))
				for _, expansion := range expansions {
					group := &JobGroup{
						MaxParallel: expansion.MaxParallel,
						Units:       make([]*JobUnit, 0, len(expansion.Instances)),
					}
					for _, inst := range expansion.Instances {
						run := expansion.Run
						inst := inst
						group.Units = append(group.Units, &JobUnit{
							Key:       unitKey(run, inst),
							Workflow:  workflowKey(run),
							JobID:     run.JobID,
							Matrix:    inst.Matrix,
							Exec:      actRunner.NewJobExecutor(ctx, inst, maxJobNameLen),
							JobResult: inst.Result,
						})
					}
					groups = append(groups, group)
				}
				return groups, nil
			},
		})
	}

	execPlan.Finalize = func() (map[string]bool, error) {
		return planResult(plan)
	}

	return execPlan, nil
}

func unitKey(run *model.Run, inst runner.JobInstance) string {
	return formatJobKey(workflowKey(run), run.JobID, inst.Index)
}

func workflowKey(run *model.Run) string {
	if run.Workflow == nil {
		return "workflow"
	}
	return run.Workflow.Name
}

func formatJobKey(workflow, jobID string, index int) string {
	return sanitizeKey(workflow) + "/" + sanitizeKey(jobID) + "/" + strconv.Itoa(index)
}
