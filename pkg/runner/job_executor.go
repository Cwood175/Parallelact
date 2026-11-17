package runner

import (
	"context"
	"fmt"
	"time"

	"github.com/nektos/act/pkg/common"
	"github.com/nektos/act/pkg/model"
)

type jobInfo interface {
	matrix() map[string]interface{}
	steps() []*model.Step
	startContainer() common.Executor
	stopContainer() common.Executor
	closeContainer() common.Executor
	interpolateOutputs() common.Executor
	result(result string)
}

//nolint:contextcheck
func newJobExecutor(info jobInfo, sf stepFactory, rc *RunContext) common.Executor {
	steps := make([]common.Executor, 0)
	preSteps := make([]common.Executor, 0)
	var postExecutor common.Executor

	steps = append(steps, func(ctx context.Context) error {
		logger := common.Logger(ctx)
		if len(info.matrix()) > 0 {
			logger.Infof("\U0001F9EA  Matrix: %v", info.matrix())
		}
		return nil
	})

	infoSteps := info.steps()

	if len(infoSteps) == 0 {
		return common.NewDebugExecutor("No steps found")
	}

	preSteps = append(preSteps, func(ctx context.Context) error {
		// Have to be skipped for some Tests
		if rc.Run == nil {
			return nil
		}
		rc.ExprEval = rc.NewExpressionEvaluator(ctx)
		// evaluate environment variables since they can contain
		// GitHub's special environment variables.
		for k, v := range rc.GetEnv() {
			rc.Env[k] = rc.ExprEval.Interpolate(ctx, v)
		}
		return nil
	})

	var setJobError = func(ctx context.Context, err error) error {
		if err == nil {
			return nil
		}
		logger := common.Logger(ctx)
		logger.Errorf("%v", err)
		common.SetJobError(ctx, err)
		return err
	}

	for i, stepModel := range infoSteps {
		if stepModel == nil {
			return func(_ context.Context) error {
				return fmt.Errorf("invalid Step %v: missing run or uses key", i)
			}
		}
		if stepModel.ID == "" {
			stepModel.ID = fmt.Sprintf("%d", i)
		}

		step, err := sf.newStep(stepModel, rc)

		if err != nil {
			return common.NewErrorExecutor(err)
		}

		preSteps = append(preSteps, useStepLogger(rc, stepModel, stepStagePre, step.pre().ThenError(setJobError)))

		stepExec := step.main()
		steps = append(steps, useStepLogger(rc, stepModel, stepStageMain, func(ctx context.Context) error {
			err := stepExec(ctx)
			if err != nil {
				_ = setJobError(ctx, err)
			} else if ctx.Err() != nil {
				_ = setJobError(ctx, ctx.Err())
			}
			return nil
		}))

		postExec := useStepLogger(rc, stepModel, stepStagePost, step.post().ThenError(setJobError))
		if postExecutor != nil {
			// run the post executor in reverse order
			postExecutor = postExec.Finally(postExecutor)
		} else {
			postExecutor = postExec
		}
	}

	// Tracks the cleanup performed by the normal path, so the safety net below
	// only runs when the body pipeline was skipped.
	stopped := false
	closed := false

	var stopContainerExecutor common.Executor = func(ctx context.Context) error {
		jobError := common.JobError(ctx)
		var err error
		if rc.Config.AutoRemove || jobError == nil {
			// always allow 1 min for stopping and removing the runner, even if we were cancelled
			ctx, cancel := context.WithTimeout(common.WithLogger(context.Background(), common.Logger(ctx)), time.Minute)
			defer cancel()

			logger := common.Logger(ctx)
			logger.Infof("Cleaning up container for job %s", rc.JobName)
			if err = info.stopContainer()(ctx); err != nil {
				logger.Errorf("Error while stop job container: %v", err)
			} else {
				stopped = true
			}
		}
		return err
	}

	closeContainerExecutor := func(ctx context.Context) error {
		err := info.closeContainer()(ctx)
		if err == nil {
			closed = true
		}
		return err
	}

	var setJobResultExecutor common.Executor = func(ctx context.Context) error {
		jobError := common.JobError(ctx)
		setJobResult(ctx, info, rc, jobError == nil)
		setJobOutputs(ctx, rc)
		return nil
	}

	pipeline := make([]common.Executor, 0)
	pipeline = append(pipeline, preSteps...)
	pipeline = append(pipeline, steps...)

	setupExecutor := common.NewFieldExecutor("step", "Set up job", common.NewFieldExecutor("stepid", []string{"--setup-job"},
		common.NewPipelineExecutor(common.NewInfoExecutor("\u2B50 Run Set up job"), info.startContainer(), rc.InitializeNodeTool()).
			Then(common.NewFieldExecutor("stepResult", model.StepStatusSuccess, common.NewInfoExecutor("  \u2705  Success - Set up job"))).
			ThenError(setJobError).OnError(common.NewFieldExecutor("stepResult", model.StepStatusFailure, common.NewInfoExecutor("  \u274C  Failure - Set up job")))))

	body := common.NewPipelineExecutor(pipeline...).
		Finally(func(ctx context.Context) error { //nolint:contextcheck
			var cancel context.CancelFunc
			if ctx.Err() == context.Canceled {
				// in case of an aborted run, we still should execute the
				// post steps to allow cleanup.
				ctx, cancel = context.WithTimeout(common.WithLogger(context.Background(), common.Logger(ctx)), 5*time.Minute)
				defer cancel()
			}
			return postExecutor(ctx)
		}).
		Finally(common.NewFieldExecutor("step", "Complete job", common.NewFieldExecutor("stepid", []string{"--complete-job"},
			common.NewInfoExecutor("\u2B50 Run Complete job").
				Finally(stopContainerExecutor).
				Finally(
					info.interpolateOutputs().Finally(closeContainerExecutor).Then(common.NewFieldExecutor("stepResult", model.StepStatusSuccess, common.NewInfoExecutor("  \u2705  Success - Complete job"))).
						OnError(common.NewFieldExecutor("stepResult", model.StepStatusFailure, common.NewInfoExecutor("  \u274C  Failure - Complete job"))),
				))))

	// The container cleanup lives on the outermost pipeline on purpose: when
	// startContainer fails after `docker create` (cancelled run, start error),
	// the body pipeline never runs, and a cleanup attached to it would leak the
	// created container, its volumes and the job slot.
	return common.NewPipelineExecutor(setupExecutor, body).
		Finally(setJobResultExecutor).
		Finally(func(ctx context.Context) error {
			if stopped || !(rc.Config.AutoRemove || common.JobError(ctx) == nil) {
				return nil
			}
			return cleanupAfterFailure("stop job container", info.stopContainer())(ctx)
		}).
		Finally(func(ctx context.Context) error {
			if closed {
				return nil
			}
			return cleanupAfterFailure("close job container", info.closeContainer())(ctx)
		}).
		Finally(rc.releaseJobSlot())
}

// cleanupAfterFailure runs a cleanup executor with a fresh context. Errors are
// logged, not returned: they must not replace the job's real error. Leaked
// resources are retried by the scheduler janitor.
func cleanupAfterFailure(name string, exec common.Executor) common.Executor {
	return func(ctx context.Context) error {
		//nolint:contextcheck
		ctx, cancel := context.WithTimeout(common.WithLogger(context.Background(), common.Logger(ctx)), time.Minute)
		defer cancel()
		if err := exec(ctx); err != nil {
			common.Logger(ctx).Debugf("%s cleanup failed: %v", name, err)
		}
		return nil
	}
}

func setJobResult(ctx context.Context, info jobInfo, rc *RunContext, success bool) {
	logger := common.Logger(ctx)

	legResult := "success"
	if !success {
		legResult = "failure"
	}

	// Job.Result is the *group* result: a matrix job is one job for needs and
	// the final plan outcome, so one failed leg makes the whole group failed.
	// The leg itself still succeeded and must be reported (and scored) as such;
	// otherwise a matrix leg whose steps all passed is shown as "Job failed"
	// just because a sibling leg failed.
	jobResult := legResult
	if len(info.matrix()) > 0 && rc.Run.Job().GetResult() != "" {
		jobResult = rc.Run.Job().GetResult()
	}
	if !success {
		jobResult = "failure"
	}

	info.result(jobResult)
	rc.setLegResult(legResult)
	if rc.caller != nil {
		// set reusable workflow job result
		rc.caller.runContext.result(jobResult)
	}

	jobResultMessage := "succeeded"
	if legResult != "success" {
		jobResultMessage = "failed"
	}

	logger.WithField("jobResult", legResult).Infof("\U0001F3C1  Job %s", jobResultMessage)
}

func setJobOutputs(ctx context.Context, rc *RunContext) {
	if rc.caller != nil {
		// map outputs for reusable workflows
		callerOutputs := make(map[string]string)

		ee := rc.NewExpressionEvaluator(ctx)

		for k, v := range rc.Run.Workflow.WorkflowCallConfig().Outputs {
			callerOutputs[k] = ee.Interpolate(ctx, ee.Interpolate(ctx, v.Value))
		}

		rc.caller.runContext.Run.Job().SetOutputs(callerOutputs)
	}
}

func useStepLogger(rc *RunContext, stepModel *model.Step, stage stepStage, executor common.Executor) common.Executor {
	return func(ctx context.Context) error {
		ctx = withStepLogger(ctx, stepModel.ID, rc.ExprEval.Interpolate(ctx, stepModel.String()), stage.String())

		rawLogger := common.Logger(ctx).WithField("raw_output", true)
		logWriter := common.NewLineWriter(rc.commandHandler(ctx), func(s string) bool {
			if rc.Config.LogOutput {
				rawLogger.Infof("%s", s)
			} else {
				rawLogger.Debugf("%s", s)
			}
			return true
		})

		oldout, olderr := rc.JobContainer.ReplaceLogWriter(logWriter, logWriter)
		defer rc.JobContainer.ReplaceLogWriter(oldout, olderr)

		return executor(ctx)
	}
}
