package scheduler

import (
	"errors"
	"fmt"

	"github.com/nektos/act/pkg/model"
)

var errNoStages = errors.New("no workflow jobs matched the request")

// planFor mirrors the event selection of the act CLI for explicit events.
func planFor(planner model.WorkflowPlanner, req *RunRequest) (*model.Plan, error) {
	if req.JobID != "" {
		return planner.PlanJob(req.JobID)
	}
	return planner.PlanEvent(eventName(req))
}

func eventName(req *RunRequest) string {
	if req.EventName == "" {
		return "push"
	}
	return req.EventName
}

// failedJobKey identifies a job inside a workflow.
func failedJobKey(workflow, jobID string) string {
	return workflow + "\x00" + jobID
}

// planResult reports the failed jobs, like the CLI does at the end of a plan.
func planResult(plan *model.Plan) (map[string]bool, error) {
	failed := map[string]bool{}
	var failures []string
	seen := map[string]bool{}
	for _, stage := range plan.Stages {
		for _, run := range stage.Runs {
			if run.Job().GetResult() != "failure" {
				continue
			}
			key := failedJobKey(workflowKey(run), run.JobID)
			failed[key] = true
			name := workflowKey(run) + "/" + run.JobID
			if seen[name] {
				continue
			}
			seen[name] = true
			failures = append(failures, name)
		}
	}
	if len(failures) == 0 {
		return failed, nil
	}
	return failed, fmt.Errorf("job(s) failed: %v", failures)
}
