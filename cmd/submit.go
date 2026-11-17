package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/nektos/act/pkg/scheduler"
)

type submitOptions struct {
	addr      string
	token     string
	workdir   string
	repo      string
	commit    string
	workflows string
	event     string
	eventFile string
	job       string
	matrix    []string
	envs      []string
	secretRef []string
	timeout   int
	priority  int
	wait      bool
	output    string
}

func createSubmitCommand(_ context.Context, input *Input) *cobra.Command {
	opts := &submitOptions{}

	cmd := &cobra.Command{
		Use:   "submit",
		Short: "Submit a run to a running `act serve` scheduler",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSubmit(cmd, input, opts)
		},
	}

	cmd.Flags().StringVar(&opts.addr, "addr", "http://127.0.0.1:8080", "address of the scheduler")
	cmd.Flags().StringVar(&opts.token, "token", "", "API token of the scheduler")
	cmd.Flags().StringVar(&opts.workdir, "workdir", ".", "local checkout to run")
	cmd.Flags().StringVar(&opts.repo, "repo", "", "repository name, metadata only")
	cmd.Flags().StringVar(&opts.commit, "commit", "", "commit SHA, metadata only")
	cmd.Flags().StringVar(&opts.workflows, "workflows-path", "", "path to workflow file(s) inside the workdir")
	cmd.Flags().StringVarP(&opts.event, "event", "e", "", "event name to run")
	cmd.Flags().StringVar(&opts.eventFile, "event-file", "", "path to a JSON event payload")
	cmd.Flags().StringVarP(&opts.job, "job", "j", "", "run a specific job ID")
	cmd.Flags().StringArrayVar(&opts.matrix, "matrix", []string{}, "matrix configuration to include (e.g. --matrix java:13)")
	cmd.Flags().StringArrayVar(&opts.envs, "env", []string{}, "env to make available to actions (e.g. --env myenv=foo)")
	cmd.Flags().StringArrayVar(&opts.secretRef, "secret-ref", []string{}, "name of a secret the scheduler should provide")
	cmd.Flags().IntVar(&opts.timeout, "timeout", 0, "timeout of the run in seconds")
	cmd.Flags().IntVar(&opts.priority, "priority", 0, "higher priority runs are dispatched first")
	cmd.Flags().BoolVar(&opts.wait, "wait", false, "wait for the run to finish and print its result")
	cmd.Flags().StringVar(&opts.output, "output", "text", "output format: text or json")

	return cmd
}

func runSubmit(cmd *cobra.Command, input *Input, opts *submitOptions) error {
	workdir, err := filepath.Abs(opts.workdir)
	if err != nil {
		return err
	}

	request := scheduler.RunRequest{
		Repo:           opts.repo,
		Commit:         opts.commit,
		Workdir:        workdir,
		WorkflowsPath:  opts.workflows,
		EventName:      opts.event,
		JobID:          opts.job,
		Matrix:         parseMatrixFlag(opts.matrix),
		Env:            parseEnvs(opts.envs),
		SecretRefs:     opts.secretRef,
		TimeoutSeconds: opts.timeout,
		Priority:       opts.priority,
	}
	for k, v := range parseEnvs(input.envs) {
		if request.Env == nil {
			request.Env = map[string]string{}
		}
		request.Env[k] = v
	}

	if opts.eventFile != "" {
		payload, err := os.ReadFile(opts.eventFile)
		if err != nil {
			return err
		}
		request.EventJSON = payload
	}

	run, err := postRun(opts, request)
	if err != nil {
		return err
	}

	if opts.output == "json" && !opts.wait {
		encoded, err := json.MarshalIndent(run, "", "  ")
		if err != nil {
			return err
		}
		fmt.Println(string(encoded))
		return nil
	}

	fmt.Printf("Submitted run %s (status %s)\n", run.ID, run.Status)
	fmt.Printf("  inspect: %s/v1/runs/%s\n", strings.TrimSuffix(opts.addr, "/"), run.ID)
	fmt.Printf("  cancel:  curl -X POST %s/v1/runs/%s/cancel\n", strings.TrimSuffix(opts.addr, "/"), run.ID)

	if !opts.wait {
		return nil
	}
	return waitForRun(opts, run.ID)
}

func postRun(opts *submitOptions, request scheduler.RunRequest) (*scheduler.RunState, error) {
	payload, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	url := strings.TrimSuffix(opts.addr, "/") + "/v1/runs"
	httpRequest, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}
	httpRequest.Header.Set("Content-Type", "application/json")
	if opts.token != "" {
		httpRequest.Header.Set("Authorization", "Bearer "+opts.token)
	}

	client := &http.Client{Timeout: 30 * time.Second}
	response, err := client.Do(httpRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to reach the scheduler at %s: %w", opts.addr, err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		return nil, err
	}
	if response.StatusCode != http.StatusAccepted {
		return nil, fmt.Errorf("scheduler returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}

	var run scheduler.RunState
	if err := json.Unmarshal(body, &run); err != nil {
		return nil, err
	}
	return &run, nil
}

func waitForRun(opts *submitOptions, runID string) error {
	url := fmt.Sprintf("%s/v1/runs/%s", strings.TrimSuffix(opts.addr, "/"), runID)
	client := &http.Client{Timeout: 30 * time.Second}

	for {
		request, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			return err
		}
		if opts.token != "" {
			request.Header.Set("Authorization", "Bearer "+opts.token)
		}

		response, err := client.Do(request)
		if err != nil {
			return err
		}
		body, readErr := io.ReadAll(response.Body)
		response.Body.Close()
		if readErr != nil {
			return readErr
		}
		if response.StatusCode != http.StatusOK {
			return fmt.Errorf("scheduler returned %s: %s", response.Status, strings.TrimSpace(string(body)))
		}

		var run scheduler.RunState
		if err := json.Unmarshal(body, &run); err != nil {
			return err
		}

		if run.Status.Terminal() {
			if opts.output == "json" {
				encoded, err := json.MarshalIndent(&run, "", "  ")
				if err != nil {
					return err
				}
				fmt.Println(string(encoded))
			}
			printRunResult(opts, &run)
			if run.Status == scheduler.StatusSuccess {
				return nil
			}
			return fmt.Errorf("run %s finished with status %s", run.ID, run.Status)
		}
		time.Sleep(2 * time.Second)
	}
}

func printRunResult(opts *submitOptions, run *scheduler.RunState) {
	if opts.output == "json" {
		return
	}
	fmt.Printf("\nRun %s finished: %s\n", run.ID, run.Status)
	for _, job := range run.Jobs {
		line := fmt.Sprintf("  %-40s %s", job.Key, job.Status)
		if job.Result != "" {
			line += "  result=" + job.Result
		}
		if job.Error != "" {
			line += "  " + job.Error
		}
		fmt.Println(line)
	}
	if run.Error != "" {
		fmt.Printf("  error: %s\n", run.Error)
	}
	fmt.Printf("\nLogs: %s/v1/runs/%s/logs\n", strings.TrimSuffix(opts.addr, "/"), run.ID)
}

// parseMatrixFlag parses repeated key:value flags into the request form.
func parseMatrixFlag(values []string) map[string][]string {
	if len(values) == 0 {
		return nil
	}
	matrix := map[string][]string{}
	for _, entry := range values {
		key, value, found := strings.Cut(entry, ":")
		if !found {
			continue
		}
		matrix[key] = append(matrix[key], value)
	}
	return matrix
}
