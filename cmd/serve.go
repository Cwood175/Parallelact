package cmd

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	docker_container "github.com/moby/moby/api/types/container"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"

	"github.com/nektos/act/pkg/artifacts"
	"github.com/nektos/act/pkg/common"
	"github.com/nektos/act/pkg/container"
	"github.com/nektos/act/pkg/gh"
	"github.com/nektos/act/pkg/scheduler"
)

type serveOptions struct {
	addr               string
	dataDir            string
	maxContainers      int
	workers            int
	maxQueuedRuns      int
	maxUnitsPerRun     int
	runTimeout         time.Duration
	allowedWorkdirRoot string
	apiToken           string
	logStdout          bool
	sharedToolCache    bool
	perRunActionCache  bool
	allowBindWorkdir   bool
	keepRuns           int
	artifactServerDir  string
	cleanupInterval    time.Duration
	cleanupGrace       time.Duration
	cleanupOrphans     bool
	cleanupOrphanAge   time.Duration
	cleanupOrphanEvery time.Duration
}

func createServeCommand(ctx context.Context, input *Input) *cobra.Command {
	opts := &serveOptions{}

	cmd := &cobra.Command{
		Use:   "serve",
		Short: "Run a scheduler that executes many workflow runs in parallel",
		Long: `Run a long lived scheduler that accepts run requests over HTTP.

Each request points at a local checkout and is executed job by job, one container
per job instance. Jobs of different runs run concurrently, bounded by
--max-containers, while the needs relationships of a run are still honoured.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runServe(ctx, cmd, input, opts)
		},
	}

	addServeRunFlags(cmd, input)

	defaultDataDir := filepath.Join(CacheHomeDir, "serve")
	cmd.Flags().StringVar(&opts.addr, "addr", "127.0.0.1:8080", "address the HTTP API listens on")
	cmd.Flags().StringVar(&opts.dataDir, "data-dir", defaultDataDir, "directory for run logs, caches and artifacts")
	cmd.Flags().IntVar(&opts.maxContainers, "max-containers", 20, "maximum number of concurrently running job containers")
	cmd.Flags().IntVar(&opts.workers, "workers", 0, "number of worker goroutines, defaults to max-containers + 8")
	cmd.Flags().IntVar(&opts.maxQueuedRuns, "max-queued-runs", 100, "maximum number of queued or running runs")
	cmd.Flags().IntVar(&opts.maxUnitsPerRun, "max-units-per-run", 200, "maximum number of job instances of a single run")
	cmd.Flags().DurationVar(&opts.runTimeout, "run-timeout", 0, "default timeout of a run, zero means no timeout")
	cmd.Flags().StringVar(&opts.allowedWorkdirRoot, "allowed-workdir-roots", "", "comma separated directories the requests may execute, defaults to the current directory")
	cmd.Flags().StringVar(&opts.apiToken, "api-token", "", "bearer token required by the API, mandatory for non loopback addresses")
	cmd.Flags().BoolVar(&opts.logStdout, "log-stdout", false, "mirror job logs to stdout")
	cmd.Flags().BoolVar(&opts.sharedToolCache, "shared-toolcache", false, "let all runs share the act-toolcache volume")
	cmd.Flags().BoolVar(&opts.perRunActionCache, "per-run-action-cache", false, "give every run its own action cache directory")
	cmd.Flags().BoolVar(&opts.allowBindWorkdir, "allow-bind-workdir", false, "allow requests to bind their workdir instead of copying it")
	cmd.Flags().IntVar(&opts.keepRuns, "keep-runs", 0, "number of finished runs kept in memory, zero keeps all")
	cmd.Flags().StringVar(&opts.artifactServerDir, "artifact-server-dir", "", "directory of the artifact server, defaults to <data-dir>/artifacts")
	cmd.Flags().DurationVar(&opts.cleanupInterval, "cleanup-interval", 15*time.Second, "how often the scheduler janitor looks for due cleanups")
	cmd.Flags().DurationVar(&opts.cleanupGrace, "cleanup-grace", 45*time.Second, "delay before leftover containers/volumes of a finished run are removed")
	cmd.Flags().BoolVar(&opts.cleanupOrphans, "cleanup-orphans", true, "periodically remove act-* containers, volumes, networks and act-built images that belong to no live run")
	cmd.Flags().DurationVar(&opts.cleanupOrphanAge, "cleanup-orphan-age", time.Hour, "minimum age before an orphaned act resource is removed")
	cmd.Flags().DurationVar(&opts.cleanupOrphanEvery, "cleanup-orphan-interval", 10*time.Minute, "orphan sweep period")

	return cmd
}

// addServeRunFlags mirrors the run related flags of the root command. The user
// configuration files are passed to every command, so serve has to accept the
// flags users typically put there.
func addServeRunFlags(cmd *cobra.Command, input *Input) {
	cmd.Flags().StringVar(&input.remoteName, "remote-name", "origin", "git remote name that will be used to retrieve url of git repo")
	cmd.Flags().StringArrayVarP(&input.secrets, "secret", "s", []string{}, "secret to make available to actions with optional value (e.g. -s mysecret=foo or -s mysecret)")
	cmd.Flags().StringArrayVar(&input.vars, "var", []string{}, "variable to make available to actions with optional value (e.g. --var myvar=foo)")
	cmd.Flags().StringArrayVarP(&input.envs, "env", "", []string{}, "env to make available to actions (e.g. --env myenv=foo)")
	cmd.Flags().StringArrayVarP(&input.inputs, "input", "", []string{}, "action input to make available to actions (e.g. --input myinput=foo)")
	cmd.Flags().StringArrayVarP(&input.platforms, "platform", "P", []string{}, "custom image to use per platform (e.g. -P ubuntu-latest=node:16-buster-slim)")
	cmd.Flags().BoolVarP(&input.reuseContainers, "reuse", "r", false, "don't remove container(s) on successfully completed workflow(s) to maintain state between runs")
	cmd.Flags().BoolVarP(&input.bindWorkdir, "bind", "b", false, "bind working directory to container, rather than copy")
	cmd.Flags().BoolVarP(&input.forcePull, "pull", "p", false, "pull docker image(s) even if already present")
	cmd.Flags().BoolVar(&input.forceRebuild, "rebuild", true, "rebuild local action docker image(s) even if already present")
	cmd.Flags().StringVarP(&input.defaultBranch, "defaultbranch", "", "", "the name of the main branch")
	cmd.Flags().BoolVar(&input.privileged, "privileged", false, "use privileged mode")
	cmd.Flags().StringVar(&input.usernsMode, "userns", "", "user namespace to use")
	cmd.Flags().BoolVar(&input.useGitIgnore, "use-gitignore", true, "Controls whether paths specified in .gitignore should be copied into container")
	cmd.Flags().StringArrayVar(&input.containerCapAdd, "container-cap-add", []string{}, "kernel capabilities to add to the workflow containers")
	cmd.Flags().StringArrayVar(&input.containerCapDrop, "container-cap-drop", []string{}, "kernel capabilities to remove from the workflow containers")
	cmd.Flags().BoolVar(&input.autoRemove, "rm", false, "automatically remove container(s)/volume(s) after a workflow(s) failure")
	cmd.Flags().StringArrayVar(&input.replaceGheActionWithGithubCom, "replace-ghe-action-with-github-com", []string{}, "If you are using GitHub Enterprise Server and allow specified actions from GitHub (github.com), you can set actions on this.")
	cmd.Flags().StringVar(&input.replaceGheActionTokenWithGithubCom, "replace-ghe-action-token-with-github-com", "", "personal access token for private actions replaced with github.com")
	cmd.Flags().StringArrayVarP(&input.matrix, "matrix", "", []string{}, "specify which matrix configuration to include (e.g. --matrix java:13)")
	cmd.Flags().StringVar(&input.containerOptions, "container-options", "", "Custom docker container options for the job container")
	cmd.Flags().StringVar(&input.containerOptionsPolicy, "container-options-policy", input.containerOptionsPolicy, "Policy for container options and host path volumes set by workflows")
	cmd.Flags().BoolVar(&input.useNewActionCache, "use-new-action-cache", false, "Enable using the new Action Cache for storing Actions locally")
	cmd.Flags().StringArrayVar(&input.localRepository, "local-repository", []string{}, "Replaces the specified repository and ref with a local folder")
	cmd.Flags().IntVar(&input.concurrentJobs, "concurrent-jobs", 0, "Maximum number of concurrent jobs per run")
}

//nolint:gocyclo
func runServe(ctx context.Context, cmd *cobra.Command, input *Input, opts *serveOptions) error {
	if opts.maxContainers < 1 {
		return fmt.Errorf("--max-containers must be at least 1")
	}
	if opts.workers < 1 {
		// Workers beyond the container limit can never run concurrently; they
		// would only add goroutines. Do not scale this with the host core
		// count (a 100+ core host used to create hundreds of workers).
		opts.workers = opts.maxContainers + 8
	}
	if err := requireAPIToken(opts.addr, opts.apiToken); err != nil {
		return err
	}

	absDataDir, err := filepath.Abs(opts.dataDir)
	if err != nil {
		return err
	}
	opts.dataDir = absDataDir
	if opts.artifactServerDir == "" {
		opts.artifactServerDir = filepath.Join(opts.dataDir, "artifacts")
	}

	allowedRoots, err := parseWorkdirRoots(opts.allowedWorkdirRoot)
	if err != nil {
		return err
	}

	if input.bindWorkdir && !opts.allowBindWorkdir {
		return fmt.Errorf("--bind shares the host workdir between jobs and breaks isolation; use --allow-bind-workdir to allow it")
	}

	if ret, err := container.GetSocketAndHost(input.containerDaemonSocket); err != nil {
		log.Warnf("Couldn't get a valid docker connection: %+v", err)
	} else {
		os.Setenv("DOCKER_HOST", ret.Host)
		input.containerDaemonSocket = ret.Socket
		log.Infof("Using docker host '%s', and daemon socket '%s'", ret.Host, ret.Socket)
	}

	log.Debugf("Loading environment from %s", input.Envfile())
	envs := parseEnvs(input.envs)
	_ = readProjectEnvs(input.Workdir(), input.Envfile(), envs, false)

	log.Debugf("Loading action inputs from %s", input.Inputfile())
	inputs := parseEnvs(input.inputs)
	_ = readProjectEnvs(input.Workdir(), input.Inputfile(), inputs, false)

	vars := parseEnvs(input.vars)
	_ = readProjectEnvs(input.Workdir(), input.Varfile(), vars, false)

	secrets, err := loadServeSecrets(ctx, input)
	if err != nil {
		return err
	}

	artifactKey, err := artifacts.NewAuthKey()
	if err != nil {
		return err
	}

	baseConfig := newRunnerConfig(runnerConfigInput{
		input:                 input,
		eventName:             "",
		defaultBranch:         input.defaultBranch,
		envs:                  envs,
		secrets:               secrets,
		vars:                  vars,
		inputs:                inputs,
		matrixes:              nil,
		artifactAddr:          input.artifactServerAddr,
		artifactServerAuthKey: artifactKey,
	})
	baseConfig.AutoRemove = true
	baseConfig.ReuseContainers = false
	baseConfig.ConcurrentJobs = opts.maxContainers
	baseConfig.ArtifactServerPath = opts.artifactServerDir
	baseConfig.ContainerNetworkMode = docker_container.NetworkMode(input.networkName)

	sched, err := scheduler.New(scheduler.Config{
		DataDir:               opts.dataDir,
		MaxContainers:         opts.maxContainers,
		WorkerCount:           opts.workers,
		MaxQueuedRuns:         opts.maxQueuedRuns,
		MaxUnitsPerRun:        opts.maxUnitsPerRun,
		RunTimeout:            opts.runTimeout,
		AllowedWorkdirRoots:   allowedRoots,
		Secrets:               secrets,
		BaseRunnerConfig:      baseConfig,
		LogStdout:             opts.logStdout,
		SharedToolCache:       opts.sharedToolCache,
		PerRunActionCache:     opts.perRunActionCache,
		ArtifactServerPath:    opts.artifactServerDir,
		ArtifactServerAddr:    input.artifactServerAddr,
		ArtifactServerPort:    input.artifactServerPort,
		ArtifactServerAuthKey: artifactKey,
		NoCacheServer:         input.noCacheServer,
		KeepRuns:              opts.keepRuns,
		CleanupInterval:       opts.cleanupInterval,
		CleanupGrace:          opts.cleanupGrace,
		CleanupOrphans:        opts.cleanupOrphans,
		CleanupOrphanAge:      opts.cleanupOrphanAge,
		CleanupOrphanInterval: opts.cleanupOrphanEvery,
	})
	if err != nil {
		return err
	}

	listener, err := net.Listen("tcp", opts.addr)
	if err != nil {
		_ = sched.Shutdown(context.Background())
		return fmt.Errorf("failed to listen on %s: %w", opts.addr, err)
	}

	server := &http.Server{
		Handler:           sched.Handler(opts.apiToken),
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       2 * time.Minute,
	}

	serveErr := make(chan error, 1)
	go func() {
		serveErr <- server.Serve(listener)
	}()

	log.Infof("act scheduler listening on http://%s (max %d containers, data dir %s)", listener.Addr(), opts.maxContainers, opts.dataDir)

	jobCancel := common.JobCancelContext(ctx)
	select {
	case <-ctx.Done():
	case <-jobCancelDone(jobCancel):
	case err := <-serveErr:
		if !errors.Is(err, http.ErrServerClosed) {
			_ = sched.Shutdown(context.Background())
			return err
		}
	}

	log.Infof("Shutting down the scheduler")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		log.Warnf("HTTP server shutdown: %v", err)
	}
	if err := sched.Shutdown(shutdownCtx); err != nil {
		return err
	}
	log.Infof("Scheduler stopped")
	return nil
}

func jobCancelDone(ctx context.Context) <-chan struct{} {
	if ctx == nil {
		return nil
	}
	return ctx.Done()
}

// requireAPIToken refuses to expose the API on a non loopback address without
// a token: requests can execute arbitrary checkouts on this host.
func requireAPIToken(addr, token string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid --addr %q: %w", addr, err)
	}
	if token != "" {
		return nil
	}
	if host == "" || host == "0.0.0.0" || host == "::" || net.ParseIP(host) == nil {
		return fmt.Errorf("--api-token is required when the API does not listen on a loopback address")
	}
	ip := net.ParseIP(host)
	if !ip.IsLoopback() {
		return fmt.Errorf("--api-token is required when the API does not listen on a loopback address")
	}
	return nil
}

func parseWorkdirRoots(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		wd, err := os.Getwd()
		if err != nil {
			return nil, err
		}
		return []string{wd}, nil
	}
	roots := make([]string, 0)
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		absolute, err := filepath.Abs(part)
		if err != nil {
			return nil, err
		}
		roots = append(roots, absolute)
	}
	if len(roots) == 0 {
		return nil, fmt.Errorf("--allowed-workdir-roots does not contain a directory")
	}
	return roots, nil
}

// loadServeSecrets loads the static secrets of the daemon. Unlike the CLI it
// never prompts: a missing value is a configuration error.
func loadServeSecrets(ctx context.Context, input *Input) (map[string]string, error) {
	secrets := map[string]string{}
	for _, pair := range input.secrets {
		name, value, hasValue := strings.Cut(pair, "=")
		name = strings.ToUpper(strings.TrimSpace(name))
		if hasValue {
			secrets[name] = value
			continue
		}
		if env, ok := os.LookupEnv(name); ok {
			secrets[name] = env
			continue
		}
		return nil, fmt.Errorf("secret %s has no value: pass %s=value or set the environment variable", name, name)
	}

	_ = readProjectEnvs(input.Workdir(), input.Secretfile(), secrets, true)

	if _, ok := secrets["GITHUB_TOKEN"]; !ok {
		if token, err := gh.GetToken(ctx, ""); err == nil && token != "" {
			secrets["GITHUB_TOKEN"] = token
		}
	}
	return secrets, nil
}
