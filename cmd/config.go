package cmd

import (
	"strings"

	docker_container "github.com/moby/moby/api/types/container"

	"github.com/nektos/act/pkg/runner"
)

// runnerConfigInput carries the values needed to build a runner.Config. It is
// shared by the classic run command and by `act serve`.
type runnerConfigInput struct {
	input                 *Input
	eventName             string
	defaultBranch         string
	envs                  map[string]string
	secrets               map[string]string
	vars                  map[string]string
	inputs                map[string]string
	matrixes              map[string]map[string]bool
	artifactAddr          string
	artifactServerAuthKey []byte
}

// newRunnerConfig builds a runner.Config from CLI input. It is a pure
// translation: no defaults are added here.
func newRunnerConfig(p runnerConfigInput) *runner.Config {
	input := p.input
	config := &runner.Config{
		Actor:                              input.actor,
		EventName:                          p.eventName,
		EventPath:                          input.EventPath(),
		DefaultBranch:                      p.defaultBranch,
		ForcePull:                          !input.actionOfflineMode && input.forcePull,
		ForceRebuild:                       input.forceRebuild,
		ReuseContainers:                    input.reuseContainers,
		Workdir:                            input.Workdir(),
		ActionCacheDir:                     input.actionCachePath,
		ActionOfflineMode:                  input.actionOfflineMode,
		BindWorkdir:                        input.bindWorkdir,
		LogOutput:                          !input.noOutput,
		JSONLogger:                         input.jsonLogger,
		LogPrefixJobID:                     input.logPrefixJobID,
		Env:                                p.envs,
		Secrets:                            p.secrets,
		Vars:                               p.vars,
		Inputs:                             p.inputs,
		Token:                              p.secrets["GITHUB_TOKEN"],
		InsecureSecrets:                    input.insecureSecrets,
		Platforms:                          input.newPlatforms(),
		Privileged:                         input.privileged,
		UsernsMode:                         input.usernsMode,
		ContainerArchitecture:              input.containerArchitecture,
		ContainerDaemonSocket:              input.containerDaemonSocket,
		ContainerOptions:                   input.containerOptions,
		ContainerOptionsPolicy:             input.containerOptionsPolicy,
		UseGitIgnore:                       input.useGitIgnore,
		GitHubInstance:                     input.githubInstance,
		ContainerCapAdd:                    input.containerCapAdd,
		ContainerCapDrop:                   input.containerCapDrop,
		AutoRemove:                         input.autoRemove,
		ArtifactServerPath:                 input.artifactServerPath,
		ArtifactServerAddr:                 p.artifactAddr,
		ArtifactServerPort:                 input.artifactServerPort,
		ArtifactServerAuthKey:              p.artifactServerAuthKey,
		NoSkipCheckout:                     input.noSkipCheckout,
		RemoteName:                         input.remoteName,
		ReplaceGheActionWithGithubCom:      input.replaceGheActionWithGithubCom,
		ReplaceGheActionTokenWithGithubCom: input.replaceGheActionTokenWithGithubCom,
		Matrix:                             p.matrixes,
		ContainerNetworkMode:               docker_container.NetworkMode(input.networkName),
		ConcurrentJobs:                     input.concurrentJobs,
		JobCPUs:                            input.jobCPUs,
	}

	if input.useNewActionCache || len(input.localRepository) > 0 {
		var cache runner.ActionCache
		if input.actionOfflineMode {
			cache = &runner.GoGitActionCacheOfflineMode{
				Parent: runner.GoGitActionCache{
					Path: config.ActionCacheDir,
				},
			}
		} else {
			cache = &runner.GoGitActionCache{
				Path: config.ActionCacheDir,
			}
		}
		if len(input.localRepository) > 0 {
			localRepositories := map[string]string{}
			for _, l := range input.localRepository {
				k, v, _ := strings.Cut(l, "=")
				localRepositories[k] = v
			}
			cache = &runner.LocalRepositoryCache{
				Parent:            cache,
				LocalRepositories: localRepositories,
				CacheDirCache:     map[string]string{},
			}
		}
		config.ActionCache = cache
	}

	return config
}
