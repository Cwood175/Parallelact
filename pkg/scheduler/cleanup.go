package scheduler

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/moby/moby/client"
	log "github.com/sirupsen/logrus"

	"github.com/nektos/act/pkg/container"
)

// Leftover docker resources are removed by a janitor instead of relying only on
// the job executor's cleanup chain. The chain is skipped when a run is
// cancelled between `docker create` and `docker start` (a client timeout is
// enough to trigger that): the run is marked cancelled, but the created
// container, its env volume and the per-job toolcache volume are never removed.
//
// The janitor works per run id, and only for run ids this scheduler knows
// about (finished runs, and run directories left by a previous process). It
// never scans for arbitrary `act-*` resources, so other act instances sharing
// the docker daemon are not affected.
const (
	defaultCleanupInterval = 15 * time.Second
	defaultCleanupGrace    = 45 * time.Second
	cleanupRetryDelay      = 30 * time.Second
	cleanupDockerTimeout   = 5 * time.Minute
	orphanDockerTimeout    = 30 * time.Minute
	defaultOrphanAge       = time.Hour
	defaultOrphanInterval  = 10 * time.Minute
	// A job container only runs `tail -f /dev/null`; one that is still "Up"
	// after this long belongs to a run that died without cleanup.
	orphanRunningAge = 3 * time.Hour
)

// runIDPattern extracts the run id from an act resource name:
//
//	act-r20260101T000000deadbeef-job-<hash>
//	act-r20260101T000000deadbeef-job-<hash>-env
//	act-toolcache-r20260101T000000deadbeef-<hash>
var runIDPattern = regexp.MustCompile(`^act-(?:toolcache-)?(r\d{8}T\d{6}[0-9a-f]{8})(?:-|$)`)

// cleanupResources removes the docker resources of the given runs and reports
// which runs were cleaned. It is a field so tests can replace it.
type cleanupResourcesFunc func(runIDs []string) map[string]bool

func (s *Scheduler) initCleanup() {
	if s.cfg.CleanupInterval <= 0 {
		s.cfg.CleanupInterval = defaultCleanupInterval
	}
	if s.cfg.CleanupGrace <= 0 {
		s.cfg.CleanupGrace = defaultCleanupGrace
	}
	if s.cfg.CleanupRetryDelay <= 0 {
		s.cfg.CleanupRetryDelay = cleanupRetryDelay
	}
	if s.cfg.CleanupOrphanAge <= 0 {
		s.cfg.CleanupOrphanAge = defaultOrphanAge
	}
	if s.cfg.CleanupOrphanInterval <= 0 {
		s.cfg.CleanupOrphanInterval = defaultOrphanInterval
	}
	s.cleanupDue = map[string]time.Time{}
	s.cleanupWake = make(chan struct{}, 1)
	s.cleanupStop = make(chan struct{})
	s.cleanupResources = s.removeRunResources
}

// startCleanup starts the janitor goroutine and schedules the run directories
// left behind by a previous process. The scheduler owns its data dir, so every
// directory below runs/ belongs to a run of this scheduler that is not active
// anymore.
func (s *Scheduler) startCleanup() {
	s.cleanupWG.Add(1)
	go s.cleanupLoop()
	go s.sweepPreviousRuns()
	if s.cfg.CleanupOrphans {
		s.cleanupWG.Add(1)
		go s.orphanLoop()
	}
}

func (s *Scheduler) sweepPreviousRuns() {
	seen := map[string]bool{}
	if entries, err := os.ReadDir(filepath.Join(s.cfg.DataDir, "runs")); err == nil {
		for _, entry := range entries {
			if entry.IsDir() {
				seen[entry.Name()] = true
			}
		}
	}
	// Run directories are janitor-cleaned data and may be gone; the run id log
	// survives, so leftovers of a process that was killed are still known.
	if data, err := os.ReadFile(s.runIDsPath()); err == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if runID := strings.TrimSpace(line); runID != "" {
				seen[runID] = true
			}
		}
	}
	for runID := range seen {
		s.scheduleCleanupAfter(runID, s.cfg.CleanupGrace)
	}
}

func (s *Scheduler) runIDsPath() string {
	return filepath.Join(s.cfg.DataDir, "runids.log")
}

// recordRunID persists the run id so a restarted scheduler still knows which
// docker resources belong to it (and can remove them without the orphan age
// penalty).
func (s *Scheduler) recordRunID(runID string) {
	if runID == "" {
		return
	}
	f, err := os.OpenFile(s.runIDsPath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	_, _ = f.WriteString(runID + "\n")
	_ = f.Close()
}

// scheduleCleanup asks the janitor to remove the docker resources of a run
// after CleanupGrace. Scheduling the same run twice keeps the first deadline.
func (s *Scheduler) scheduleCleanup(runID string) {
	s.scheduleCleanupAfter(runID, s.cfg.CleanupGrace)
}

func (s *Scheduler) scheduleCleanupAfter(runID string, delay time.Duration) {
	if runID == "" || s.cleanupDue == nil {
		return
	}
	due := time.Now().Add(delay)
	s.cleanupMu.Lock()
	if existing, ok := s.cleanupDue[runID]; !ok || due.Before(existing) {
		s.cleanupDue[runID] = due
	}
	s.cleanupMu.Unlock()
	select {
	case s.cleanupWake <- struct{}{}:
	default:
	}
}

func (s *Scheduler) cleanupLoop() {
	defer s.cleanupWG.Done()
	ticker := time.NewTicker(s.cfg.CleanupInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.cleanupStop:
			return
		case <-s.cleanupWake:
		case <-ticker.C:
		}
		s.runDueCleanup()
	}
}

// orphanLoop removes resources of runs this scheduler does not know anymore
// (wiped data dir, SIGKILL). It runs separately from the per-run cleanup so a
// long sweep of large toolcache volumes does not delay normal cleanups.
func (s *Scheduler) orphanLoop() {
	defer s.cleanupWG.Done()
	ticker := time.NewTicker(s.cfg.CleanupOrphanInterval)
	defer ticker.Stop()
	for {
		s.orphanSweep()
		select {
		case <-s.cleanupStop:
			return
		case <-ticker.C:
		}
	}
}

func (s *Scheduler) runDueCleanup() {
	now := time.Now()
	var ids []string
	s.cleanupMu.Lock()
	for runID, due := range s.cleanupDue {
		if !due.After(now) {
			ids = append(ids, runID)
		}
	}
	s.cleanupMu.Unlock()
	if len(ids) == 0 {
		return
	}

	s.cleanupMu.Lock()
	cleanup := s.cleanupResources
	s.cleanupMu.Unlock()
	if cleanup == nil {
		cleanup = s.removeRunResources
	}
	cleaned := cleanup(ids)

	s.cleanupMu.Lock()
	for _, runID := range ids {
		if cleaned[runID] {
			delete(s.cleanupDue, runID)
			continue
		}
		// Docker was busy or a removal failed; retry later. The run stays in
		// the map, so a leaked container is not forgotten.
		s.cleanupDue[runID] = time.Now().Add(s.cfg.CleanupRetryDelay)
	}
	s.cleanupMu.Unlock()
}

// setCleanupResources replaces the docker cleanup implementation (tests).
func (s *Scheduler) setCleanupResources(fn cleanupResourcesFunc) {
	s.cleanupMu.Lock()
	s.cleanupResources = fn
	s.cleanupMu.Unlock()
}

// pendingCleanups returns the number of runs waiting for their resources to be
// removed. Used by tests and /v1/stats style diagnostics.
func (s *Scheduler) pendingCleanups() int {
	s.cleanupMu.Lock()
	defer s.cleanupMu.Unlock()
	return len(s.cleanupDue)
}

func (s *Scheduler) stopCleanup() {
	if s.cleanupStop == nil {
		return
	}
	select {
	case <-s.cleanupStop:
	default:
		close(s.cleanupStop)
	}
	s.cleanupWG.Wait()
}

// cleanupActiveRuns removes the resources of runs that are still active. Called
// on shutdown when workers could not finish their own cleanup.
func (s *Scheduler) cleanupActiveRuns() {
	s.mu.Lock()
	ids := make([]string, 0, len(s.handles))
	for runID := range s.handles {
		ids = append(ids, runID)
	}
	s.mu.Unlock()
	if len(ids) > 0 {
		s.cleanupResources(ids)
	}
}

// removeRunResources removes every container, volume and network whose name is
// derived from one of the given run ids:
//
//	act-<runID>-<job>...        job container, its -env volume, service network
//	act-toolcache-<runID>-...   per-job toolcache volume
func (s *Scheduler) removeRunResources(runIDs []string) map[string]bool {
	cleaned := make(map[string]bool, len(runIDs))
	wanted := make(map[string]bool, len(runIDs))
	for _, runID := range runIDs {
		cleaned[runID] = true
		wanted[runID] = true
	}
	if len(runIDs) == 0 {
		return cleaned
	}
	if s.cfg.BaseRunnerConfig != nil && s.cfg.BaseRunnerConfig.ReuseContainers {
		// The operator asked to keep containers between runs.
		return cleaned
	}

	ctx, cancel := context.WithTimeout(context.Background(), cleanupDockerTimeout)
	defer cancel()

	cli, err := container.GetDockerClient(ctx)
	if err != nil {
		log.Debugf("cleanup: docker client unavailable: %v", err)
		for _, runID := range runIDs {
			cleaned[runID] = false
		}
		return cleaned
	}
	defer cli.Close()

	containers, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err != nil {
		log.Debugf("cleanup: cannot list containers: %v", err)
		for _, runID := range runIDs {
			cleaned[runID] = false
		}
		return cleaned
	}
	volumes, err := cli.VolumeList(ctx, client.VolumeListOptions{})
	if err != nil {
		log.Debugf("cleanup: cannot list volumes: %v", err)
		for _, runID := range runIDs {
			cleaned[runID] = false
		}
		return cleaned
	}
	networks, err := cli.NetworkList(ctx, client.NetworkListOptions{})
	if err != nil {
		log.Debugf("cleanup: cannot list networks: %v", err)
		for _, runID := range runIDs {
			cleaned[runID] = false
		}
		return cleaned
	}

	removed := 0
	failed := func(runID string, what string, err error) {
		if isDockerNotFound(err) {
			return
		}
		log.WithField("run", runID).Debugf("cleanup: remove %s: %v", what, err)
		cleaned[runID] = false
	}

	// Index by run id once: O(resources + runs) instead of O(runs × resources).
	for _, c := range containers.Items {
		for _, rawName := range c.Names {
			name := strings.TrimPrefix(rawName, "/")
			match := runIDPattern.FindStringSubmatch(name)
			if match == nil || !wanted[match[1]] {
				continue
			}
			if _, err := cli.ContainerRemove(ctx, c.ID, client.ContainerRemoveOptions{
				Force:         true,
				RemoveVolumes: true,
			}); err != nil {
				failed(match[1], "container "+name, err)
				continue
			}
			removed++
		}
	}

	for _, volume := range volumes.Items {
		name := volume.Name
		if name == "act-toolcache" {
			// Shared toolcache volume, never owned by a single run.
			continue
		}
		match := runIDPattern.FindStringSubmatch(name)
		if match == nil || !wanted[match[1]] {
			continue
		}
		if _, err := cli.VolumeRemove(ctx, name, client.VolumeRemoveOptions{Force: true}); err != nil {
			failed(match[1], "volume "+name, err)
			continue
		}
		removed++
	}

	for _, network := range networks.Items {
		match := runIDPattern.FindStringSubmatch(network.Name)
		if match == nil || !wanted[match[1]] {
			continue
		}
		if _, err := cli.NetworkRemove(ctx, network.ID, client.NetworkRemoveOptions{}); err != nil {
			failed(match[1], "network "+network.Name, err)
			continue
		}
		removed++
	}

	if removed > 0 {
		log.Infof("cleanup: removed %d docker resource(s) for %d run(s)", removed, len(runIDs))
	}
	return cleaned
}

// isDockerNotFound reports errors that mean "already gone". Treating them as
// failures made the janitor retry the same run forever.
func isDockerNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no such container") ||
		strings.Contains(msg, "no such volume") ||
		strings.Contains(msg, "no such network") ||
		strings.Contains(msg, "not found") ||
		strings.Contains(msg, "404")
}

// knownRunIDs returns every run id this scheduler has a record of, regardless
// of status. Resources of a known run that is no longer active can be removed
// without waiting for the orphan age.
func (s *Scheduler) knownRunIDs() map[string]bool {
	known := map[string]bool{}
	for _, run := range s.store.List(0) {
		known[run.ID] = true
	}
	s.cleanupMu.Lock()
	for runID := range s.cleanupDue {
		known[runID] = true
	}
	s.cleanupMu.Unlock()
	return known
}

// activeRunIDs returns the runs that may still own docker resources.
func (s *Scheduler) activeRunIDs() map[string]bool {
	active := map[string]bool{}
	s.mu.Lock()
	for runID := range s.handles {
		active[runID] = true
	}
	s.mu.Unlock()
	for _, run := range s.store.List(0) {
		if run.Status == StatusQueued || run.Status == StatusRunning {
			active[run.ID] = true
		}
	}
	return active
}

func parseDockerTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

// orphanSweep removes act-owned docker resources that do not belong to a live
// run. It is the second line of defence after per-run cleanup: it also covers
// resources whose run id is no longer present in the store (wiped data dir,
// scheduler killed with SIGKILL, run pruned before its cleanup).
//
// Only names that carry an act run id (`act-<runID>-…` /
// `act-toolcache-<runID>…`) and are older than CleanupOrphanAge are touched;
// unrelated volumes (pact-*, sonarqube_*, trace-hv-dind-*) never match.
func (s *Scheduler) orphanSweep() {
	if !s.cfg.CleanupOrphans {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), orphanDockerTimeout)
	defer cancel()

	cli, err := container.GetDockerClient(ctx)
	if err != nil {
		log.Debugf("cleanup: orphan sweep skipped, docker unavailable: %v", err)
		return
	}
	defer cli.Close()

	active := s.activeRunIDs()
	known := s.knownRunIDs()
	cutoff := time.Now().Add(-s.cfg.CleanupOrphanAge)
	removed := 0

	containers, err := cli.ContainerList(ctx, client.ContainerListOptions{All: true})
	if err == nil {
		for _, c := range containers.Items {
			for _, rawName := range c.Names {
				name := strings.TrimPrefix(rawName, "/")
				match := runIDPattern.FindStringSubmatch(name)
				if match == nil || active[match[1]] {
					continue
				}
				created := time.Unix(c.Created, 0)
				if known[match[1]] {
					// A run of this scheduler that is not active anymore: its
					// containers are dead even if they still show as running
					// (the job container is just `tail -f /dev/null`).
					if time.Since(created) < s.cfg.CleanupGrace {
						continue
					}
				} else {
					// Unknown run id (another act instance or a wiped data
					// dir). Be conservative.
					if strings.EqualFold(string(c.State), "running") && time.Since(created) < orphanRunningAge {
						continue
					}
					if created.After(cutoff) {
						continue
					}
				}
				if _, err := cli.ContainerRemove(ctx, c.ID, client.ContainerRemoveOptions{
					Force:         true,
					RemoveVolumes: true,
				}); err != nil {
					log.Debugf("cleanup: orphan container %s: %v", name, err)
					continue
				}
				removed++
			}
		}
	}

	volumes, err := cli.VolumeList(ctx, client.VolumeListOptions{})
	if err == nil {
		for _, volume := range volumes.Items {
			match := runIDPattern.FindStringSubmatch(volume.Name)
			if match == nil || active[match[1]] {
				continue
			}
			if volume.Name == "act-toolcache" || (s.cfg.SharedToolCache && volume.Name == "act-toolcache") {
				continue
			}
			created := parseDockerTime(volume.CreatedAt)
			if known[match[1]] {
				if !created.IsZero() && time.Since(created) < s.cfg.CleanupGrace {
					continue
				}
			} else if created.IsZero() || created.After(cutoff) {
				// Unknown owner and unknown age: keep it rather than risk
				// deleting a resource of another process.
				continue
			}
			if _, err := cli.VolumeRemove(ctx, volume.Name, client.VolumeRemoveOptions{Force: true}); err != nil {
				log.Debugf("cleanup: orphan volume %s: %v", volume.Name, err)
				continue
			}
			removed++
		}
	}

	networks, err := cli.NetworkList(ctx, client.NetworkListOptions{})
	if err == nil {
		for _, network := range networks.Items {
			match := runIDPattern.FindStringSubmatch(network.Name)
			if match == nil || active[match[1]] {
				continue
			}
			if _, err := cli.NetworkRemove(ctx, network.ID, client.NetworkRemoveOptions{}); err != nil {
				log.Debugf("cleanup: orphan network %s: %v", network.Name, err)
				continue
			}
			removed++
		}
	}

	// Images: only those built by act for docker actions, plus dangling images
	// older than the cutoff. Pulled runner images are never removed.
	images, err := cli.ImageList(ctx, client.ImageListOptions{All: true})
	if err == nil {
		for _, img := range images.Items {
			if time.Unix(img.Created, 0).After(cutoff) {
				continue
			}
			actBuilt := false
			for _, tag := range img.RepoTags {
				if strings.HasPrefix(tag, "act-github-actions-exec-in-build-image-") {
					actBuilt = true
					break
				}
			}
			dangling := len(img.RepoTags) == 0 && len(img.RepoDigests) == 0
			if !actBuilt && !dangling {
				continue
			}
			if _, err := cli.ImageRemove(ctx, img.ID, client.ImageRemoveOptions{
				Force:         false,
				PruneChildren: true,
			}); err != nil {
				log.Debugf("cleanup: orphan image %s: %v", img.ID, err)
				continue
			}
			removed++
		}
	}

	if removed > 0 {
		log.Infof("cleanup: orphan sweep removed %d docker resource(s)", removed)
	}
}

// cleanupState is embedded in Scheduler.
type cleanupState struct {
	cleanupMu        sync.Mutex
	cleanupDue       map[string]time.Time
	cleanupWake      chan struct{}
	cleanupStop      chan struct{}
	cleanupWG        sync.WaitGroup
	cleanupResources cleanupResourcesFunc
}
