package scheduler

import (
	"errors"
	"sync"
	"time"
)

var errRunNotFound = errors.New("run not found")

// Store keeps the state of all runs in memory. Reads always return deep copies,
// so callers never share pointers with the scheduler goroutines.
type Store struct {
	mu    sync.RWMutex
	runs  map[string]*RunState
	order []string // insertion order, oldest first
}

func NewStore() *Store {
	return &Store{runs: map[string]*RunState{}}
}

func (s *Store) Create(run *RunState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.runs[run.ID]; ok {
		return errors.New("run already exists")
	}
	s.runs[run.ID] = run.clone()
	s.order = append(s.order, run.ID)
	return nil
}

func (s *Store) Get(id string) (*RunState, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	run, ok := s.runs[id]
	if !ok {
		return nil, false
	}
	return run.clone(), true
}

// List returns the runs newest first. limit <= 0 returns all runs.
func (s *Store) List(limit int) []*RunState {
	s.mu.RLock()
	defer s.mu.RUnlock()
	runs := make([]*RunState, 0, len(s.order))
	for i := len(s.order) - 1; i >= 0; i-- {
		if run, ok := s.runs[s.order[i]]; ok {
			runs = append(runs, run.clone())
		}
		if limit > 0 && len(runs) >= limit {
			break
		}
	}
	return runs
}

// Len returns the number of runs kept in the store.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.runs)
}

// Update calls fn with the mutable state of a run under the write lock.
func (s *Store) Update(id string, fn func(*RunState)) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	run, ok := s.runs[id]
	if !ok {
		return false
	}
	fn(run)
	return true
}

// AddJobs registers the job instances of a run.
func (s *Store) AddJobs(runID string, jobs []*JobState) {
	s.Update(runID, func(run *RunState) {
		run.Jobs = append(run.Jobs, jobs...)
	})
}

// SetJob updates one job instance and returns the updated copy.
func (s *Store) SetJob(runID, key string, fn func(*JobState)) *JobState {
	var updated *JobState
	s.Update(runID, func(run *RunState) {
		for _, job := range run.Jobs {
			if job.Key == key {
				fn(job)
				updated = job.clone()
				return
			}
		}
	})
	return updated
}

// Finish marks a run as finished.
func (s *Store) Finish(runID string, status Status, runErr error) {
	s.Update(runID, func(run *RunState) {
		if run.Status.Terminal() {
			return
		}
		run.Status = status
		run.FinishedAt = time.Now()
		if runErr != nil {
			run.Error = runErr.Error()
		}
	})
}

// Prune drops the oldest finished runs so that at most keep runs remain. It
// never drops a run that is still queued or running.
func (s *Store) Prune(keep int) {
	if keep <= 0 {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	for len(s.order) > keep {
		idx := -1
		for i, id := range s.order {
			if run, ok := s.runs[id]; ok && run.Status.Terminal() {
				idx = i
				break
			}
		}
		if idx < 0 {
			return
		}
		delete(s.runs, s.order[idx])
		s.order = append(s.order[:idx], s.order[idx+1:]...)
	}
}

// Count returns the number of runs by status.
func (s *Store) Count() map[Status]int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	counts := map[Status]int{}
	for _, run := range s.runs {
		counts[run.Status]++
	}
	return counts
}

// ActiveRuns returns how many runs are queued or running.
func (s *Store) ActiveRuns() int {
	counts := s.Count()
	return counts[StatusQueued] + counts[StatusRunning]
}

// JobsRunning returns how many job instances are running.
func (s *Store) JobsRunning() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	running := 0
	for _, run := range s.runs {
		for _, job := range run.Jobs {
			if job.Status == StatusRunning {
				running++
			}
		}
	}
	return running
}

// JobsQueued returns how many job instances are queued.
func (s *Store) JobsQueued() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	queued := 0
	for _, run := range s.runs {
		for _, job := range run.Jobs {
			if job.Status == StatusQueued {
				queued++
			}
		}
	}
	return queued
}
