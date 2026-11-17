package scheduler

import (
	"context"
	"sync/atomic"
)

// Semaphore is the global limit of concurrently running job containers. It
// implements runner.JobSlotLimiter.
type Semaphore struct {
	slots chan struct{}
	inUse atomic.Int64
}

func NewSemaphore(size int) *Semaphore {
	if size < 1 {
		size = 1
	}
	return &Semaphore{slots: make(chan struct{}, size)}
}

// Acquire blocks until a slot is free or the context is done.
func (s *Semaphore) Acquire(ctx context.Context) error {
	select {
	case s.slots <- struct{}{}:
		s.inUse.Add(1)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Release returns a slot. It must only be called after a successful Acquire.
func (s *Semaphore) Release() {
	select {
	case <-s.slots:
		s.inUse.Add(-1)
	default:
	}
}

func (s *Semaphore) InUse() int64 {
	return s.inUse.Load()
}

func (s *Semaphore) Capacity() int {
	return cap(s.slots)
}
