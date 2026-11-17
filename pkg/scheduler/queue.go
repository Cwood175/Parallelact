package scheduler

import (
	"container/heap"
	"context"
	"errors"
	"sync"

	"github.com/nektos/act/pkg/runner"
)

// ErrQueueClosed is returned by Pop once the queue is closed and drained.
var ErrQueueClosed = errors.New("scheduler queue closed")

// Task is one job instance waiting to be executed by a worker.
type Task struct {
	RunID    string
	Unit     *JobUnit
	Config   *runner.Config
	Ctx      context.Context
	Priority int
	Seq      uint64

	start       func()
	finish      func(error)
	resultStart string
}

type taskHeap []*Task

func (h taskHeap) Len() int { return len(h) }

func (h taskHeap) Less(i, j int) bool {
	// Higher priority first, then FIFO by submission sequence.
	if h[i].Priority != h[j].Priority {
		return h[i].Priority > h[j].Priority
	}
	return h[i].Seq < h[j].Seq
}

func (h taskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *taskHeap) Push(x any) {
	*h = append(*h, x.(*Task))
}

func (h *taskHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

// Queue is a priority queue of job instances. Workers block in Pop until a task
// is available, the queue is closed or their context is done.
type Queue struct {
	mu       sync.Mutex
	items    taskHeap
	notify   chan struct{}
	closedCh chan struct{}
	closed   bool
}

func NewQueue() *Queue {
	return &Queue{
		notify:   make(chan struct{}, 1),
		closedCh: make(chan struct{}),
	}
}

func (q *Queue) Push(task *Task) bool {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return false
	}
	heap.Push(&q.items, task)
	q.mu.Unlock()
	q.signal()
	return true
}

func (q *Queue) Pop(ctx context.Context) (*Task, error) {
	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			task := heap.Pop(&q.items).(*Task)
			q.mu.Unlock()
			return task, nil
		}
		if q.closed {
			q.mu.Unlock()
			return nil, ErrQueueClosed
		}
		q.mu.Unlock()

		select {
		case <-q.notify:
		case <-q.closedCh:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// Close wakes up all workers and drains the queue.
func (q *Queue) Close() {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return
	}
	q.closed = true
	q.items = nil
	q.mu.Unlock()
	// Wake every blocked worker, not just one.
	close(q.closedCh)
}

func (q *Queue) Len() int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.items)
}

func (q *Queue) signal() {
	select {
	case q.notify <- struct{}{}:
	default:
	}
}
