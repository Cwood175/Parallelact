package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueOrder(t *testing.T) {
	q := NewQueue()
	q.Push(&Task{RunID: "low", Priority: 0, Seq: 1})
	q.Push(&Task{RunID: "high", Priority: 5, Seq: 2})
	q.Push(&Task{RunID: "high2", Priority: 5, Seq: 3})

	ctx := context.Background()
	for _, want := range []string{"high", "high2", "low"} {
		task, err := q.Pop(ctx)
		require.NoError(t, err)
		assert.Equal(t, want, task.RunID)
	}
}

func TestQueuePopBlocksUntilPush(t *testing.T) {
	q := NewQueue()
	got := make(chan string, 1)
	go func() {
		task, err := q.Pop(context.Background())
		if err == nil {
			got <- task.RunID
		}
	}()

	time.Sleep(10 * time.Millisecond)
	q.Push(&Task{RunID: "late"})

	select {
	case id := <-got:
		assert.Equal(t, "late", id)
	case <-time.After(time.Second):
		t.Fatal("Pop did not return the pushed task")
	}
}

func TestQueueCloseWakesPop(t *testing.T) {
	q := NewQueue()
	errCh := make(chan error, 1)
	go func() {
		_, err := q.Pop(context.Background())
		errCh <- err
	}()

	time.Sleep(10 * time.Millisecond)
	q.Close()

	select {
	case err := <-errCh:
		assert.ErrorIs(t, err, ErrQueueClosed)
	case <-time.After(time.Second):
		t.Fatal("Pop did not return after Close")
	}
}

func TestQueuePopHonoursContext(t *testing.T) {
	q := NewQueue()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := q.Pop(ctx)
	assert.ErrorIs(t, err, context.Canceled)
}

func TestSemaphoreLimit(t *testing.T) {
	sem := NewSemaphore(2)
	ctx := context.Background()
	require.NoError(t, sem.Acquire(ctx))
	require.NoError(t, sem.Acquire(ctx))
	assert.Equal(t, int64(2), sem.InUse())

	cctx, cancel := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancel()
	assert.Error(t, sem.Acquire(cctx), "third acquire must block until the limit frees up")

	sem.Release()
	assert.Equal(t, int64(1), sem.InUse())
	require.NoError(t, sem.Acquire(ctx))
	assert.Equal(t, int64(2), sem.InUse())
}
