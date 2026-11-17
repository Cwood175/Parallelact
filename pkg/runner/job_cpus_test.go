package runner

import (
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseCPUList(t *testing.T) {
	cases := map[string][]int{
		"":            nil,
		"0":           {0},
		"0-3":         {0, 1, 2, 3},
		"0-3,8,10-12": {0, 1, 2, 3, 8, 10, 11, 12},
		" 0-1 , 4 ":   {0, 1, 4},
		"3-1":         nil,
		"a-b":         nil,
		"1,,2":        {1, 2},
		"0-3,2-4":     {0, 1, 2, 3, 4},
		"1-2a":        nil,
	}
	for input, expected := range cases {
		assert.Equal(t, expected, parseCPUList(input), input)
	}
}

func TestFormatCPUList(t *testing.T) {
	assert.Equal(t, "", formatCPUList(nil))
	assert.Equal(t, "0", formatCPUList([]int{0}))
	assert.Equal(t, "0-3", formatCPUList([]int{0, 1, 2, 3}))
	assert.Equal(t, "0-2,5,7-9", formatCPUList([]int{0, 1, 2, 5, 7, 8, 9}))
}

func TestPickCPUWindow(t *testing.T) {
	cpus := []int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}
	assert.Equal(t, []int{0, 1, 2, 3}, pickCPUWindow(cpus, 4, 0))
	assert.Equal(t, []int{4, 5, 6, 7}, pickCPUWindow(cpus, 4, 4))
	// wrapping around
	assert.Equal(t, []int{0, 1, 8, 9}, pickCPUWindow(cpus, 4, 8))
	// more CPUs requested than available
	assert.Equal(t, cpus, pickCPUWindow(cpus, 20, 0))
	assert.Nil(t, pickCPUWindow(nil, 4, 0))
	assert.Nil(t, pickCPUWindow(cpus, 0, 0))
}

func TestJobContainerCPULimit(t *testing.T) {
	nanoCPUs, cpuset := jobContainerCPULimit(0)
	assert.Zero(t, nanoCPUs)
	assert.Empty(t, cpuset)

	nanoCPUs, cpuset = jobContainerCPULimit(4)
	assert.Equal(t, int64(4_000_000_000), nanoCPUs)
	if runtime.GOOS == "linux" {
		allowed := allowedCPUs()
		if len(allowed) >= 4 {
			assert.NotEmpty(t, cpuset)
			assert.Len(t, parseCPUList(cpuset), 4)
			for _, cpu := range parseCPUList(cpuset) {
				assert.Contains(t, allowed, cpu)
			}
		}
	} else {
		assert.Empty(t, cpuset)
	}
}

func TestWithJobCPUEnv(t *testing.T) {
	env := withJobCPUEnv([]string{"PATH=/bin"}, 4)
	for _, name := range jobCPUEnvDefaults {
		assert.Contains(t, env, name+"=4")
	}
	assert.Contains(t, env, "PATH=/bin")

	// explicitly configured values win
	env = withJobCPUEnv([]string{"GOMAXPROCS=8"}, 4)
	assert.Contains(t, env, "GOMAXPROCS=8")
	assert.NotContains(t, env, "GOMAXPROCS=4")
}

func TestGetConcurrentJobsHonoursJobCPUs(t *testing.T) {
	// An explicit concurrency is never changed.
	assert.Equal(t, 9, (&Config{ConcurrentJobs: 9, JobCPUs: 4}).GetConcurrentJobs())

	// Without an explicit value the per-job budget caps the host core count.
	assert.Equal(t, 4, (&Config{JobCPUs: 4}).GetConcurrentJobs())
	assert.Equal(t, 1, (&Config{JobCPUs: 1}).GetConcurrentJobs())

	// Zero keeps the historical behaviour (host cores, at least one).
	got := (&Config{JobCPUs: 0}).GetConcurrentJobs()
	expected := runtime.NumCPU()
	if expected < 1 {
		expected = 1
	}
	assert.Equal(t, expected, got)
}

func TestToolCacheVolumeFor(t *testing.T) {
	if got := toolCacheVolumeFor("", "act-r-job"); got != "act-toolcache" {
		t.Fatalf("shared volume = %q", got)
	}
	a := toolCacheVolumeFor("act-toolcache-r1", "act-r1-test-1")
	b := toolCacheVolumeFor("act-toolcache-r1", "act-r1-test-2")
	if a == b {
		t.Fatalf("matrix legs share toolcache volume %q", a)
	}
	if !strings.HasPrefix(a, "act-toolcache-r1-") {
		t.Fatalf("unexpected volume %q", a)
	}
}

func TestLegResult(t *testing.T) {
	rc := &RunContext{}
	if rc.LegResult() != "" {
		t.Fatal("empty result expected")
	}
	rc.setLegResult("failure")
	if rc.LegResult() != "failure" {
		t.Fatal("leg result not stored")
	}
}
