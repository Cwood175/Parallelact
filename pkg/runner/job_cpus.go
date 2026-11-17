package runner

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
)

// jobCPUWindowCounter rotates the CPU windows handed to job containers so that
// concurrent jobs do not all end up on the same cores.
var jobCPUWindowCounter atomic.Uint64

// parseCPUList parses a Linux CPU list such as "0-3,8,10-12".
func parseCPUList(list string) []int {
	var cpus []int
	for _, part := range strings.Split(strings.TrimSpace(list), ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		first, last, hasRange := strings.Cut(part, "-")
		start, err := strconv.Atoi(strings.TrimSpace(first))
		if err != nil || start < 0 {
			continue
		}
		end := start
		if hasRange {
			end, err = strconv.Atoi(strings.TrimSpace(last))
			if err != nil || end < start {
				continue
			}
		}
		for cpu := start; cpu <= end; cpu++ {
			cpus = append(cpus, cpu)
		}
	}
	sort.Ints(cpus)
	unique := cpus[:0]
	for i, cpu := range cpus {
		if i > 0 && cpu == cpus[i-1] {
			continue
		}
		unique = append(unique, cpu)
	}
	return unique
}

// formatCPUList renders CPU ids in the compact Linux cpuset format.
func formatCPUList(cpus []int) string {
	if len(cpus) == 0 {
		return ""
	}
	var parts []string
	start, prev := cpus[0], cpus[0]
	flush := func() {
		if start == prev {
			parts = append(parts, strconv.Itoa(start))
			return
		}
		parts = append(parts, fmt.Sprintf("%d-%d", start, prev))
	}
	for _, cpu := range cpus[1:] {
		if cpu == prev+1 {
			prev = cpu
			continue
		}
		flush()
		start, prev = cpu, cpu
	}
	flush()
	return strings.Join(parts, ",")
}

// pickCPUWindow selects count CPUs starting at start, wrapping around. It is
// used to give every job container an affinity window of its own.
func pickCPUWindow(cpus []int, count, start int) []int {
	if count <= 0 || len(cpus) == 0 {
		return nil
	}
	if count > len(cpus) {
		count = len(cpus)
	}
	if start < 0 {
		start = 0
	}
	window := make([]int, 0, count)
	for i := 0; i < count; i++ {
		window = append(window, cpus[(start+i)%len(cpus)])
	}
	sort.Ints(window)
	return window
}

// allocateJobCpuset returns a rotating affinity window of count CPUs from the
// CPUs this process may use, or "" when the platform has no CPU affinity.
func allocateJobCpuset(count int) string {
	if count <= 0 {
		return ""
	}
	allowed := allowedCPUs()
	if len(allowed) == 0 {
		return ""
	}
	window := int(jobCPUWindowCounter.Add(1) - 1)
	return formatCPUList(pickCPUWindow(allowed, count, window*count))
}

// jobContainerCPULimit translates the CPU budget into Docker host config
// values: a CFS quota and, where supported, an affinity window.
func jobContainerCPULimit(cpus int) (nanoCPUs int64, cpuset string) {
	if cpus <= 0 {
		return 0, ""
	}
	return int64(cpus) * 1_000_000_000, allocateJobCpuset(cpus)
}

// jobCPUEnvDefaults are environment variables through which common runtimes
// size their worker pools. They all default to the number of CPUs, so a test
// suite running in a container with host CPUs visible would start far more
// workers (and allocate far more memory) than the CPU budget allows.
var jobCPUEnvDefaults = []string{
	"GOMAXPROCS",
	"OMP_NUM_THREADS",
	"OPENBLAS_NUM_THREADS",
	"MKL_NUM_THREADS",
	"NUMEXPR_NUM_THREADS",
	"VECLIB_MAXIMUM_THREADS",
	"PYTEST_XDIST_AUTO_NUM_WORKERS",
	"PYTHON_CPU_COUNT",
}

// withJobCPUEnv adds the CPU budget to env, without overriding variables the
// caller already set.
func withJobCPUEnv(env []string, cpus int) []string {
	if cpus <= 0 {
		return env
	}
	value := strconv.Itoa(cpus)
	for _, name := range jobCPUEnvDefaults {
		if hasEnv(env, name) {
			continue
		}
		env = append(env, name+"="+value)
	}
	return env
}

func hasEnv(env []string, name string) bool {
	prefix := name + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			return true
		}
	}
	return false
}
