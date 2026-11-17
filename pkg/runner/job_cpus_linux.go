//go:build linux

package runner

import (
	"os"
	"strings"
)

// allowedCPUs returns the CPUs this process is allowed to run on, as reported
// by the kernel. Docker only accepts CPU ids that exist on the host, so the
// scheduler's own affinity is a safe source for the window handed to a job.
func allowedCPUs() []int {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return nil
	}
	for _, line := range strings.Split(string(data), "\n") {
		value, ok := strings.CutPrefix(line, "Cpus_allowed_list:")
		if !ok {
			continue
		}
		return parseCPUList(value)
	}
	return nil
}
