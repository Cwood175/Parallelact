//go:build !linux

package runner

// allowedCPUs is only meaningful on Linux. Elsewhere the docker daemon runs in
// a VM whose CPU ids are unrelated to this process, so no affinity window is
// set and only the CPU quota applies.
func allowedCPUs() []int {
	return nil
}
