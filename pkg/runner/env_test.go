package runner

import "testing"

func TestAppendNetworkEnv(t *testing.T) {
	env := []string{"HTTP_PROXY=old", "PATH=/bin"}
	got := appendNetworkEnv(env, map[string]string{
		"HTTP_PROXY": "new", "HTTPS_PROXY": "https://proxy",
		"NODE_USE_ENV_PROXY": "1", "GH_TOKEN": "secret", "PATH": "/other",
	})
	joined := ""
	for _, x := range got {
		joined += x + "\n"
	}
	for _, want := range []string{"HTTP_PROXY=old", "HTTPS_PROXY=https://proxy", "NODE_USE_ENV_PROXY=1", "PATH=/bin"} {
		if !containsLine(joined, want) {
			t.Fatalf("missing %q in %q", want, joined)
		}
	}
	if containsLine(joined, "GH_TOKEN=secret") || containsLine(joined, "PATH=/other") {
		t.Fatalf("unexpected override in %q", joined)
	}
}

func containsLine(s, line string) bool {
	for _, x := range splitLines(s) {
		if x == line {
			return true
		}
	}
	return false
}
func splitLines(s string) []string {
	var out []string
	start := 0
	for i, c := range s {
		if c == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	if start < len(s) {
		out = append(out, s[start:])
	}
	return out
}
