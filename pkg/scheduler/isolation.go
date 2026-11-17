package scheduler

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync/atomic"
	"time"
)

const defaultWorkflowsPath = ".github/workflows/"

var unsafeNameChars = regexp.MustCompile(`[^a-zA-Z0-9._-]+`)

// sanitizeKey makes a string usable as a file name and as part of a container
// name, while keeping it readable.
func sanitizeKey(in string) string {
	sanitized := unsafeNameChars.ReplaceAllString(in, "-")
	sanitized = strings.Trim(sanitized, "-.")
	if sanitized == "" {
		return "_"
	}
	if len(sanitized) > 80 {
		sanitized = sanitized[:80]
	}
	return sanitized
}

// workflowsPath returns the absolute workflows path of a request.
func workflowsPath(req *RunRequest) string {
	if req.WorkflowsPath == "" {
		return filepath.Join(req.Workdir, filepath.FromSlash(defaultWorkflowsPath))
	}
	return req.WorkflowsPath
}

var runCounter atomic.Uint64

// newRunID returns a unique, schedulable identifier for a run.
func newRunID() string {
	random := make([]byte, 4)
	if _, err := rand.Read(random); err != nil {
		// crypto/rand does not fail in practice; keep the ID valid anyway.
		return fmt.Sprintf("r%d", time.Now().UnixNano())
	}
	return fmt.Sprintf("r%s%s", time.Now().UTC().Format("20060102T150405"), hex.EncodeToString(random))
}

var githubRunIDGenerator atomic.Int64

// nextGitHubRunID hands out a process unique numeric run id. Artifact URLs and
// the runtime token are scoped by it, so concurrent runs must never share one.
func nextGitHubRunID() int64 {
	if v := githubRunIDGenerator.Load(); v != 0 {
		return githubRunIDGenerator.Add(1)
	}
	githubRunIDGenerator.CompareAndSwap(0, time.Now().UnixMilli())
	return githubRunIDGenerator.Add(1)
}

// validateWorkdir resolves the workdir and makes sure it is an existing
// directory below one of the allowed roots.
func validateWorkdir(workdir string, allowedRoots []string) (string, error) {
	if workdir == "" {
		return "", fmt.Errorf("workdir is required")
	}
	if !filepath.IsAbs(workdir) {
		return "", fmt.Errorf("workdir must be an absolute path")
	}
	resolved, err := filepath.EvalSymlinks(workdir)
	if err != nil {
		return "", fmt.Errorf("invalid workdir %q: %w", workdir, err)
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", fmt.Errorf("invalid workdir %q: %w", workdir, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("workdir %q is not a directory", workdir)
	}
	if len(allowedRoots) == 0 {
		return "", fmt.Errorf("no allowed workdir roots configured")
	}
	for _, root := range allowedRoots {
		rootResolved, err := filepath.EvalSymlinks(root)
		if err != nil {
			continue
		}
		rel, err := filepath.Rel(rootResolved, resolved)
		if err != nil {
			continue
		}
		if rel == "." || filepath.IsLocal(rel) {
			return resolved, nil
		}
	}
	return "", fmt.Errorf("workdir %q is outside the allowed roots", workdir)
}

// resolveWorkflowsPath validates the workflows path and returns its absolute
// form. It must stay inside the workdir.
func resolveWorkflowsPath(workdir, workflows string) (string, error) {
	if workflows == "" {
		workflows = defaultWorkflowsPath
	}
	var absolute string
	if filepath.IsAbs(workflows) {
		absolute = workflows
	} else {
		absolute = filepath.Join(workdir, filepath.FromSlash(workflows))
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err != nil {
		return "", fmt.Errorf("invalid workflows path %q: %w", workflows, err)
	}
	rel, err := filepath.Rel(workdir, resolved)
	if err != nil || (!filepath.IsLocal(rel) && rel != ".") {
		return "", fmt.Errorf("workflows path %q is outside the workdir", workflows)
	}
	return resolved, nil
}

// runDir is the per run directory below the data dir.
func runDir(dataDir, runID string) string {
	return filepath.Join(dataDir, "runs", sanitizeKey(runID))
}

func logsDir(dataDir, runID string) string {
	return filepath.Join(runDir(dataDir, runID), "logs")
}
