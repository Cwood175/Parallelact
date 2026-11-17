package scheduler

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/sirupsen/logrus"

	"github.com/nektos/act/pkg/runner"
)

// LogStore keeps one log file per job instance below <data-dir>/runs/<id>/logs.
type LogStore struct {
	dataDir string
	stdout  bool

	mu    sync.Mutex
	files map[string]*os.File
}

func NewLogStore(dataDir string, stdout bool) *LogStore {
	return &LogStore{
		dataDir: dataDir,
		stdout:  stdout,
		files:   map[string]*os.File{},
	}
}

func (l *LogStore) RunLogPath(runID string) string {
	return filepath.Join(logsDir(l.dataDir, runID), "_scheduler.log")
}

func (l *LogStore) JobLogPath(runID, key string) string {
	return filepath.Join(logsDir(l.dataDir, runID), sanitizeKey(key)+".log")
}

// OpenJobLog opens (or reuses) the log file of a job instance. Log files are
// append only because a job log may be reopened by child jobs.
func (l *LogStore) OpenJobLog(runID, key string) (*os.File, error) {
	path := l.JobLogPath(runID, key)

	l.mu.Lock()
	defer l.mu.Unlock()
	if file, ok := l.files[path]; ok {
		return file, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, err
	}
	l.files[path] = file
	return file, nil
}

// RunLogger returns a logger writing to the run level scheduler log.
func (l *LogStore) RunLogger(runID string, fields logrus.Fields) (*logrus.Logger, error) {
	path := l.RunLogPath(runID)

	l.mu.Lock()
	file, ok := l.files[path]
	if !ok {
		if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
			l.mu.Unlock()
			return nil, err
		}
		var err error
		file, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			l.mu.Unlock()
			return nil, err
		}
		l.files[path] = file
	}
	l.mu.Unlock()

	writers := []io.Writer{file}
	if l.stdout {
		writers = append(writers, os.Stdout)
	}
	logger := logrus.New()
	logger.SetOutput(io.MultiWriter(writers...))
	logger.SetLevel(logrus.GetLevel())
	logger.SetFormatter(&logrus.TextFormatter{
		DisableColors: true,
		FullTimestamp: true,
	})
	return logger, nil
}

// JobLog is the metadata of a job log file.
type JobLog struct {
	Key  string `json:"key"`
	Size int64  `json:"size"`
}

// ListJobLogs lists the log files of a run. keys maps a file key back to the job
// instance key it belongs to; child job logs are reported with their file key.
func (l *LogStore) ListJobLogs(runID string) ([]JobLog, error) {
	entries, err := os.ReadDir(logsDir(l.dataDir, runID))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	logs := make([]JobLog, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") || entry.Name() == "_scheduler.log" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			continue
		}
		logs = append(logs, JobLog{
			Key:  strings.TrimSuffix(entry.Name(), ".log"),
			Size: info.Size(),
		})
	}
	sort.Slice(logs, func(i, j int) bool { return logs[i].Key < logs[j].Key })
	return logs, nil
}

// Close closes all open log files.
func (l *LogStore) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var firstErr error
	for path, file := range l.files {
		if err := file.Close(); err != nil && firstErr == nil {
			firstErr = fmt.Errorf("failed to close %s: %w", path, err)
		}
		delete(l.files, path)
	}
	return firstErr
}

// jobLogFactory creates one logger per job instance; child jobs of reusable
// workflows get their own files through the named variant.
type jobLogFactory struct {
	store       *LogStore
	runID       string
	key         string
	jobID       string
	file        *os.File
	stdout      bool
	json        bool
	prefixJobID bool
}

func (f *jobLogFactory) WithJobLogger() *logrus.Logger {
	return f.newLogger(f.file)
}

// WithJobLoggerNamed is called for the job instance itself and for every child
// job of a reusable workflow. Child jobs get their own file.
func (f *jobLogFactory) WithJobLoggerNamed(jobID, jobName string) *logrus.Logger {
	if jobID == f.jobID {
		return f.newLogger(f.file)
	}

	key := sanitizeKey(f.key + "-" + jobID)
	if jobName != "" {
		key = sanitizeKey(key + "-" + strings.TrimSpace(jobName))
	}
	file, err := f.store.OpenJobLog(f.runID, key)
	if err != nil {
		return f.newLogger(f.file)
	}
	return f.newLogger(file)
}

func (f *jobLogFactory) newLogger(file *os.File) *logrus.Logger {
	writers := make([]io.Writer, 0, 2)
	if file != nil {
		writers = append(writers, file)
	}
	if f.stdout {
		writers = append(writers, os.Stdout)
	}

	logger := logrus.New()
	if len(writers) == 0 {
		logger.SetOutput(io.Discard)
	} else {
		logger.SetOutput(io.MultiWriter(writers...))
	}
	logger.SetLevel(logrus.GetLevel())
	if f.json {
		logger.SetFormatter(&logrus.JSONFormatter{})
	} else {
		logger.SetFormatter(runner.NewJobLogFormatter(runner.NextJobLogColor(), f.prefixJobID))
	}
	return logger
}
