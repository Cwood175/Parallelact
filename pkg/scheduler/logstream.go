package scheduler

import (
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

// ServeLog streams a job log from offset. With follow it keeps the connection
// open until the run finished (and the file reached EOF) or the client leaves.
func ServeLog(w http.ResponseWriter, r *http.Request, path string, offset int64, follow bool, store *Store, runID string) error {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("%w: log file not found", errRunNotFound)
		}
		return err
	}
	defer file.Close()

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return err
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(http.StatusOK)
	flusher, _ := w.(http.Flusher)
	if flusher != nil {
		flusher.Flush()
	}

	buf := make([]byte, 32*1024)
	for {
		n, readErr := file.Read(buf)
		if n > 0 {
			if _, writeErr := w.Write(buf[:n]); writeErr != nil {
				return nil
			}
			if flusher != nil {
				flusher.Flush()
			}
		}

		if readErr == nil {
			continue
		}
		if !errors.Is(readErr, io.EOF) {
			return nil
		}
		if !follow || runFinished(store, runID) {
			return nil
		}

		select {
		case <-r.Context().Done():
			return nil
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func runFinished(store *Store, runID string) bool {
	run, ok := store.Get(runID)
	return ok && run.Status.Terminal()
}
