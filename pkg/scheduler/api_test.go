package scheduler

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/nektos/act/pkg/common"
)

func newTestAPI(t *testing.T) (*httptest.Server, *Scheduler, string) {
	t.Helper()

	s, root := newTestScheduler(t, Config{MaxContainers: 2})
	s.SetEngine(staticEngine(stageWith("one", 1, testUnit("wf/a/0", "a", func(ctx context.Context) error {
		common.Logger(ctx).Infof("job output")
		return nil
	}))))

	server := httptest.NewServer(s.Handler("secret-token"))
	t.Cleanup(server.Close)
	return server, s, root
}

func doRequest(t *testing.T, server *httptest.Server, method, path, token string, body []byte) (*http.Response, []byte) {
	t.Helper()
	request, err := http.NewRequest(method, server.URL+path, bytes.NewReader(body))
	require.NoError(t, err)
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	require.NoError(t, err)
	defer response.Body.Close()
	content, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	return response, content
}

func TestAPIAuth(t *testing.T) {
	server, _, _ := newTestAPI(t)

	response, _ := doRequest(t, server, http.MethodGet, "/v1/stats", "", nil)
	assert.Equal(t, http.StatusUnauthorized, response.StatusCode)

	response, _ = doRequest(t, server, http.MethodGet, "/v1/stats", "wrong", nil)
	assert.Equal(t, http.StatusUnauthorized, response.StatusCode)

	response, _ = doRequest(t, server, http.MethodGet, "/v1/stats", "secret-token", nil)
	assert.Equal(t, http.StatusOK, response.StatusCode)

	response, _ = doRequest(t, server, http.MethodGet, "/healthz", "", nil)
	assert.Equal(t, http.StatusOK, response.StatusCode)
}

func TestAPISubmitAndInspect(t *testing.T) {
	server, s, root := newTestAPI(t)

	payload, err := json.Marshal(RunRequest{Workdir: root, Repo: "org/repo", Commit: "abc", EventName: "push"})
	require.NoError(t, err)

	response, content := doRequest(t, server, http.MethodPost, "/v1/runs", "secret-token", payload)
	require.Equal(t, http.StatusAccepted, response.StatusCode, string(content))

	var created RunState
	require.NoError(t, json.Unmarshal(content, &created))
	require.NotEmpty(t, created.ID)

	finished := waitRun(t, s, created.ID)
	require.Equal(t, StatusSuccess, finished.Status)

	response, content = doRequest(t, server, http.MethodGet, "/v1/runs/"+created.ID, "secret-token", nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	var run RunState
	require.NoError(t, json.Unmarshal(content, &run))
	assert.Equal(t, StatusSuccess, run.Status)
	require.Len(t, run.Jobs, 1)

	response, content = doRequest(t, server, http.MethodGet, "/v1/runs", "secret-token", nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	assert.Contains(t, string(content), created.ID)

	response, content = doRequest(t, server, http.MethodGet, "/v1/runs/"+created.ID+"/logs", "secret-token", nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	assert.Contains(t, string(content), "wf-a-0")

	response, content = doRequest(t, server, http.MethodGet, "/v1/runs/"+created.ID+"/logs?job="+run.Jobs[0].Key, "secret-token", nil)
	require.Equal(t, http.StatusOK, response.StatusCode)
	assert.Contains(t, string(content), "job output")

	response, _ = doRequest(t, server, http.MethodGet, "/v1/runs/unknown", "secret-token", nil)
	assert.Equal(t, http.StatusNotFound, response.StatusCode)

	response, _ = doRequest(t, server, http.MethodPost, "/v1/runs/unknown/cancel", "secret-token", nil)
	assert.Equal(t, http.StatusNotFound, response.StatusCode)
}

func TestAPIRejectsBadRequests(t *testing.T) {
	server, _, root := newTestAPI(t)

	response, _ := doRequest(t, server, http.MethodPost, "/v1/runs", "secret-token", []byte("{not json"))
	assert.Equal(t, http.StatusBadRequest, response.StatusCode)

	response, _ = doRequest(t, server, http.MethodPost, "/v1/runs", "secret-token", []byte(`{"workdir":"/nope"}`))
	assert.Equal(t, http.StatusBadRequest, response.StatusCode)

	payload, err := json.Marshal(map[string]any{"workdir": root, "unknownField": 1})
	require.NoError(t, err)
	response, _ = doRequest(t, server, http.MethodPost, "/v1/runs", "secret-token", payload)
	assert.Equal(t, http.StatusBadRequest, response.StatusCode)
}

func TestAPILogFollow(t *testing.T) {
	server, s, root := newTestAPI(t)

	payload, err := json.Marshal(RunRequest{Workdir: root, EventName: "push"})
	require.NoError(t, err)
	response, content := doRequest(t, server, http.MethodPost, "/v1/runs", "secret-token", payload)
	require.Equal(t, http.StatusAccepted, response.StatusCode)

	var created RunState
	require.NoError(t, json.Unmarshal(content, &created))
	run := waitRun(t, s, created.ID)
	require.Equal(t, StatusSuccess, run.Status)

	// Following a finished run must return immediately instead of hanging.
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, body := doRequest(t, server, http.MethodGet,
			"/v1/runs/"+created.ID+"/logs?follow=1&job="+run.Jobs[0].Key, "secret-token", nil)
		assert.Contains(t, string(body), "job output")
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("following a finished log did not return")
	}
}
