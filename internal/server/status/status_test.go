package status

import (
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startTestServer(t *testing.T, configFn func() interface{}) *Server {
	t.Helper()
	s := New(Config{
		Addr:     "127.0.0.1:0",
		ConfigFn: configFn,
	})
	err := s.Start()
	require.NoError(t, err)
	t.Cleanup(func() { s.Stop() })
	return s
}

func TestStatusServerStartStop(t *testing.T) {
	s := New(Config{Addr: "127.0.0.1:0"})
	err := s.Start()
	require.NoError(t, err)
	assert.NotEmpty(t, s.Addr())
	err = s.Stop()
	assert.NoError(t, err)
}

func TestStatusServerDoubleStart(t *testing.T) {
	s := New(Config{Addr: "127.0.0.1:0"})
	err := s.Start()
	require.NoError(t, err)
	defer s.Stop()

	err = s.Start()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already started")
}

func TestStopWithoutStart(t *testing.T) {
	s := New(Config{Addr: "127.0.0.1:0"})
	err := s.Stop()
	assert.NoError(t, err)
}

func TestHealthEndpoint(t *testing.T) {
	s := startTestServer(t, nil)

	resp, err := http.Get("http://" + s.Addr() + "/health")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]string
	err = json.NewDecoder(resp.Body).Decode(&body)
	require.NoError(t, err)
	assert.Equal(t, "ok", body["status"])
}

func TestStatusEndpoint(t *testing.T) {
	s := startTestServer(t, nil)

	resp, err := http.Get("http://" + s.Addr() + "/status")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var body map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&body)
	require.NoError(t, err)
	assert.Equal(t, "ok", body["status"])
	assert.Equal(t, "gookvs-dev", body["version"])
}

func TestConfigEndpoint(t *testing.T) {
	testCfg := map[string]interface{}{
		"log_level":  "info",
		"data_dir":   "/tmp/data",
		"listen_addr": "127.0.0.1:20160",
	}

	s := startTestServer(t, func() interface{} {
		return testCfg
	})

	resp, err := http.Get("http://" + s.Addr() + "/config")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Contains(t, resp.Header.Get("Content-Type"), "application/json")

	var body map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&body)
	require.NoError(t, err)
	assert.Equal(t, "info", body["log_level"])
	assert.Equal(t, "/tmp/data", body["data_dir"])
}

func TestConfigEndpointNoConfigFn(t *testing.T) {
	s := startTestServer(t, nil)

	resp, err := http.Get("http://" + s.Addr() + "/config")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
}

func TestConfigEndpointMethodNotAllowed(t *testing.T) {
	s := startTestServer(t, func() interface{} { return nil })

	resp, err := http.Post("http://"+s.Addr()+"/config", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func TestStatusEndpointMethodNotAllowed(t *testing.T) {
	s := startTestServer(t, nil)

	resp, err := http.Post("http://"+s.Addr()+"/status", "application/json", nil)
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func TestMetricsEndpoint(t *testing.T) {
	s := startTestServer(t, nil)

	resp, err := http.Get("http://" + s.Addr() + "/metrics")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	// Prometheus metrics should contain at least go runtime metrics.
	assert.Contains(t, string(body), "go_")
}

func TestPprofEndpoint(t *testing.T) {
	s := startTestServer(t, nil)

	resp, err := http.Get("http://" + s.Addr() + "/debug/pprof/")
	require.NoError(t, err)
	defer resp.Body.Close()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestAddrBeforeStart(t *testing.T) {
	s := New(Config{Addr: "127.0.0.1:20180"})
	assert.Equal(t, "127.0.0.1:20180", s.Addr())
}

func TestServerTimeouts(t *testing.T) {
	s := New(Config{Addr: "127.0.0.1:0"})
	assert.Equal(t, 10*time.Second, s.httpServer.ReadTimeout)
	assert.Equal(t, 30*time.Second, s.httpServer.WriteTimeout)
	assert.Equal(t, 60*time.Second, s.httpServer.IdleTimeout)
}
