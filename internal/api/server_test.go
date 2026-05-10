package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alexmchughdev/foghorn/internal/store"
)

const testToken = "test-token"

func newTestServer(t *testing.T, relearn RelearnFunc) (*Server, store.Store) {
	t.Helper()
	p := filepath.Join(t.TempDir(), "api.db")
	st, err := store.OpenSQLite(context.Background(), p)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	srv := New(st, relearn, ":0", testToken, "abc123")
	return srv, st
}

func do(t *testing.T, srv *Server, req *http.Request) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	srv.routes().ServeHTTP(rec, req)
	return rec
}

func TestHealthz_noAuth(t *testing.T) {
	srv, _ := newTestServer(t, nil)
	rec := do(t, srv, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("healthz: %d", rec.Code)
	}
	var body map[string]string
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["status"] != "ok" {
		t.Errorf("body: %v", body)
	}
}

func TestVersion_noAuth(t *testing.T) {
	srv, _ := newTestServer(t, nil)
	rec := do(t, srv, httptest.NewRequest(http.MethodGet, "/version", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("version: %d", rec.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body["build"] != "abc123" {
		t.Errorf("expected build=abc123, got %v", body["build"])
	}
	if _, ok := body["started_at"].(string); !ok {
		t.Errorf("started_at missing or not string: %v", body["started_at"])
	}
	// /version should not contain anything that looks like a SemVer label.
	if v, ok := body["version"]; ok {
		t.Errorf("unexpected version field: %v", v)
	}
}

func TestAuth_missingBearer(t *testing.T) {
	srv, _ := newTestServer(t, nil)
	for _, path := range []string{"/clusters", "/senders", "/alerts"} {
		rec := do(t, srv, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("%s: expected 401, got %d", path, rec.Code)
		}
	}
}

func TestAuth_wrongBearer(t *testing.T) {
	srv, _ := newTestServer(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/senders", nil)
	req.Header.Set("Authorization", "Bearer wrong")
	rec := do(t, srv, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 401, got %d", rec.Code)
	}
}

func TestAuth_correctBearer(t *testing.T) {
	srv, _ := newTestServer(t, nil)
	req := httptest.NewRequest(http.MethodGet, "/senders", nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	rec := do(t, srv, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d body=%s", rec.Code, rec.Body.String())
	}
}

func TestSenders_returnsRows(t *testing.T) {
	srv, st := newTestServer(t, nil)
	now := time.Now().Truncate(time.Second)
	if err := st.UpsertSender(context.Background(), &store.Sender{
		SenderID: "U1", ChannelID: "C1",
		FirstSeen: now, LastSeen: now,
		IntervalMean: 60, MsgCount: 5,
		State: store.StateHealthy, StateEnteredAt: now,
		BaselineReady: true,
	}); err != nil {
		t.Fatal(err)
	}
	rec := authed(t, srv, http.MethodGet, "/senders")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	var got []senderDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].SenderID != "U1" || got[0].State != "healthy" {
		t.Errorf("unexpected: %+v", got)
	}
}

func TestClusters_channelFilter(t *testing.T) {
	srv, st := newTestServer(t, nil)
	ctx := context.Background()
	if err := st.UpsertCluster(ctx, &store.Cluster{
		ChannelID: "C1", ClusterIndex: 0, Size: 4,
		SampleMessage: "deploy succeeded",
		Centroid:      map[string]float64{"deploy": 0.7},
		StableTokens:  []string{"deploy", "succeeded"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.UpsertCluster(ctx, &store.Cluster{
		ChannelID: "C2", ClusterIndex: 0, Size: 2,
		SampleMessage: "build green",
		Centroid:      map[string]float64{"build": 0.5},
		StableTokens:  []string{"build", "green"},
	}); err != nil {
		t.Fatal(err)
	}

	// Channel-scoped: only C1.
	rec := authed(t, srv, http.MethodGet, "/clusters?channel=C1")
	if rec.Code != http.StatusOK {
		t.Fatalf("status %d", rec.Code)
	}
	var got []clusterDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].ChannelID != "C1" {
		t.Errorf("channel filter: %+v", got)
	}

	// No filter: both.
	rec = authed(t, srv, http.MethodGet, "/clusters")
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Errorf("all: %+v", got)
	}
}

func TestClusters_noCentroidExposed(t *testing.T) {
	srv, st := newTestServer(t, nil)
	ctx := context.Background()
	if err := st.UpsertCluster(ctx, &store.Cluster{
		ChannelID: "C1", ClusterIndex: 0, Size: 4,
		SampleMessage: "deploy succeeded",
		Centroid:      map[string]float64{"deploy": 0.7, "succeeded": 0.6},
		StableTokens:  []string{"deploy", "succeeded"},
	}); err != nil {
		t.Fatal(err)
	}
	rec := authed(t, srv, http.MethodGet, "/clusters?channel=C1")
	body := rec.Body.String()
	if strings.Contains(body, "centroid") {
		t.Errorf("centroid should not be exposed: %s", body)
	}
}

func TestClusters_stableTokensTopN(t *testing.T) {
	srv, st := newTestServer(t, nil)
	ctx := context.Background()
	tokens := make([]string, 0, 15)
	for i := 'a'; i < 'a'+15; i++ {
		tokens = append(tokens, string(i))
	}
	if err := st.UpsertCluster(ctx, &store.Cluster{
		ChannelID: "C1", ClusterIndex: 0, Size: 4,
		SampleMessage: "x",
		Centroid:      map[string]float64{"a": 0.5},
		StableTokens:  tokens,
	}); err != nil {
		t.Fatal(err)
	}
	rec := authed(t, srv, http.MethodGet, "/clusters?channel=C1")
	var got []clusterDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || len(got[0].StableTokens) != stableTokenLimit {
		t.Errorf("expected %d tokens, got %d", stableTokenLimit, len(got[0].StableTokens))
	}
}

func TestAlerts_openFilter(t *testing.T) {
	srv, st := newTestServer(t, nil)
	ctx := context.Background()
	now := time.Now().Truncate(time.Second)

	if _, err := st.RaiseAlert(ctx, &store.Alert{
		Kind: store.AlertKindFrequency, SenderID: "U1", ChannelID: "C1",
		State: store.StateOffline, RaisedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.RaiseAlert(ctx, &store.Alert{
		Kind: store.AlertKindFrequency, SenderID: "U2", ChannelID: "C1",
		State: store.StateOffline, RaisedAt: now,
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.ClearOpenAlerts(ctx, "U2", "C1", now.Add(time.Minute)); err != nil {
		t.Fatal(err)
	}

	rec := authed(t, srv, http.MethodGet, "/alerts?state=open")
	var open []alertDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &open); err != nil {
		t.Fatal(err)
	}
	if len(open) != 1 || open[0].SenderID != "U1" {
		t.Errorf("open filter: %+v", open)
	}

	rec = authed(t, srv, http.MethodGet, "/alerts")
	var all []alertDTO
	if err := json.Unmarshal(rec.Body.Bytes(), &all); err != nil {
		t.Fatal(err)
	}
	if len(all) != 2 {
		t.Errorf("all: %+v", all)
	}
}

func TestRelearn_requiresChannel(t *testing.T) {
	srv, _ := newTestServer(t, func(context.Context, string) error { return nil })
	rec := authedPost(t, srv, "/relearn")
	if rec.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", rec.Code)
	}
}

func TestRelearn_invokesFunc(t *testing.T) {
	var got string
	srv, _ := newTestServer(t, func(_ context.Context, channelID string) error {
		got = channelID
		return nil
	})
	rec := authedPost(t, srv, "/relearn?channel=C42")
	if rec.Code != http.StatusOK {
		t.Errorf("status %d body=%s", rec.Code, rec.Body.String())
	}
	if got != "C42" {
		t.Errorf("channel passed: %q", got)
	}
}

func TestRelearn_propagatesError(t *testing.T) {
	srv, _ := newTestServer(t, func(context.Context, string) error {
		return errors.New("nope")
	})
	rec := authedPost(t, srv, "/relearn?channel=C1")
	if rec.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", rec.Code)
	}
}

func TestRelearn_rejectsGET(t *testing.T) {
	srv, _ := newTestServer(t, func(context.Context, string) error { return nil })
	rec := authed(t, srv, http.MethodGet, "/relearn?channel=C1")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

func authed(t *testing.T, srv *Server, method, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	return do(t, srv, req)
}

func authedPost(t *testing.T, srv *Server, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, nil)
	req.Header.Set("Authorization", "Bearer "+testToken)
	return do(t, srv, req)
}
