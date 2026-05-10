package api

import (
	"encoding/json"
	"net/http"
	"sort"
	"time"

	"github.com/alexmchughdev/foghorn/internal/store"
)

const stableTokenLimit = 10

// clusterDTO is the wire shape returned by /clusters. The full centroid
// vector is intentionally not exposed — it's an in-tree implementation
// detail and large enough to bloat responses.
type clusterDTO struct {
	ID             int64      `json:"id"`
	ChannelID      string     `json:"channel_id"`
	ClusterIndex   int        `json:"cluster_index"`
	Size           int        `json:"size"`
	SampleMessage  string     `json:"sample_message"`
	StableTokens   []string   `json:"stable_tokens"`
	LastMessageAt  *time.Time `json:"last_message_at,omitempty"`
	IntervalMean   float64    `json:"interval_mean_seconds"`
	IntervalStddev float64    `json:"interval_stddev_seconds"`
}

type senderDTO struct {
	SenderID       string    `json:"sender_id"`
	ChannelID      string    `json:"channel_id"`
	State          string    `json:"state"`
	FirstSeen      time.Time `json:"first_seen"`
	LastSeen       time.Time `json:"last_seen"`
	IntervalMean   float64   `json:"interval_mean_seconds"`
	IntervalStddev float64   `json:"interval_stddev_seconds"`
	MsgCount       int       `json:"msg_count"`
	BaselineReady  bool      `json:"baseline_ready"`
}

type alertDTO struct {
	ID                  int64      `json:"id"`
	Kind                string     `json:"kind"`
	SenderID            string     `json:"sender_id,omitempty"`
	ClusterID           *int64     `json:"cluster_id,omitempty"`
	ChannelID           string     `json:"channel_id"`
	State               string     `json:"state,omitempty"`
	RaisedAt            time.Time  `json:"raised_at"`
	ClearedAt           *time.Time `json:"cleared_at,omitempty"`
	LastIntervalSeconds float64    `json:"last_interval_seconds"`
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (s *Server) handleVersion(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"build":      s.sha,
		"started_at": s.started.Format(time.RFC3339),
	})
}

func (s *Server) handleClusters(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	channel := r.URL.Query().Get("channel")
	var (
		clusters []*store.Cluster
		err      error
	)
	if channel != "" {
		clusters, err = s.store.ListClusters(r.Context(), channel)
	} else {
		clusters, err = s.store.ListAllClusters(r.Context())
	}
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]clusterDTO, 0, len(clusters))
	for _, c := range clusters {
		out = append(out, projectCluster(c))
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleSenders(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	senders, err := s.store.ListSenders(r.Context())
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]senderDTO, 0, len(senders))
	for _, sn := range senders {
		out = append(out, senderDTO{
			SenderID:       sn.SenderID,
			ChannelID:      sn.ChannelID,
			State:          string(sn.State),
			FirstSeen:      sn.FirstSeen,
			LastSeen:       sn.LastSeen,
			IntervalMean:   sn.IntervalMean,
			IntervalStddev: sn.IntervalStddev,
			MsgCount:       sn.MsgCount,
			BaselineReady:  sn.BaselineReady,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleAlerts(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		writeJSONError(w, http.StatusMethodNotAllowed, "GET only")
		return
	}
	openOnly := r.URL.Query().Get("state") == "open"
	alerts, err := s.store.ListAlerts(r.Context(), openOnly)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	out := make([]alertDTO, 0, len(alerts))
	for _, a := range alerts {
		out = append(out, alertDTO{
			ID:                  a.ID,
			Kind:                a.Kind,
			SenderID:            a.SenderID,
			ClusterID:           a.ClusterID,
			ChannelID:           a.ChannelID,
			State:               string(a.State),
			RaisedAt:            a.RaisedAt,
			ClearedAt:           a.ClearedAt,
			LastIntervalSeconds: a.LastIntervalSeconds,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

func (s *Server) handleRelearn(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "POST only")
		return
	}
	channel := r.URL.Query().Get("channel")
	if channel == "" {
		writeJSONError(w, http.StatusBadRequest, "channel query parameter required")
		return
	}
	if s.relearn == nil {
		writeJSONError(w, http.StatusServiceUnavailable, "relearn not configured")
		return
	}
	if err := s.relearn(r.Context(), channel); err != nil {
		writeJSONError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok", "channel": channel})
}

// projectCluster trims a stored cluster to the public DTO shape: top-N
// stable tokens, no centroid vector.
func projectCluster(c *store.Cluster) clusterDTO {
	tokens := c.StableTokens
	if len(tokens) > stableTokenLimit {
		// StableTokens has no inherent ordering guarantee from the cluster
		// builder, so sort deterministically before truncating.
		sorted := make([]string, len(tokens))
		copy(sorted, tokens)
		sort.Strings(sorted)
		tokens = sorted[:stableTokenLimit]
	}
	return clusterDTO{
		ID:             c.ID,
		ChannelID:      c.ChannelID,
		ClusterIndex:   c.ClusterIndex,
		Size:           c.Size,
		SampleMessage:  c.SampleMessage,
		StableTokens:   tokens,
		LastMessageAt:  c.LastMessageAt,
		IntervalMean:   c.IntervalMean,
		IntervalStddev: c.IntervalStddev,
	}
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
