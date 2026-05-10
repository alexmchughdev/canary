package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

const baseSchema = `
CREATE TABLE IF NOT EXISTS senders (
    sender_id         TEXT    NOT NULL,
    channel_id        TEXT    NOT NULL,
    first_seen        INTEGER NOT NULL,
    last_seen         INTEGER NOT NULL,
    interval_mean     REAL    NOT NULL DEFAULT 0,
    interval_stddev   REAL    NOT NULL DEFAULT 0,
    msg_count         INTEGER NOT NULL DEFAULT 0,
    state             TEXT    NOT NULL,
    state_entered_at  INTEGER NOT NULL,
    baseline_ready    INTEGER NOT NULL DEFAULT 0,
    muted_until       INTEGER,
    PRIMARY KEY (sender_id, channel_id)
);

CREATE TABLE IF NOT EXISTS clusters (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    channel_id          TEXT    NOT NULL,
    cluster_index       INTEGER NOT NULL,
    size                INTEGER NOT NULL,
    sample_message      TEXT    NOT NULL,
    centroid_json       TEXT    NOT NULL,
    stable_tokens_json  TEXT    NOT NULL,
    last_message_at     INTEGER,
    interval_mean       REAL,
    interval_stddev     REAL,
    UNIQUE(channel_id, cluster_index)
);
CREATE INDEX IF NOT EXISTS idx_clusters_channel ON clusters(channel_id);

CREATE TABLE IF NOT EXISTS config_overrides (
    sender_id          TEXT PRIMARY KEY,
    interval_override  INTEGER,
    priority           TEXT,
    notes              TEXT
);
`

const alertsCreate = `
CREATE TABLE IF NOT EXISTS alerts (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    sender_id                TEXT,
    channel_id               TEXT    NOT NULL,
    state                    TEXT    NOT NULL,
    raised_at                INTEGER NOT NULL,
    cleared_at               INTEGER,
    last_interval_seconds    REAL    NOT NULL DEFAULT 0,
    kind                     TEXT    NOT NULL DEFAULT 'frequency',
    cluster_id               INTEGER,
    CHECK ((sender_id IS NOT NULL) <> (cluster_id IS NOT NULL))
);
CREATE INDEX IF NOT EXISTS idx_alerts_open
    ON alerts (sender_id, channel_id)
    WHERE cleared_at IS NULL AND sender_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_alerts_cluster_open
    ON alerts (cluster_id, channel_id)
    WHERE cleared_at IS NULL AND cluster_id IS NOT NULL;
`

type sqliteStore struct {
	db *sql.DB
}

func OpenSQLite(ctx context.Context, path string) (Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if err := migrate(ctx, db); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &sqliteStore{db: db}, nil
}

func migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, baseSchema); err != nil {
		return err
	}
	return migrateAlerts(ctx, db)
}

// migrateAlerts brings an existing alerts table up to the v2 shape
// (kind + cluster_id columns, sender_id nullable, mutual-exclusion CHECK).
// Fresh databases get the v2 shape directly. Idempotent.
func migrateAlerts(ctx context.Context, db *sql.DB) error {
	cols, err := tableColumns(ctx, db, "alerts")
	if err != nil {
		return err
	}
	if len(cols) == 0 {
		_, err := db.ExecContext(ctx, alertsCreate)
		return err
	}
	_, hasKind := cols["kind"]
	_, hasCluster := cols["cluster_id"]
	senderNotNull := cols["sender_id"]
	if hasKind && hasCluster && !senderNotNull {
		return nil
	}

	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	stmts := []string{
		`DROP INDEX IF EXISTS idx_alerts_open`,
		`ALTER TABLE alerts RENAME TO alerts_old`,
		alertsCreate,
		`INSERT INTO alerts (id, sender_id, channel_id, state, raised_at, cleared_at, last_interval_seconds, kind, cluster_id)
		 SELECT id, sender_id, channel_id, state, raised_at, cleared_at, last_interval_seconds, 'frequency', NULL
		 FROM alerts_old`,
		`DROP TABLE alerts_old`,
	}
	for _, q := range stmts {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("migrate alerts: %w", err)
		}
	}
	return tx.Commit()
}

// tableColumns returns column-name → notNull-flag for the named table.
// Empty map means the table doesn't exist.
func tableColumns(ctx context.Context, db *sql.DB, table string) (map[string]bool, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]bool{}
	for rows.Next() {
		var (
			cid     int
			name    string
			ctype   string
			notnull int
			dflt    sql.NullString
			pk      int
		)
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return nil, err
		}
		out[name] = notnull != 0
	}
	return out, rows.Err()
}

func (s *sqliteStore) Close() error { return s.db.Close() }

func (s *sqliteStore) UpsertSender(ctx context.Context, sn *Sender) error {
	var muted *int64
	if sn.MutedUntil != nil {
		v := sn.MutedUntil.Unix()
		muted = &v
	}
	_, err := s.db.ExecContext(ctx, `
INSERT INTO senders (sender_id, channel_id, first_seen, last_seen, interval_mean,
                     interval_stddev, msg_count, state, state_entered_at,
                     baseline_ready, muted_until)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(sender_id, channel_id) DO UPDATE SET
    last_seen=excluded.last_seen,
    interval_mean=excluded.interval_mean,
    interval_stddev=excluded.interval_stddev,
    msg_count=excluded.msg_count,
    state=excluded.state,
    state_entered_at=excluded.state_entered_at,
    baseline_ready=excluded.baseline_ready,
    muted_until=excluded.muted_until`,
		sn.SenderID, sn.ChannelID,
		sn.FirstSeen.Unix(), sn.LastSeen.Unix(),
		sn.IntervalMean, sn.IntervalStddev, sn.MsgCount,
		string(sn.State), sn.StateEnteredAt.Unix(),
		boolInt(sn.BaselineReady), muted,
	)
	return err
}

func (s *sqliteStore) GetSender(ctx context.Context, senderID, channelID string) (*Sender, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT sender_id, channel_id, first_seen, last_seen, interval_mean, interval_stddev,
       msg_count, state, state_entered_at, baseline_ready, muted_until
FROM senders WHERE sender_id = ? AND channel_id = ?`, senderID, channelID)
	return scanSender(row.Scan)
}

func (s *sqliteStore) ListSenders(ctx context.Context) ([]*Sender, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT sender_id, channel_id, first_seen, last_seen, interval_mean, interval_stddev,
       msg_count, state, state_entered_at, baseline_ready, muted_until
FROM senders`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Sender
	for rows.Next() {
		sn, err := scanSender(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, sn)
	}
	return out, rows.Err()
}

func (s *sqliteStore) RaiseAlert(ctx context.Context, a *Alert) (int64, error) {
	kind := a.Kind
	if kind == "" {
		kind = AlertKindFrequency
	}
	res, err := s.db.ExecContext(ctx, `
INSERT INTO alerts (sender_id, channel_id, state, raised_at, last_interval_seconds, kind, cluster_id)
VALUES (?, ?, ?, ?, ?, ?, NULL)`,
		a.SenderID, a.ChannelID, string(a.State), a.RaisedAt.Unix(), a.LastIntervalSeconds, kind)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *sqliteStore) ClearOpenAlerts(ctx context.Context, senderID, channelID string, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE alerts SET cleared_at = ?
WHERE sender_id = ? AND channel_id = ? AND cleared_at IS NULL`,
		at.Unix(), senderID, channelID)
	return err
}

func (s *sqliteStore) HasOpenAlert(ctx context.Context, senderID, channelID string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `
SELECT COUNT(1) FROM alerts
WHERE sender_id = ? AND channel_id = ? AND cleared_at IS NULL`,
		senderID, channelID).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *sqliteStore) RaiseClusterAlert(ctx context.Context, a *Alert) (int64, error) {
	if a.ClusterID == nil {
		return 0, errors.New("RaiseClusterAlert: ClusterID required")
	}
	if a.Kind == "" {
		return 0, errors.New("RaiseClusterAlert: Kind required")
	}
	res, err := s.db.ExecContext(ctx, `
INSERT INTO alerts (sender_id, channel_id, state, raised_at, last_interval_seconds, kind, cluster_id)
VALUES (NULL, ?, ?, ?, ?, ?, ?)`,
		a.ChannelID, string(a.State), a.RaisedAt.Unix(), a.LastIntervalSeconds, a.Kind, *a.ClusterID)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (s *sqliteStore) ClearOpenClusterAlerts(ctx context.Context, channelID string, clusterID int64, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE alerts SET cleared_at = ?
WHERE channel_id = ? AND cluster_id = ? AND cleared_at IS NULL`,
		at.Unix(), channelID, clusterID)
	return err
}

func (s *sqliteStore) HasOpenClusterAlert(ctx context.Context, channelID string, clusterID int64) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `
SELECT COUNT(1) FROM alerts
WHERE channel_id = ? AND cluster_id = ? AND cleared_at IS NULL`,
		channelID, clusterID).Scan(&n)
	if err != nil {
		return false, err
	}
	return n > 0, nil
}

func (s *sqliteStore) UpsertCluster(ctx context.Context, c *Cluster) error {
	centroidJSON, err := json.Marshal(c.Centroid)
	if err != nil {
		return fmt.Errorf("marshal centroid: %w", err)
	}
	stableJSON, err := json.Marshal(c.StableTokens)
	if err != nil {
		return fmt.Errorf("marshal stable tokens: %w", err)
	}
	var lastUnix *int64
	if c.LastMessageAt != nil {
		v := c.LastMessageAt.Unix()
		lastUnix = &v
	}
	res, err := s.db.ExecContext(ctx, `
INSERT INTO clusters (channel_id, cluster_index, size, sample_message,
                      centroid_json, stable_tokens_json, last_message_at,
                      interval_mean, interval_stddev)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(channel_id, cluster_index) DO UPDATE SET
    size=excluded.size,
    sample_message=excluded.sample_message,
    centroid_json=excluded.centroid_json,
    stable_tokens_json=excluded.stable_tokens_json,
    last_message_at=excluded.last_message_at,
    interval_mean=excluded.interval_mean,
    interval_stddev=excluded.interval_stddev`,
		c.ChannelID, c.ClusterIndex, c.Size, c.SampleMessage,
		string(centroidJSON), string(stableJSON), lastUnix,
		c.IntervalMean, c.IntervalStddev)
	if err != nil {
		return err
	}
	if c.ID == 0 {
		c.ID, _ = res.LastInsertId()
	}
	return nil
}

func (s *sqliteStore) GetClusterByIndex(ctx context.Context, channelID string, clusterIndex int) (*Cluster, error) {
	row := s.db.QueryRowContext(ctx, `
SELECT id, channel_id, cluster_index, size, sample_message, centroid_json,
       stable_tokens_json, last_message_at, interval_mean, interval_stddev
FROM clusters WHERE channel_id = ? AND cluster_index = ?`, channelID, clusterIndex)
	return scanCluster(row.Scan)
}

func (s *sqliteStore) ListClusters(ctx context.Context, channelID string) ([]*Cluster, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, channel_id, cluster_index, size, sample_message, centroid_json,
       stable_tokens_json, last_message_at, interval_mean, interval_stddev
FROM clusters WHERE channel_id = ? ORDER BY cluster_index`, channelID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Cluster
	for rows.Next() {
		c, err := scanCluster(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *sqliteStore) ListAllClusters(ctx context.Context) ([]*Cluster, error) {
	rows, err := s.db.QueryContext(ctx, `
SELECT id, channel_id, cluster_index, size, sample_message, centroid_json,
       stable_tokens_json, last_message_at, interval_mean, interval_stddev
FROM clusters ORDER BY channel_id, cluster_index`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Cluster
	for rows.Next() {
		c, err := scanCluster(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

func (s *sqliteStore) DeleteClustersByChannel(ctx context.Context, channelID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM clusters WHERE channel_id = ?`, channelID)
	return err
}

func (s *sqliteStore) ClearOpenClusterAlertsByChannel(ctx context.Context, channelID string, at time.Time) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE alerts SET cleared_at = ?
WHERE channel_id = ? AND cluster_id IS NOT NULL AND cleared_at IS NULL`,
		at.Unix(), channelID)
	return err
}

func (s *sqliteStore) ListAlerts(ctx context.Context, openOnly bool) ([]*Alert, error) {
	q := `
SELECT id, sender_id, channel_id, state, raised_at, cleared_at,
       last_interval_seconds, kind, cluster_id
FROM alerts`
	if openOnly {
		q += ` WHERE cleared_at IS NULL`
	}
	q += ` ORDER BY raised_at DESC`
	rows, err := s.db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*Alert
	for rows.Next() {
		a, err := scanAlert(rows.Scan)
		if err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *sqliteStore) UpdateClusterStats(ctx context.Context, id int64, lastMessageAt time.Time, mean, stddev float64) error {
	_, err := s.db.ExecContext(ctx, `
UPDATE clusters SET last_message_at = ?, interval_mean = ?, interval_stddev = ?
WHERE id = ?`, lastMessageAt.Unix(), mean, stddev, id)
	return err
}

type scanFn func(dest ...any) error

func scanSender(scan scanFn) (*Sender, error) {
	var (
		s         Sender
		first     int64
		last      int64
		entered   int64
		ready     int
		mutedUnix sql.NullInt64
	)
	if err := scan(&s.SenderID, &s.ChannelID, &first, &last,
		&s.IntervalMean, &s.IntervalStddev, &s.MsgCount,
		(*string)(&s.State), &entered, &ready, &mutedUnix); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	s.FirstSeen = time.Unix(first, 0)
	s.LastSeen = time.Unix(last, 0)
	s.StateEnteredAt = time.Unix(entered, 0)
	s.BaselineReady = ready != 0
	if mutedUnix.Valid {
		t := time.Unix(mutedUnix.Int64, 0)
		s.MutedUntil = &t
	}
	return &s, nil
}

func scanAlert(scan scanFn) (*Alert, error) {
	var (
		a            Alert
		senderID     sql.NullString
		state        sql.NullString
		raised       int64
		cleared      sql.NullInt64
		clusterID    sql.NullInt64
	)
	if err := scan(&a.ID, &senderID, &a.ChannelID, &state, &raised, &cleared,
		&a.LastIntervalSeconds, &a.Kind, &clusterID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if senderID.Valid {
		a.SenderID = senderID.String
	}
	if state.Valid {
		a.State = SenderState(state.String)
	}
	a.RaisedAt = time.Unix(raised, 0)
	if cleared.Valid {
		t := time.Unix(cleared.Int64, 0)
		a.ClearedAt = &t
	}
	if clusterID.Valid {
		v := clusterID.Int64
		a.ClusterID = &v
	}
	return &a, nil
}

func scanCluster(scan scanFn) (*Cluster, error) {
	var (
		c           Cluster
		centroidStr string
		stableStr   string
		lastUnix    sql.NullInt64
		mean        sql.NullFloat64
		stddev      sql.NullFloat64
	)
	if err := scan(&c.ID, &c.ChannelID, &c.ClusterIndex, &c.Size, &c.SampleMessage,
		&centroidStr, &stableStr, &lastUnix, &mean, &stddev); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	if err := json.Unmarshal([]byte(centroidStr), &c.Centroid); err != nil {
		return nil, fmt.Errorf("unmarshal centroid: %w", err)
	}
	if err := json.Unmarshal([]byte(stableStr), &c.StableTokens); err != nil {
		return nil, fmt.Errorf("unmarshal stable tokens: %w", err)
	}
	if lastUnix.Valid {
		t := time.Unix(lastUnix.Int64, 0)
		c.LastMessageAt = &t
	}
	if mean.Valid {
		c.IntervalMean = mean.Float64
	}
	if stddev.Valid {
		c.IntervalStddev = stddev.Float64
	}
	return &c, nil
}

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
