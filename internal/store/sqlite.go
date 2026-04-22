package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

const schema = `
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

CREATE TABLE IF NOT EXISTS alerts (
    id                       INTEGER PRIMARY KEY AUTOINCREMENT,
    sender_id                TEXT    NOT NULL,
    channel_id               TEXT    NOT NULL,
    state                    TEXT    NOT NULL,
    raised_at                INTEGER NOT NULL,
    cleared_at               INTEGER,
    last_interval_seconds    REAL    NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_alerts_open
    ON alerts (sender_id, channel_id)
    WHERE cleared_at IS NULL;

CREATE TABLE IF NOT EXISTS config_overrides (
    sender_id          TEXT PRIMARY KEY,
    interval_override  INTEGER,
    priority           TEXT,
    notes              TEXT
);
`

type sqliteStore struct {
	db *sql.DB
}

func OpenSQLite(ctx context.Context, path string) (Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(on)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	if _, err := db.ExecContext(ctx, schema); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &sqliteStore{db: db}, nil
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
	res, err := s.db.ExecContext(ctx, `
INSERT INTO alerts (sender_id, channel_id, state, raised_at, last_interval_seconds)
VALUES (?, ?, ?, ?, ?)`,
		a.SenderID, a.ChannelID, string(a.State), a.RaisedAt.Unix(), a.LastIntervalSeconds)
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

func boolInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
