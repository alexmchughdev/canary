# Operations

Notes for running Foghorn against a real workspace: known issues, tuning guidance, and behavioural quirks worth understanding before they show up in production.

## Known issues

### Frequency alert `cleared_at` can precede `raised_at`

When a previous boot raised a frequency alert and the next boot's backfill processes an older message that drives the affected sender from `offline` to `recovering`, the open alert gets cleared with the backfilled message's Slack timestamp. That timestamp is older than the original raise, so the persisted row ends up with `cleared_at < raised_at`.

Cause: `applyDecision` in `internal/app/app.go` passes `m.Timestamp` (the message's Slack timestamp) into `ClearOpenAlerts` rather than wall-clock time. The state transition is correct; only the persisted clear timestamp is misleading.

Impact: cosmetic. The alert is in the right state (cleared) and won't fire again until silence accrues. Reporting that displays "alert duration = cleared_at minus raised_at" will return a negative value.

Fix shape: clear with `time.Now()` at boot, or refuse to set `cleared_at` earlier than the original `raised_at`.

### Content alerts don't auto-clear across boots

`missing_pattern`, `unknown_pattern`, and `abnormal_content` rows have no lifecycle hook that clears them when the underlying condition resolves. Once raised they stay open until either:

- `POST /relearn?channel=C123` runs, which calls `ClearOpenClusterAlertsByChannel` as part of rebuilding the channel's clusters, or
- An operator updates the rows directly.

This matters because `HasOpenClusterAlert` short-circuits any new `missing_pattern` raise while an alert is already open against the same cluster row. A `missing_pattern` from a previous boot can suppress the same kind from firing again this boot, even after a long silence, until the row is cleared.

Workaround: `POST /relearn?channel=...` (clears open cluster alerts for that channel, then rebuilds), or manual `UPDATE alerts SET cleared_at = ...` against the SQLite store.

## Tuning

### Synthetic corpora are a poor substrate

A dense synthetic corpus with tight token vocabularies amplifies structural-token swaps. Replacing `succeeded` with `FAILED` in a small corpus can drop cosine similarity below `cluster.match_threshold`, producing `unknown_pattern` where an operator might expect `abnormal_content`. Both kinds raise alerts, so the operational outcome is the same, but the kind label diverges from intuition.

Tune thresholds against production data after observing real traffic. The defaults are starting points, not recommendations.

### Knob reference

| Field | Notes |
| --- | --- |
| `learning.lookback` | How far back to scan history at boot. Too short, clusters don't form. Too long, slow boot. Start at `7d`, adjust based on channel volume. |
| `cluster.epsilon` | DBSCAN distance threshold. Lower means tighter, more clusters. Default `0.4` is conservative. |
| `cluster.min_pts` | DBSCAN minimum cluster size. Default `3`. Raise if backfill is large enough that noise messages keep forming spurious clusters. |
| `cluster.match_threshold` | Cosine threshold for a live message to count as matching a cluster. Below this, the message is `unknown_pattern`. Default `0.5`. |
| `cluster.stable_ratio` | Fraction of a cluster's stable tokens that must be present in a matched message to count as `known` rather than `abnormal_content`. Default `0.8`. |
| `detection.drift_sigma` | Sigma threshold for sender cadence to count as drifting. Default `3`. |
| `detection.offline_multiplier` | Multiplier on baseline mean for sender or cluster to count as offline. Default `5`. |
| `detection.hard_cap` | Maximum silence threshold for offline detection. Default `30m`. |
| `alerts.cooldown_per_kind` | How long after a content alert before the same `(channel, kind)` can fire again. Default `15m`. Lower for tighter response, higher to avoid storms. |

### Cluster cadence biases under tight synthetic data

If the backfill window captures a burst of messages with very tight inter-arrival times (e.g. a synth generator posting every 1-2 seconds), the resulting cluster's `interval_mean` ends up low and `offline_multiplier * mean` falls below the 30-second tick interval. Every tick then re-evaluates cluster silence and can produce `missing_pattern` plus surrounding `frequency` flap on per-sender state. This is correct behaviour given the configured values; the synthetic data is the problem, not the detector.

## Behavioural notes

### Slack Socket Mode redelivers buffered events

When the bot reconnects after a disconnect (planned restart, network blip, crash recovery), Slack delivers events that were buffered while the WebSocket was down. Some of those events overlap with what `History` already returned during boot backfill, so the same physical message can arrive twice within one boot.

A per-channel watermark recorded during `backfillHistory` drops the duplicates before they reach `onMessage`. The watermark check fires in `loop` at `internal/app/app.go`. Observable via:

```
foghorn_messages_skipped_total{channel="...", reason="backfill_overlap"}
```

A non-zero counter on this metric is normal after restarts and indicates the deduplication is working as intended. A *zero* counter on every restart suggests either Slack isn't redelivering for some reason, or the watermark logic isn't running for the channels in question.

### Each boot rebuilds clusters from scratch

`learnClusters` runs every boot, regenerating cluster fingerprints from the current lookback window. Cluster row primary keys survive because `UpsertCluster` uses `ON CONFLICT(channel_id, cluster_index) DO UPDATE`, so foreign references in the `alerts` table stay valid across boots, but the in-memory state (classifier, baselines) is fresh each time.

A side-effect is that `senders.msg_count` accumulates across boots: the same backfilled message is ingested once per boot. This is by design and only affects the counter; nothing in detection logic uses `msg_count` for thresholds.

### Verifying bot channel membership

Slack's default channel Members panel hides app and bot members from the user-facing view, so the bot can appear absent from a channel it's actually in. To verify whether the Foghorn bot is a member of a given channel, open the channel's settings and use the **Integrations** tab — that's the authoritative UI view for bot memberships. The API equivalents are `users.conversations` (returns only memberships) or `conversations.list` with the `is_member` field; both will agree with Integrations. Don't take a missing entry in the default Members panel as evidence the bot isn't joined.

### State machine flap on synthetic data

The frequency detector ticks every 30 seconds (constant `tickInterval` in `internal/app/app.go`). If a sender's baseline mean inter-arrival is much smaller than 30 seconds (typical for a synth generator), every tick finds the sender silent past the offline threshold and produces a state transition. The result is a healthy → offline → recovering → healthy flap visible in logs and Slack alerts.

Real workloads with service-status cadences in the seconds-to-minutes range don't trigger this. Mitigations if it shows up on real data:

- Raise `detection.offline_multiplier` so the offline threshold is well above one tick interval.
- Set `detection.hard_cap` high enough that genuinely-slow senders don't get clamped to a short offline threshold.

## Alert sinks

### PagerDuty severity allow-list

The PagerDuty sink filters trigger events by Foghorn severity. The default allow-list is `["critical"]` — only critical-severity alerts surface as PagerDuty incidents. Set `PAGERDUTY_SEVERITIES=critical,warning` to widen it, or `critical,warning,info` to capture every event (rarely a good idea on a human-paging integration).

Resolve events bypass the filter. If a `critical` frequency alert fires, the matching `info`-severity recovery still posts as `event_action: "resolve"` regardless of whether `info` is in the allow-list. This keeps PagerDuty incidents from dangling open after the underlying condition clears.

Dedup keys are derived from `connector`, `channel`, `kind`, and `sender_id`. Cluster-scoped `missing_pattern` alerts arrive without a sender id and currently fall back to a per-(channel, kind) key — multiple clusters in the same channel collapse to one PagerDuty incident, matching the existing per-(channel, kind) cooldown gate in the worker.

### Email (SMTP) gotchas

- Most providers require STARTTLS on port 587 with PLAIN auth. Port 465 (SMTPS, implicit TLS) is not supported by the current sink — use a 587/STARTTLS-capable relay.
- Gmail and Workspace require an **App Password**, not the account password, when 2FA is enabled. Generate one at <https://myaccount.google.com/apppasswords> and put it in `SMTP_PASSWORD`.
- The sink fans email out to every address in `SMTP_TO` in one envelope; recipient-specific failures fall back to a single error returned from `smtp.SendMail`. For large distribution lists, prefer a mailing-list address that handles fan-out at the MTA layer.

## Where to look first when something breaks

| Symptom | Where to look |
| --- | --- |
| No clusters formed for a channel | `clusters_learned messages=N clusters=0` in boot log. If N is small or zero, lookback or `min_pts` is wrong. If N is reasonable, `epsilon` is too low. |
| Alerts not landing in `#alerts` | Check `foghorn_alerts_raised_total` and the alert sink configuration. Then check the slack-alerter and email-alerter logs at WARN level for delivery errors. |
| Many `unknown_pattern` alerts on a channel that should be settled | `match_threshold` may be too high for this channel's corpus. Try `POST /relearn?channel=...` first to rule out a stale cluster. |
| API returns 401 | `FOGHORN_API_TOKEN` env var unset, or wrong token in `Authorization: Bearer ...`. Foghorn refuses to start if the env var is empty, so the API only ever serves with a populated token. |
| `backfill_overlap` counter climbing forever | Means Slack is redelivering events well past the expected catch-up window. Worth investigating connection stability. |

## Known verification gaps

The scope-mismatch error path in `ValidateAccess` is covered by unit tests but has not been exercised against a live Slack workspace. The unit tests pin `RequiredScopes` against `slack/manifest.yaml`, so the mismatch case is structurally tested. Verifying the live error message would require rotating the bot token twice (once to remove a scope, once to restore), which is not yet automated.
