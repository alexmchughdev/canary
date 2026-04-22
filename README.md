# Canary

Slack heartbeat monitor. Detects when services go quiet.

Canary listens to Slack channels where services post status messages, learns each sender's normal cadence, and alerts when the cadence drifts or stops. Not a canary deployment tool — named for the idiom ("the canary's gone quiet").

See [canary-plan.md](canary-plan.md) for the full design doc. Note: the plan lists Python as the tech stack; this repository implements v0.1 in Go instead (CGO-free, single static binary, same Slack Socket Mode model).

## Build

    make build
    ./canary -config examples/canary.yaml

## Layout

    cmd/canary/        main binary
    internal/config/   YAML config loader
    internal/store/    SQLite persistence
    internal/detect/   rolling baseline + state machine
    internal/slackx/   Slack Socket Mode wrapper
    internal/metrics/  Prometheus /metrics
    deploy/            Dockerfile, K8s manifests, ArgoCD app
    examples/          sample canary.yaml

## Status

v0.1 MVP — see `canary-plan.md` §Scope.
