# Foghorn

Slack heartbeat monitor. Detects when services go quiet.

Foghorn listens to Slack channels where services post status messages, learns each sender's normal cadence, and alerts when the cadence drifts or stops. 

See [foghorn-plan.md](foghorn-plan.md) for the full design doc. Note: the plan lists Python as the tech stack; this repository implements the worker in Go instead (CGO-free, single static binary, same Slack Socket Mode model).

## Build

    make build
    ./foghorn -config examples/foghorn.yaml

## Layout

    cmd/foghorn/                 main binary
    internal/config/             YAML config loader
    internal/store/              SQLite persistence
    internal/detect/             rolling baseline + state machine
    internal/connector/          Connector interface
      slack/                     Slack Socket Mode implementation
    internal/metrics/            Prometheus /metrics
    deploy/                      Dockerfile, K8s manifests, ArgoCD app
    examples/                    sample foghorn.yaml
