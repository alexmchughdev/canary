package alerter

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
)

// Multi fans an Alert out to all configured sinks concurrently. A
// failing sink does not block or short-circuit the others; errors are
// joined and returned.
type Multi struct {
	sinks []Alerter
	log   *slog.Logger
}

func NewMulti(log *slog.Logger, sinks ...Alerter) *Multi {
	if log == nil {
		log = slog.Default()
	}
	return &Multi{sinks: sinks, log: log}
}

func (m *Multi) Name() string { return "multi" }

func (m *Multi) Sinks() []Alerter { return m.sinks }

func (m *Multi) Send(ctx context.Context, a Alert) error {
	if len(m.sinks) == 0 {
		return nil
	}
	var wg sync.WaitGroup
	errs := make([]error, len(m.sinks))
	for i, s := range m.sinks {
		wg.Add(1)
		go func(i int, s Alerter) {
			defer wg.Done()
			if err := s.Send(ctx, a); err != nil {
				m.log.Error("alerter send failed", "sink", s.Name(), "err", err)
				errs[i] = fmt.Errorf("%s: %w", s.Name(), err)
			}
		}(i, s)
	}
	wg.Wait()
	return errors.Join(errs...)
}
