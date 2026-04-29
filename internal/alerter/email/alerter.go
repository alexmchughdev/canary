// Package email is the SMTP implementation of alerter.Alerter.
package email

import (
	"context"
	"errors"
	"fmt"
	"net/smtp"

	"github.com/alexmchughdev/foghorn/internal/alerter"
)

type Alerter struct {
	name string
	addr string
	auth smtp.Auth
	from string
	to   []string

	// sendFn is overridable so tests can intercept the SMTP call.
	sendFn func(addr string, auth smtp.Auth, from string, to []string, msg []byte) error
}

type Options struct {
	Name     string
	Host     string
	Port     int
	User     string
	Password string
	From     string
	To       []string
}

func New(opts Options) (*Alerter, error) {
	if opts.Host == "" || opts.Port == 0 {
		return nil, errors.New("email alerter: host and port required")
	}
	if opts.From == "" || len(opts.To) == 0 {
		return nil, errors.New("email alerter: from and at least one recipient required")
	}
	var auth smtp.Auth
	if opts.User != "" {
		auth = smtp.PlainAuth("", opts.User, opts.Password, opts.Host)
	}
	return &Alerter{
		name:   opts.Name,
		addr:   fmt.Sprintf("%s:%d", opts.Host, opts.Port),
		auth:   auth,
		from:   opts.From,
		to:     opts.To,
		sendFn: smtp.SendMail,
	}, nil
}

func (a *Alerter) Name() string { return a.name }

// Send blocks on smtp.SendMail in a goroutine and races it against ctx
// cancellation. SMTP TCP timeouts cap the worst case in seconds anyway.
func (a *Alerter) Send(ctx context.Context, alert alerter.Alert) error {
	msg := alerter.FormatEmail(alert, a.from, a.to)
	done := make(chan error, 1)
	go func() {
		done <- a.sendFn(a.addr, a.auth, a.from, a.to, msg)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}
