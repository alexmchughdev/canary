package email

import (
	"context"
	"errors"
	"net/smtp"
	"strings"
	"testing"
	"time"

	"github.com/alexmchughdev/foghorn/internal/alerter"
)

func TestNew_requiresHostAndPort(t *testing.T) {
	if _, err := New(Options{From: "x@y", To: []string{"a@b"}}); err == nil {
		t.Fatal("expected error when host/port missing")
	}
}

func TestNew_requiresFromAndTo(t *testing.T) {
	if _, err := New(Options{Host: "smtp.example", Port: 25}); err == nil {
		t.Fatal("expected error when from/to missing")
	}
}

func TestSend_capturesMessage(t *testing.T) {
	a, err := New(Options{
		Name: "ops", Host: "smtp.example", Port: 25,
		From: "foghorn@example.com", To: []string{"ops@example.com"},
	})
	if err != nil {
		t.Fatal(err)
	}
	var captured []byte
	a.sendFn = func(_ string, _ smtp.Auth, _ string, _ []string, msg []byte) error {
		captured = msg
		return nil
	}
	err = a.Send(context.Background(), alerter.Alert{
		Severity: alerter.SeverityCritical,
		Title:    "deploys offline",
		Body:     "silent for 12m",
		Kind:     "frequency",
	})
	if err != nil {
		t.Fatal(err)
	}
	got := string(captured)
	if !strings.Contains(got, "Subject: [Foghorn] CRITICAL: deploys offline") {
		t.Errorf("subject missing in email: %q", got)
	}
	if !strings.Contains(got, "silent for 12m") {
		t.Errorf("body missing in email: %q", got)
	}
}

func TestSend_ctxCancellationWins(t *testing.T) {
	a, _ := New(Options{
		Name: "ops", Host: "smtp.example", Port: 25,
		From: "foghorn@example.com", To: []string{"ops@example.com"},
	})
	a.sendFn = func(_ string, _ smtp.Auth, _ string, _ []string, _ []byte) error {
		time.Sleep(2 * time.Second)
		return errors.New("should not reach")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := a.Send(ctx, alerter.Alert{Title: "x"})
	if err == nil {
		t.Fatal("expected ctx error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("got %v, want DeadlineExceeded", err)
	}
}
