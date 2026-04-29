package slack

import "testing"

func TestNew_requiresToken(t *testing.T) {
	if _, err := New(Options{Channels: []string{"C1"}}); err == nil {
		t.Fatal("expected error when token missing")
	}
}

func TestNew_requiresChannels(t *testing.T) {
	if _, err := New(Options{Token: "xoxb-1"}); err == nil {
		t.Fatal("expected error when channels empty")
	}
}

func TestNew_ok(t *testing.T) {
	a, err := New(Options{Name: "ops", Token: "xoxb-1", Channels: []string{"C1"}})
	if err != nil {
		t.Fatal(err)
	}
	if a.Name() != "ops" {
		t.Errorf("name = %q, want ops", a.Name())
	}
}
