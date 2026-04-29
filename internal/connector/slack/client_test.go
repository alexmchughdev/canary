package slack

import (
	"testing"
	"time"

	"github.com/alexmchughdev/foghorn/internal/connector"
)

func TestParseSlackTS(t *testing.T) {
	got, err := parseSlackTS("1713790000.000123")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	want := time.Unix(1713790000, 123000).UTC()
	if !got.Equal(want) {
		t.Errorf("ts = %v, want %v", got, want)
	}
	if got.Location() != time.UTC {
		t.Errorf("ts not UTC: %v", got.Location())
	}
}

func TestParseSlackTS_invalid(t *testing.T) {
	if _, err := parseSlackTS("not-a-timestamp"); err == nil {
		t.Fatal("expected error on garbage input")
	}
}

func TestNew_requiresTokens(t *testing.T) {
	if _, err := New(Options{}); err == nil {
		t.Fatal("expected error when tokens missing")
	}
	if _, err := New(Options{AppToken: "xapp-1"}); err == nil {
		t.Fatal("expected error when bot token missing")
	}
	if _, err := New(Options{BotToken: "xoxb-1"}); err == nil {
		t.Fatal("expected error when app token missing")
	}
}

// Exercises the in-place sort History applies after merging
// conversations.history results across channels.
func TestSortChronological(t *testing.T) {
	t0 := time.Unix(1700000000, 0).UTC()
	in := []connector.Message{
		{ChannelID: "C2", Timestamp: t0.Add(30 * time.Second)},
		{ChannelID: "C1", Timestamp: t0.Add(10 * time.Second)},
		{ChannelID: "C2", Timestamp: t0.Add(5 * time.Second)},
		{ChannelID: "C1", Timestamp: t0.Add(20 * time.Second)},
	}
	sortChronological(in)
	for i := 1; i < len(in); i++ {
		if in[i-1].Timestamp.After(in[i].Timestamp) {
			t.Fatalf("not chronological at %d: %v then %v", i, in[i-1].Timestamp, in[i].Timestamp)
		}
	}
}

func TestSortChronological_empty(t *testing.T) {
	sortChronological(nil)
	sortChronological([]connector.Message{})
}

func TestClient_NameAndPlatform(t *testing.T) {
	c, err := New(Options{
		Name:     "prod-slack",
		AppToken: "xapp-1",
		BotToken: "xoxb-1",
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if got := c.Name(); got != "prod-slack" {
		t.Errorf("Name = %q, want %q", got, "prod-slack")
	}
	if got := c.Platform(); got != "slack" {
		t.Errorf("Platform = %q, want %q", got, "slack")
	}
}
