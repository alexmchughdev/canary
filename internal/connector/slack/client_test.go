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
	if got := c.Monitored(); len(got) != 0 {
		t.Errorf("Monitored should be empty before Bootstrap, got %v", got)
	}
}

func TestLooksLikeChannelID(t *testing.T) {
	cases := map[string]bool{
		"C0B3Q17FZ2L":  true,  // real public channel id
		"G01234ABCDE":  true,  // real private channel id
		"C12345678":    true,  // 9-char threshold
		"#deploys":     false, // leading hash is a name marker
		"deploys":      false, // lowercase name
		"Cdeploys":     false, // lowercase tail
		"":             false,
		"C1234567":     false, // too short
		"D0B3Q17FZ2L":  false, // DM, not a channel
		"C0B3Q17FZ2l":  false, // lowercase digit at end
	}
	for in, want := range cases {
		if got := looksLikeChannelID(in); got != want {
			t.Errorf("looksLikeChannelID(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestResolveMonitor(t *testing.T) {
	known := []channelMeta{
		{ID: "C100", Name: "deploys"},
		{ID: "C200", Name: "health"},
		{ID: "C300", Name: "alerts"},
	}

	t.Run("names with and without hash resolve", func(t *testing.T) {
		got, missing := resolveMonitor([]string{"#deploys", "health"}, known)
		if len(missing) != 0 {
			t.Fatalf("unexpected missing: %v", missing)
		}
		want := []string{"C100", "C200"}
		if !equalStrings(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("channel ids pass through", func(t *testing.T) {
		got, missing := resolveMonitor([]string{"C999AAAAAAA", "#deploys"}, known)
		if len(missing) != 0 {
			t.Fatalf("unexpected missing: %v", missing)
		}
		want := []string{"C999AAAAAAA", "C100"}
		if !equalStrings(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("unknown names aggregate into missing", func(t *testing.T) {
		got, missing := resolveMonitor([]string{"#deploys", "#missing", "ghost"}, known)
		want := []string{"C100"}
		if !equalStrings(got, want) {
			t.Errorf("resolved = %v, want %v", got, want)
		}
		wantMissing := []string{"#missing", "#ghost"}
		if !equalStrings(missing, wantMissing) {
			t.Errorf("missing = %v, want %v", missing, wantMissing)
		}
	})

	t.Run("empty input returns empty", func(t *testing.T) {
		got, missing := resolveMonitor(nil, known)
		if len(got) != 0 || len(missing) != 0 {
			t.Errorf("expected empty: got=%v missing=%v", got, missing)
		}
	})
}

func TestScopeDiff(t *testing.T) {
	t.Run("all present returns nil", func(t *testing.T) {
		got := scopeDiff(
			[]string{"channels:read", "chat:write"},
			[]string{"channels:read", "chat:write", "extra:scope"},
		)
		if len(got) != 0 {
			t.Errorf("expected no missing, got %v", got)
		}
	})

	t.Run("missing surfaces in required order", func(t *testing.T) {
		got := scopeDiff(
			[]string{"channels:history", "channels:read", "chat:write", "groups:history", "groups:read"},
			[]string{"channels:read", "chat:write"},
		)
		want := []string{"channels:history", "groups:history", "groups:read"}
		if !equalStrings(got, want) {
			t.Errorf("missing = %v, want %v", got, want)
		}
	})

	t.Run("whitespace in granted is tolerated", func(t *testing.T) {
		got := scopeDiff(
			[]string{"channels:read"},
			[]string{" channels:read ", "chat:write"},
		)
		if len(got) != 0 {
			t.Errorf("whitespace not trimmed: missing=%v", got)
		}
	})

	t.Run("empty granted returns full required", func(t *testing.T) {
		got := scopeDiff([]string{"a", "b"}, nil)
		want := []string{"a", "b"}
		if !equalStrings(got, want) {
			t.Errorf("got %v want %v", got, want)
		}
	})
}

func TestRequiredScopes_matchManifest(t *testing.T) {
	// Pin the documented required-scopes set so manifest changes that
	// drift from runtime requirements surface as a test failure.
	want := []string{
		"channels:history",
		"channels:read",
		"chat:write",
		"groups:history",
		"groups:read",
	}
	if !equalStrings(RequiredScopes, want) {
		t.Errorf("RequiredScopes = %v, want %v", RequiredScopes, want)
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
