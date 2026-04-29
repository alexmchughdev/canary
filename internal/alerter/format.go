package alerter

import (
	"bytes"
	"fmt"
	"strings"
	"time"
)

// FormatSlack renders an Alert as a Slack-friendly text block: an icon
// for the severity, the Title bold, and the Body in a code block.
func FormatSlack(a Alert) string {
	icon := ":warning:"
	switch a.Severity {
	case SeverityCritical:
		icon = ":rotating_light:"
	case SeverityInfo:
		icon = ":information_source:"
	}
	if a.Body == "" {
		return fmt.Sprintf("%s *%s*", icon, a.Title)
	}
	return fmt.Sprintf("%s *%s*\n```\n%s\n```", icon, a.Title, a.Body)
}

// FormatEmail returns a complete RFC 822 message ready for net/smtp.
func FormatEmail(a Alert, from string, to []string) []byte {
	subject := fmt.Sprintf("[Foghorn] %s: %s", strings.ToUpper(string(a.Severity)), a.Title)
	var b bytes.Buffer
	fmt.Fprintf(&b, "From: %s\r\n", from)
	fmt.Fprintf(&b, "To: %s\r\n", strings.Join(to, ", "))
	fmt.Fprintf(&b, "Subject: %s\r\n", subject)
	fmt.Fprintf(&b, "MIME-Version: 1.0\r\n")
	fmt.Fprintf(&b, "Content-Type: text/plain; charset=utf-8\r\n\r\n")
	fmt.Fprintf(&b, "%s\n\nKind: %s\nConnector: %s\nChannel: %s\nRaised: %s\n",
		a.Body, a.Kind, a.Connector, a.ChannelID, a.RaisedAt.Format(time.RFC3339))
	return b.Bytes()
}
