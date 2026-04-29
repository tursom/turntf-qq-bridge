package qqbridge

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseEnvelopeKeepsUserProtocolBackendAgnostic(t *testing.T) {
	t.Parallel()

	raw := []byte(`{
		"version":"v1alpha1",
		"kind":"chat",
		"conversation_ref":{"platform":"qq","scene":"group","chat_id":"123"},
		"content":{"segments":[{"kind":"text","text":"hello"}]},
		"remote_sender":{"id":"42","nickname":"alice"},
		"message_ref":{"id":"msg-1"},
		"metadata":{"trace_id":"abc"}
	}`)

	envelope, err := ParseEnvelope(raw)
	if err != nil {
		t.Fatalf("ParseEnvelope: %v", err)
	}
	if envelope.ConversationRef.Platform != "qq" {
		t.Fatalf("expected qq platform, got %+v", envelope.ConversationRef)
	}

	marshaled, err := json.Marshal(envelope)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	text := string(marshaled)
	for _, forbidden := range []string{`"adapter"`, `"backend"`, `"account"`} {
		if strings.Contains(text, forbidden) {
			t.Fatalf("unexpected backend-specific field %s in %s", forbidden, text)
		}
	}
}

func TestConversationRefDefaultsPlatformQQ(t *testing.T) {
	t.Parallel()

	envelope, err := ParseEnvelope([]byte(`{
		"kind":"chat",
		"conversation_ref":{"scene":"private","chat_id":"10001"},
		"content":{"segments":[{"kind":"text","text":"hello"}]}
	}`))
	if err != nil {
		t.Fatalf("ParseEnvelope: %v", err)
	}
	if envelope.ConversationRef.Platform != "qq" {
		t.Fatalf("expected default platform qq, got %q", envelope.ConversationRef.Platform)
	}
}
