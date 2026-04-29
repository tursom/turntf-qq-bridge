package qqbridge

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
)

const EnvelopeVersion = "v1alpha1"

type EnvelopeKind string

const (
	EnvelopeKindChat    EnvelopeKind = "chat"
	EnvelopeKindSystem  EnvelopeKind = "system"
	EnvelopeKindReceipt EnvelopeKind = "receipt"
)

type ConversationScene string

const (
	ConversationSceneGroup   ConversationScene = "group"
	ConversationScenePrivate ConversationScene = "private"
)

type SegmentKind string

const (
	SegmentKindText    SegmentKind = "text"
	SegmentKindImage   SegmentKind = "image"
	SegmentKindAudio   SegmentKind = "audio"
	SegmentKindVideo   SegmentKind = "video"
	SegmentKindFile    SegmentKind = "file"
	SegmentKindReply   SegmentKind = "reply"
	SegmentKindMention SegmentKind = "mention"
	SegmentKindJSON    SegmentKind = "json"
)

type BridgeEnvelope struct {
	Version         string          `json:"version,omitempty"`
	Kind            EnvelopeKind    `json:"kind"`
	ConversationRef ConversationRef `json:"conversation_ref"`
	Content         Content         `json:"content"`
	RemoteSender    *RemoteSender   `json:"remote_sender,omitempty"`
	MessageRef      MessageRef      `json:"message_ref,omitempty"`
	Metadata        Metadata        `json:"metadata,omitempty"`
}

type ConversationRef struct {
	Platform string            `json:"platform,omitempty"`
	Scene    ConversationScene `json:"scene"`
	ChatID   string            `json:"chat_id"`
	ThreadID string            `json:"thread_id,omitempty"`
}

type Content struct {
	Segments []Segment `json:"segments"`
}

type Segment struct {
	Kind      SegmentKind      `json:"kind"`
	Text      string           `json:"text,omitempty"`
	URL       string           `json:"url,omitempty"`
	FileName  string           `json:"file_name,omitempty"`
	MIME      string           `json:"mime,omitempty"`
	MessageID string           `json:"message_id,omitempty"`
	UserID    string           `json:"user_id,omitempty"`
	Payload   *json.RawMessage `json:"payload,omitempty"`
}

type RemoteSender struct {
	ID        string `json:"id"`
	Nickname  string `json:"nickname"`
	Remark    string `json:"remark,omitempty"`
	Role      string `json:"role,omitempty"`
	AvatarURL string `json:"avatar_url,omitempty"`
}

type MessageRef struct {
	ID      string `json:"id,omitempty"`
	ReplyTo string `json:"reply_to,omitempty"`
}

type Metadata map[string]any

func ParseEnvelope(data []byte) (BridgeEnvelope, error) {
	var env BridgeEnvelope
	if err := json.Unmarshal(data, &env); err != nil {
		return BridgeEnvelope{}, fmt.Errorf("parse bridge envelope: %w", err)
	}
	if err := env.NormalizeAndValidate(); err != nil {
		return BridgeEnvelope{}, err
	}
	return env, nil
}

func (e *BridgeEnvelope) NormalizeAndValidate() error {
	if e == nil {
		return fmt.Errorf("bridge envelope is required")
	}
	if e.Version == "" {
		e.Version = EnvelopeVersion
	}
	if e.Version != EnvelopeVersion {
		return fmt.Errorf("unsupported bridge envelope version %q", e.Version)
	}
	switch e.Kind {
	case EnvelopeKindChat, EnvelopeKindSystem, EnvelopeKindReceipt:
	default:
		return fmt.Errorf("unsupported bridge envelope kind %q", e.Kind)
	}
	if err := e.ConversationRef.NormalizeAndValidate(); err != nil {
		return err
	}
	if err := e.Content.NormalizeAndValidate(); err != nil {
		return err
	}
	return nil
}

func (r *ConversationRef) NormalizeAndValidate() error {
	if r == nil {
		return fmt.Errorf("conversation_ref is required")
	}
	r.Platform = strings.TrimSpace(r.Platform)
	if r.Platform == "" {
		r.Platform = "qq"
	}
	if r.Platform != "qq" {
		return fmt.Errorf("unsupported conversation_ref.platform %q", r.Platform)
	}
	switch r.Scene {
	case ConversationSceneGroup, ConversationScenePrivate:
	default:
		return fmt.Errorf("unsupported conversation_ref.scene %q", r.Scene)
	}
	r.ChatID = strings.TrimSpace(r.ChatID)
	if r.ChatID == "" {
		return fmt.Errorf("conversation_ref.chat_id is required")
	}
	r.ThreadID = strings.TrimSpace(r.ThreadID)
	return nil
}

func (r ConversationRef) Key() string {
	threadID := r.ThreadID
	if threadID == "" {
		threadID = "-"
	}
	return strings.Join([]string{r.Platform, string(r.Scene), r.ChatID, threadID}, "|")
}

func (r ConversationRef) Hash() string {
	sum := sha256.Sum256([]byte(r.Key()))
	return hex.EncodeToString(sum[:])
}

func (c *Content) NormalizeAndValidate() error {
	if c == nil {
		return fmt.Errorf("content is required")
	}
	if len(c.Segments) == 0 {
		return fmt.Errorf("content.segments is required")
	}
	for i := range c.Segments {
		if err := c.Segments[i].NormalizeAndValidate(); err != nil {
			return fmt.Errorf("content.segments[%d]: %w", i, err)
		}
	}
	return nil
}

func (s *Segment) NormalizeAndValidate() error {
	if s == nil {
		return fmt.Errorf("segment is required")
	}
	switch s.Kind {
	case SegmentKindText:
		if strings.TrimSpace(s.Text) == "" {
			return fmt.Errorf("text segment requires text")
		}
	case SegmentKindImage, SegmentKindAudio, SegmentKindVideo, SegmentKindFile:
		if strings.TrimSpace(s.URL) == "" {
			return fmt.Errorf("%s segment requires url", s.Kind)
		}
	case SegmentKindReply:
		if strings.TrimSpace(s.MessageID) == "" {
			return fmt.Errorf("reply segment requires message_id")
		}
	case SegmentKindMention:
		if strings.TrimSpace(s.UserID) == "" {
			return fmt.Errorf("mention segment requires user_id")
		}
	case SegmentKindJSON:
		if s.Payload == nil || len(*s.Payload) == 0 || bytes.Equal(bytes.TrimSpace(*s.Payload), []byte("null")) {
			return fmt.Errorf("json segment requires payload")
		}
		var tmp any
		if err := json.Unmarshal(*s.Payload, &tmp); err != nil {
			return fmt.Errorf("json segment payload must be valid json: %w", err)
		}
	default:
		return fmt.Errorf("unsupported segment kind %q", s.Kind)
	}
	return nil
}

func (c Content) TextSummary() string {
	var builder strings.Builder
	for _, segment := range c.Segments {
		if segment.Kind != SegmentKindText {
			continue
		}
		builder.WriteString(segment.Text)
	}
	return builder.String()
}

func NewReceiptEnvelope(conversation ConversationRef, messageRef MessageRef, code ReceiptCode, text string) BridgeEnvelope {
	return BridgeEnvelope{
		Version:         EnvelopeVersion,
		Kind:            EnvelopeKindReceipt,
		ConversationRef: conversation,
		MessageRef:      messageRef,
		Content: Content{
			Segments: []Segment{{
				Kind: SegmentKindText,
				Text: text,
			}},
		},
		Metadata: Metadata{
			"code": string(code),
		},
	}
}
