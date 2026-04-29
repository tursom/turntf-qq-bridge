package qqbridge

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

type napCatGateway struct {
	cfg          NapCatConfig
	logger       *log.Logger
	events       chan GatewayInboundEvent
	capabilities gatewayCapabilities

	ctx    context.Context
	cancel context.CancelFunc

	stateMu sync.RWMutex
	conn    *websocket.Conn

	pendingMu sync.Mutex
	pending   map[string]chan napCatActionResult
	seq       atomic.Uint64
}

type napCatFrame struct {
	PostType    string          `json:"post_type"`
	MessageType string          `json:"message_type"`
	MessageID   json.RawMessage `json:"message_id"`
	UserID      json.RawMessage `json:"user_id"`
	GroupID     json.RawMessage `json:"group_id"`
	SelfID      json.RawMessage `json:"self_id"`
	RawMessage  string          `json:"raw_message"`
	Message     json.RawMessage `json:"message"`
	Sender      napCatSender    `json:"sender"`
	Time        int64           `json:"time"`

	Status  string          `json:"status"`
	RetCode int             `json:"retcode"`
	Wording string          `json:"wording"`
	Data    json.RawMessage `json:"data"`
	Echo    string          `json:"echo"`
}

type napCatSender struct {
	UserID   json.RawMessage `json:"user_id"`
	Nickname string          `json:"nickname"`
	Card     string          `json:"card"`
	Role     string          `json:"role"`
}

type napCatSegment struct {
	Type string                 `json:"type"`
	Data map[string]interface{} `json:"data"`
}

type napCatActionResult struct {
	RemoteMessageID string
	Err             error
}

func newNapCatGateway(parent context.Context, cfg NapCatConfig, logger *log.Logger) (QQGateway, error) {
	ctx, cancel := context.WithCancel(parent)
	gateway := &napCatGateway{
		cfg:    cfg,
		logger: logger,
		events: make(chan GatewayInboundEvent, 64),
		capabilities: newGatewayCapabilities(
			SegmentKindText,
			SegmentKindImage,
			SegmentKindAudio,
			SegmentKindVideo,
			SegmentKindFile,
			SegmentKindReply,
			SegmentKindMention,
			SegmentKindJSON,
		),
		ctx:     ctx,
		cancel:  cancel,
		pending: make(map[string]chan napCatActionResult),
	}
	go gateway.run()
	return gateway, nil
}

func (g *napCatGateway) Events() <-chan GatewayInboundEvent {
	return g.events
}

func (g *napCatGateway) Capabilities() gatewayCapabilities {
	return g.capabilities
}

func (g *napCatGateway) Close() error {
	g.cancel()
	g.stateMu.Lock()
	if g.conn != nil {
		g.conn.CloseNow()
		g.conn = nil
	}
	g.stateMu.Unlock()
	return nil
}

func (g *napCatGateway) Send(ctx context.Context, msg outboundMessage) (gatewaySendResult, error) {
	if err := msg.ConversationRef.NormalizeAndValidate(); err != nil {
		return gatewaySendResult{}, terminalBridgeError(ReceiptCodeUnsupportedContent, "conversation_ref is invalid", err)
	}
	if err := msg.Content.NormalizeAndValidate(); err != nil {
		return gatewaySendResult{}, terminalBridgeError(ReceiptCodeUnsupportedContent, "content is invalid", err)
	}
	if err := g.capabilities.SupportsAll(msg.Content); err != nil {
		return gatewaySendResult{}, err
	}
	conn := g.currentConn()
	if conn == nil {
		return gatewaySendResult{}, retryableBridgeError(ReceiptCodePlatformUnavailable, "napcat websocket is not connected", nil)
	}

	action, params, err := g.buildAction(msg)
	if err != nil {
		return gatewaySendResult{}, err
	}
	echo := fmt.Sprintf("napcat-%d", g.seq.Add(1))
	payload := map[string]any{
		"action": action,
		"params": params,
		"echo":   echo,
	}

	resultCh := make(chan napCatActionResult, 1)
	g.pendingMu.Lock()
	g.pending[echo] = resultCh
	g.pendingMu.Unlock()
	defer func() {
		g.pendingMu.Lock()
		delete(g.pending, echo)
		g.pendingMu.Unlock()
	}()

	if err := wsjson.Write(ctx, conn, payload); err != nil {
		return gatewaySendResult{}, retryableBridgeError(ReceiptCodePlatformUnavailable, "write napcat action failed", err)
	}

	select {
	case result := <-resultCh:
		if result.Err != nil {
			return gatewaySendResult{}, result.Err
		}
		return gatewaySendResult{RemoteMessageID: result.RemoteMessageID}, nil
	case <-ctx.Done():
		return gatewaySendResult{}, retryableBridgeError(ReceiptCodePlatformUnavailable, "wait napcat action response timed out", ctx.Err())
	case <-g.ctx.Done():
		return gatewaySendResult{}, retryableBridgeError(ReceiptCodePlatformUnavailable, "napcat gateway is closing", g.ctx.Err())
	}
}

func (g *napCatGateway) run() {
	backoff := time.Second
	for {
		if g.ctx.Err() != nil {
			close(g.events)
			return
		}
		conn, _, err := websocket.Dial(g.ctx, g.cfg.WSURL, &websocket.DialOptions{
			HTTPHeader: g.dialHeaders(),
		})
		if err != nil {
			g.logger.Printf("qqbridge napcat dial failed: %v", err)
			select {
			case <-time.After(backoff):
			case <-g.ctx.Done():
				close(g.events)
				return
			}
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		backoff = time.Second
		g.stateMu.Lock()
		g.conn = conn
		g.stateMu.Unlock()
		g.logger.Printf("qqbridge napcat connected: %s", g.cfg.WSURL)
		err = g.readLoop(conn)
		if err != nil && g.ctx.Err() == nil {
			g.logger.Printf("qqbridge napcat read loop ended: %v", err)
		}
		g.stateMu.Lock()
		if g.conn == conn {
			g.conn = nil
		}
		g.stateMu.Unlock()
		g.failPending(retryableBridgeError(ReceiptCodePlatformUnavailable, "napcat websocket disconnected", err))
		conn.CloseNow()
	}
}

func (g *napCatGateway) readLoop(conn *websocket.Conn) error {
	for {
		var frame napCatFrame
		if err := wsjson.Read(g.ctx, conn, &frame); err != nil {
			return err
		}
		if frame.Echo != "" {
			g.resolvePending(frame)
			continue
		}
		if frame.PostType != "message" {
			continue
		}
		event, err := g.normalizeEvent(frame)
		if err != nil {
			g.logger.Printf("qqbridge napcat normalize event failed: %v", err)
			continue
		}
		select {
		case g.events <- event:
		case <-g.ctx.Done():
			return g.ctx.Err()
		}
	}
}

func (g *napCatGateway) resolvePending(frame napCatFrame) {
	g.pendingMu.Lock()
	ch, ok := g.pending[frame.Echo]
	g.pendingMu.Unlock()
	if !ok {
		return
	}
	if frame.Status != "ok" || frame.RetCode != 0 {
		ch <- napCatActionResult{
			Err: terminalBridgeError(ReceiptCodeDeliveryFailed, strings.TrimSpace(frame.Wording), nil),
		}
		return
	}
	ch <- napCatActionResult{RemoteMessageID: extractID(frame.Data)}
}

func (g *napCatGateway) failPending(err error) {
	g.pendingMu.Lock()
	defer g.pendingMu.Unlock()
	for echo, ch := range g.pending {
		ch <- napCatActionResult{Err: err}
		delete(g.pending, echo)
	}
}

func (g *napCatGateway) currentConn() *websocket.Conn {
	g.stateMu.RLock()
	defer g.stateMu.RUnlock()
	return g.conn
}

func (g *napCatGateway) dialHeaders() http.Header {
	header := http.Header{}
	if token := strings.TrimSpace(g.cfg.AccessToken); token != "" {
		header.Set("Authorization", "Bearer "+token)
	}
	return header
}

func (g *napCatGateway) buildAction(msg outboundMessage) (string, map[string]any, error) {
	segments, err := renderNapCatSegments(msg.Content)
	if err != nil {
		return "", nil, err
	}
	switch msg.ConversationRef.Scene {
	case ConversationSceneGroup:
		return "send_group_msg", map[string]any{
			"group_id": msg.ConversationRef.ChatID,
			"message":  segments,
		}, nil
	case ConversationScenePrivate:
		return "send_private_msg", map[string]any{
			"user_id": msg.ConversationRef.ChatID,
			"message": segments,
		}, nil
	default:
		return "", nil, terminalBridgeError(ReceiptCodeTargetNotFound, "unsupported qq conversation scene", nil)
	}
}

func (g *napCatGateway) normalizeEvent(frame napCatFrame) (GatewayInboundEvent, error) {
	selfID := extractID(frame.SelfID)
	if g.cfg.SelfID != "" && selfID == g.cfg.SelfID {
		return GatewayInboundEvent{}, fmt.Errorf("ignore self message")
	}
	if senderID := extractID(frame.Sender.UserID); senderID != "" && g.cfg.SelfID != "" && senderID == g.cfg.SelfID {
		return GatewayInboundEvent{}, fmt.Errorf("ignore bot sender message")
	}

	conversation := ConversationRef{
		Platform: "qq",
	}
	switch frame.MessageType {
	case "group":
		conversation.Scene = ConversationSceneGroup
		conversation.ChatID = extractID(frame.GroupID)
	case "private":
		conversation.Scene = ConversationScenePrivate
		conversation.ChatID = extractID(frame.UserID)
	default:
		return GatewayInboundEvent{}, fmt.Errorf("unsupported napcat message_type %q", frame.MessageType)
	}
	if err := conversation.NormalizeAndValidate(); err != nil {
		return GatewayInboundEvent{}, err
	}

	segments, err := parseNapCatSegments(frame.Message, frame.RawMessage)
	if err != nil {
		return GatewayInboundEvent{}, err
	}
	envelope := BridgeEnvelope{
		Version:         EnvelopeVersion,
		Kind:            EnvelopeKindChat,
		ConversationRef: conversation,
		Content:         Content{Segments: segments},
		RemoteSender: &RemoteSender{
			ID:        extractID(frame.UserID),
			Nickname:  strings.TrimSpace(frame.Sender.Nickname),
			Remark:    strings.TrimSpace(frame.Sender.Card),
			Role:      strings.TrimSpace(frame.Sender.Role),
			AvatarURL: "",
		},
		MessageRef: MessageRef{ID: extractID(frame.MessageID)},
		Metadata: Metadata{
			"platform": "qq",
			"scene":    string(conversation.Scene),
			"self_id":  selfID,
			"time":     frame.Time,
		},
	}
	if err := envelope.NormalizeAndValidate(); err != nil {
		return GatewayInboundEvent{}, err
	}
	return GatewayInboundEvent{
		GatewayMessageID: strings.TrimSpace(envelope.MessageRef.ID),
		Envelope:         envelope,
	}, nil
}

func renderNapCatSegments(content Content) ([]map[string]any, error) {
	message := make([]map[string]any, 0, len(content.Segments))
	for _, segment := range content.Segments {
		switch segment.Kind {
		case SegmentKindText:
			message = append(message, map[string]any{
				"type": "text",
				"data": map[string]any{"text": segment.Text},
			})
		case SegmentKindImage:
			message = append(message, map[string]any{
				"type": "image",
				"data": map[string]any{"file": segment.URL},
			})
		case SegmentKindAudio:
			message = append(message, map[string]any{
				"type": "record",
				"data": map[string]any{"file": segment.URL},
			})
		case SegmentKindVideo:
			message = append(message, map[string]any{
				"type": "video",
				"data": map[string]any{"file": segment.URL},
			})
		case SegmentKindFile:
			message = append(message, map[string]any{
				"type": "file",
				"data": map[string]any{"file": segment.URL, "name": segment.FileName},
			})
		case SegmentKindReply:
			message = append(message, map[string]any{
				"type": "reply",
				"data": map[string]any{"id": segment.MessageID},
			})
		case SegmentKindMention:
			message = append(message, map[string]any{
				"type": "at",
				"data": map[string]any{"qq": segment.UserID},
			})
		case SegmentKindJSON:
			var payload any
			if segment.Payload == nil {
				return nil, terminalBridgeError(ReceiptCodeUnsupportedContent, "json segment payload is empty", nil)
			}
			if err := json.Unmarshal(*segment.Payload, &payload); err != nil {
				return nil, terminalBridgeError(ReceiptCodeUnsupportedContent, "json segment payload is invalid", err)
			}
			message = append(message, map[string]any{
				"type": "json",
				"data": map[string]any{"data": payload},
			})
		default:
			return nil, terminalBridgeError(ReceiptCodeUnsupportedContent, "unsupported segment kind for napcat", nil)
		}
	}
	return message, nil
}

func parseNapCatSegments(raw json.RawMessage, rawMessage string) ([]Segment, error) {
	var segments []napCatSegment
	if err := json.Unmarshal(raw, &segments); err == nil && len(segments) > 0 {
		out := make([]Segment, 0, len(segments))
		for _, segment := range segments {
			switch segment.Type {
			case "text":
				out = append(out, Segment{Kind: SegmentKindText, Text: stringValue(segment.Data["text"])})
			case "image":
				out = append(out, Segment{Kind: SegmentKindImage, URL: stringValue(segment.Data["url"], segment.Data["file"])})
			case "record":
				out = append(out, Segment{Kind: SegmentKindAudio, URL: stringValue(segment.Data["url"], segment.Data["file"])})
			case "video":
				out = append(out, Segment{Kind: SegmentKindVideo, URL: stringValue(segment.Data["url"], segment.Data["file"])})
			case "file":
				out = append(out, Segment{
					Kind:     SegmentKindFile,
					URL:      stringValue(segment.Data["url"], segment.Data["file"]),
					FileName: stringValue(segment.Data["name"]),
				})
			case "reply":
				out = append(out, Segment{Kind: SegmentKindReply, MessageID: stringValue(segment.Data["id"])})
			case "at":
				out = append(out, Segment{Kind: SegmentKindMention, UserID: stringValue(segment.Data["qq"])})
			case "json":
				payload, err := json.Marshal(segment.Data["data"])
				if err != nil {
					return nil, err
				}
				rawPayload := json.RawMessage(payload)
				out = append(out, Segment{Kind: SegmentKindJSON, Payload: &rawPayload})
			}
		}
		if len(out) > 0 {
			return out, nil
		}
	}
	if strings.TrimSpace(rawMessage) == "" {
		return nil, terminalBridgeError(ReceiptCodeUnsupportedContent, "napcat message has no usable payload", nil)
	}
	return []Segment{{Kind: SegmentKindText, Text: rawMessage}}, nil
}

func extractID(raw json.RawMessage) string {
	if len(raw) == 0 {
		return ""
	}
	var number json.Number
	if err := json.Unmarshal(raw, &number); err == nil {
		return number.String()
	}
	var value string
	if err := json.Unmarshal(raw, &value); err == nil {
		return strings.TrimSpace(value)
	}
	var floatValue float64
	if err := json.Unmarshal(raw, &floatValue); err == nil {
		return strconv.FormatInt(int64(floatValue), 10)
	}
	var object map[string]any
	if err := json.Unmarshal(raw, &object); err == nil {
		return stringValue(object["message_id"], object["id"])
	}
	return strings.TrimSpace(string(raw))
}

func stringValue(values ...any) string {
	for _, value := range values {
		switch typed := value.(type) {
		case nil:
		case string:
			if trimmed := strings.TrimSpace(typed); trimmed != "" {
				return trimmed
			}
		case json.Number:
			return typed.String()
		case float64:
			return strconv.FormatInt(int64(typed), 10)
		case int64:
			return strconv.FormatInt(typed, 10)
		case int:
			return strconv.Itoa(typed)
		default:
			if raw, err := json.Marshal(typed); err == nil && string(raw) != "null" {
				return strings.Trim(string(raw), `"`)
			}
		}
	}
	return ""
}
