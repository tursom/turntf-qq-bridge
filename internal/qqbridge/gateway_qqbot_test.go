package qqbridge

import (
	"context"
	"io"
	"log"
	"testing"
	"time"

	"github.com/tencent-connect/botgo/dto"
	"github.com/tencent-connect/botgo/event"
	"github.com/tencent-connect/botgo/openapi/options"
	"golang.org/x/oauth2"
)

func TestQQBotGatewaySendUsesPlatformAgnosticConversation(t *testing.T) {
	api := &fakeQQBotAPI{}
	gateway := &qqBotGateway{
		events:       make(chan GatewayInboundEvent, 1),
		capabilities: newGatewayCapabilities(SegmentKindText, SegmentKindImage, SegmentKindAudio, SegmentKindVideo, SegmentKindReply),
		api:          api,
		logger:       log.New(io.Discard, "", 0),
	}
	_, err := gateway.Send(context.Background(), outboundMessage{
		ConversationRef: ConversationRef{Platform: "qq", Scene: ConversationScenePrivate, ChatID: "u100"},
		Content:         Content{Segments: []Segment{{Kind: SegmentKindText, Text: "hello"}}},
		MessageRef:      MessageRef{ID: "local-1"},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if api.privateTarget != "u100" {
		t.Fatalf("unexpected private target: %q", api.privateTarget)
	}
	if api.lastText == "" {
		t.Fatal("expected text payload to be sent")
	}
}

func TestQQBotGatewayStartsWebsocketManager(t *testing.T) {
	api := &fakeQQBotAPI{wsURL: "wss://qq.example/ws", wsShards: 2}
	manager := &fakeQQBotSessionManager{started: make(chan struct{}, 1)}
	gateway, err := newQQBotGatewayWithAPI(
		context.Background(),
		func() {},
		QQBotConfig{AppID: "123", Secret: "secret"},
		log.New(io.Discard, "", 0),
		api,
		manager,
		oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "token", TokenType: "QQBot"}),
	)
	if err != nil {
		t.Fatalf("newQQBotGatewayWithAPI: %v", err)
	}
	defer gateway.Close()

	select {
	case <-manager.started:
		if manager.apInfo == nil || manager.apInfo.URL != "wss://qq.example/ws" {
			t.Fatalf("unexpected websocket ap info: %+v", manager.apInfo)
		}
		if manager.intents == nil || *manager.intents == 0 {
			t.Fatal("expected websocket intents to be registered")
		}
	case <-time.After(time.Second):
		t.Fatal("expected qqbot session manager to start")
	}
}

func TestQQBotWebsocketHandlersNormalizeInboundMessage(t *testing.T) {
	gateway := &qqBotGateway{
		events:       make(chan GatewayInboundEvent, 1),
		capabilities: newGatewayCapabilities(SegmentKindText, SegmentKindImage, SegmentKindAudio, SegmentKindVideo, SegmentKindReply),
		logger:       log.New(io.Discard, "", 0),
	}
	if _, err := gateway.registerHandlers(); err != nil {
		t.Fatalf("registerHandlers: %v", err)
	}
	t.Cleanup(func() {
		event.DefaultHandlers.C2CMessage = nil
		event.DefaultHandlers.GroupATMessage = nil
	})

	err := gateway.handleC2CEvent(nil, &dto.Message{
		ID:      "msg-1",
		Content: "hello from qq",
		Author:  &dto.User{ID: "u1", Username: "alice", Avatar: "avatar-url"},
		Attachments: []*dto.MessageAttachment{{
			URL:         "https://example.com/a.png",
			FileName:    "a.png",
			ContentType: "image/png",
		}},
	})
	if err != nil {
		t.Fatalf("handleC2CEvent: %v", err)
	}

	select {
	case got := <-gateway.events:
		if got.Envelope.ConversationRef.Platform != "qq" || got.Envelope.ConversationRef.Scene != ConversationScenePrivate {
			t.Fatalf("unexpected conversation ref: %+v", got.Envelope.ConversationRef)
		}
		if got.Envelope.RemoteSender == nil || got.Envelope.RemoteSender.Nickname != "alice" {
			t.Fatalf("unexpected remote sender: %+v", got.Envelope.RemoteSender)
		}
		if len(got.Envelope.Content.Segments) != 2 {
			t.Fatalf("expected text + image segments, got %+v", got.Envelope.Content.Segments)
		}
	default:
		t.Fatal("expected qqbot inbound event")
	}
}

type fakeQQBotAPI struct {
	privateTarget string
	groupTarget   string
	lastText      string
	wsURL         string
	wsShards      uint32
}

func (f *fakeQQBotAPI) PostGroupMessage(_ context.Context, groupID string, msg dto.APIMessage, _ ...options.Option) (*dto.Message, error) {
	f.groupTarget = groupID
	f.lastText = extractQQBotText(msg)
	return &dto.Message{ID: "group-remote"}, nil
}

func (f *fakeQQBotAPI) PostC2CMessage(_ context.Context, userID string, msg dto.APIMessage, _ ...options.Option) (*dto.Message, error) {
	f.privateTarget = userID
	f.lastText = extractQQBotText(msg)
	return &dto.Message{ID: "c2c-remote"}, nil
}

func (f *fakeQQBotAPI) WS(_ context.Context, _ map[string]string, _ string) (*dto.WebsocketAP, error) {
	return &dto.WebsocketAP{
		URL:    f.wsURL,
		Shards: f.wsShards,
	}, nil
}

type fakeQQBotSessionManager struct {
	apInfo      *dto.WebsocketAP
	intents     *dto.Intent
	tokenSource oauth2.TokenSource
	started     chan struct{}
}

func (f *fakeQQBotSessionManager) Start(apInfo *dto.WebsocketAP, tokenSource oauth2.TokenSource, intents *dto.Intent) error {
	f.apInfo = apInfo
	f.tokenSource = tokenSource
	f.intents = intents
	if f.started != nil {
		f.started <- struct{}{}
	}
	return nil
}

func extractQQBotText(msg dto.APIMessage) string {
	switch typed := msg.(type) {
	case dto.MessageToCreate:
		return typed.Content
	case dto.RichMediaMessage:
		return typed.Content
	default:
		return ""
	}
}
