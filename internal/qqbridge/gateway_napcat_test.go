package qqbridge

import (
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/coder/websocket/wsjson"
)

func TestNapCatGatewayReceivesEventAndSendsAction(t *testing.T) {
	t.Parallel()

	receivedAction := make(chan map[string]any, 1)
	handler := httpHandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			t.Fatalf("Accept: %v", err)
		}
		defer conn.Close(websocket.StatusNormalClosure, "done")

		if err := wsjson.Write(context.Background(), conn, map[string]any{
			"post_type":    "message",
			"message_type": "group",
			"self_id":      "20000",
			"message_id":   "m1",
			"group_id":     "g1",
			"user_id":      "u1",
			"raw_message":  "hello",
			"message": []map[string]any{{
				"type": "text",
				"data": map[string]any{"text": "hello"},
			}},
			"sender": map[string]any{
				"user_id":  "u1",
				"nickname": "alice",
				"card":     "Alice",
				"role":     "member",
			},
		}); err != nil {
			t.Fatalf("Write event: %v", err)
		}

		var outbound map[string]any
		if err := wsjson.Read(context.Background(), conn, &outbound); err != nil {
			t.Fatalf("Read outbound action: %v", err)
		}
		receivedAction <- outbound
		if err := wsjson.Write(context.Background(), conn, map[string]any{
			"status":  "ok",
			"retcode": 0,
			"data": map[string]any{
				"message_id": "remote-1",
			},
			"echo": outbound["echo"],
		}); err != nil {
			t.Fatalf("Write response: %v", err)
		}
	})
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Skipf("skip websocket integration test: %v", err)
	}
	server := &http.Server{Handler: handler}
	go func() {
		_ = server.Serve(listener)
	}()
	defer server.Close()
	defer listener.Close()

	wsURL := "ws://" + listener.Addr().String()
	gateway, err := newNapCatGateway(context.Background(), NapCatConfig{WSURL: wsURL}, log.New(io.Discard, "", 0))
	if err != nil {
		t.Fatalf("newNapCatGateway: %v", err)
	}
	defer gateway.Close()

	select {
	case event := <-gateway.Events():
		if event.Envelope.ConversationRef.Platform != "qq" || event.Envelope.ConversationRef.Scene != ConversationSceneGroup {
			t.Fatalf("unexpected event conversation: %+v", event.Envelope.ConversationRef)
		}
		if event.Envelope.RemoteSender == nil || event.Envelope.RemoteSender.Nickname != "alice" {
			t.Fatalf("unexpected remote sender: %+v", event.Envelope.RemoteSender)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for napcat inbound event")
	}

	sendCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	result, err := gateway.Send(sendCtx, outboundMessage{
		ConversationRef: ConversationRef{Platform: "qq", Scene: ConversationSceneGroup, ChatID: "g1"},
		Content:         Content{Segments: []Segment{{Kind: SegmentKindText, Text: "bridge"}}},
		MessageRef:      MessageRef{ID: "local-1"},
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if result.RemoteMessageID != "remote-1" {
		t.Fatalf("unexpected remote message id: %q", result.RemoteMessageID)
	}

	select {
	case action := <-receivedAction:
		if got := action["action"]; got != "send_group_msg" {
			t.Fatalf("unexpected action: %#v", got)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timeout waiting for napcat outbound action")
	}
}

type httpHandlerFunc func(http.ResponseWriter, *http.Request)

func (f httpHandlerFunc) ServeHTTP(w http.ResponseWriter, r *http.Request) { f(w, r) }
