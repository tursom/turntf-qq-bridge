package qqbridge

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"

	turntf "github.com/tursom/turntf-go"
)

func TestStoreSaveMessageCreatesBindingAndRoutesInbound(t *testing.T) {
	t.Parallel()

	store := openTestBridgeStore(t)
	defer store.Close()

	ctx := context.Background()
	outbound := BridgeEnvelope{
		Version: EnvelopeVersion,
		Kind:    EnvelopeKindChat,
		ConversationRef: ConversationRef{
			Platform: "qq",
			Scene:    ConversationSceneGroup,
			ChatID:   "group-1",
		},
		Content: Content{
			Segments: []Segment{{Kind: SegmentKindText, Text: "hello qq"}},
		},
		MessageRef: MessageRef{ID: "local-msg-1"},
	}
	body, err := json.Marshal(outbound)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}

	if err := store.SaveMessage(ctx, turntf.Message{
		Recipient:    store.bridgeUser,
		NodeID:       4096,
		Seq:          11,
		Sender:       turntf.UserRef{NodeID: 4096, UserID: 2001},
		Body:         body,
		CreatedAtHLC: "hlc-1",
	}); err != nil {
		t.Fatalf("SaveMessage: %v", err)
	}

	job, err := store.ClaimOutboundJob(ctx)
	if err != nil {
		t.Fatalf("ClaimOutboundJob: %v", err)
	}
	if job == nil {
		t.Fatal("expected outbound job")
	}
	if job.SourceSender != (turntf.UserRef{NodeID: 4096, UserID: 2001}) {
		t.Fatalf("unexpected source sender: %+v", job.SourceSender)
	}
	if err := store.MarkOutboundDelivered(ctx, job.ID, "remote-1"); err != nil {
		t.Fatalf("MarkOutboundDelivered: %v", err)
	}

	inbound := BridgeEnvelope{
		Version:         EnvelopeVersion,
		Kind:            EnvelopeKindChat,
		ConversationRef: outbound.ConversationRef,
		Content:         Content{Segments: []Segment{{Kind: SegmentKindText, Text: "reply from qq"}}},
		RemoteSender:    &RemoteSender{ID: "30001", Nickname: "qq-user"},
		MessageRef:      MessageRef{ID: "qq-msg-1"},
	}
	if err := store.EnqueueInboundEvent(ctx, GatewayInboundEvent{
		GatewayMessageID: "qq-msg-1",
		Envelope:         inbound,
	}); err != nil {
		t.Fatalf("EnqueueInboundEvent: %v", err)
	}

	deliveryJob, err := store.ClaimTurnTFDeliveryJob(ctx)
	if err != nil {
		t.Fatalf("ClaimTurnTFDeliveryJob: %v", err)
	}
	if deliveryJob == nil {
		t.Fatal("expected turntf delivery job")
	}
	if deliveryJob.Target != (turntf.UserRef{NodeID: 4096, UserID: 2001}) {
		t.Fatalf("unexpected delivery target: %+v", deliveryJob.Target)
	}
}

func TestStoreInvalidBridgeMessageQueuesReceipt(t *testing.T) {
	t.Parallel()

	store := openTestBridgeStore(t)
	defer store.Close()

	ctx := context.Background()
	if err := store.SaveMessage(ctx, turntf.Message{
		Recipient:    store.bridgeUser,
		NodeID:       4096,
		Seq:          12,
		Sender:       turntf.UserRef{NodeID: 4096, UserID: 2002},
		Body:         []byte(`{"kind":"chat","conversation_ref":{"platform":"qq","scene":"group"}}`),
		CreatedAtHLC: "hlc-2",
	}); err != nil {
		t.Fatalf("SaveMessage: %v", err)
	}

	receiptJob, err := store.ClaimTurnTFDeliveryJob(ctx)
	if err != nil {
		t.Fatalf("ClaimTurnTFDeliveryJob: %v", err)
	}
	if receiptJob == nil {
		t.Fatal("expected receipt job")
	}
	if receiptJob.Kind != "receipt" {
		t.Fatalf("expected receipt job, got %q", receiptJob.Kind)
	}
	if receiptJob.Target != (turntf.UserRef{NodeID: 4096, UserID: 2002}) {
		t.Fatalf("unexpected receipt target: %+v", receiptJob.Target)
	}
	if got := receiptJob.Envelope.Metadata["code"]; got != string(ReceiptCodeUnsupportedContent) {
		t.Fatalf("unexpected receipt code: %#v", got)
	}
}

func openTestBridgeStore(t *testing.T) *Store {
	t.Helper()
	store, err := OpenStore(filepath.Join(t.TempDir(), "bridge.sqlite"), turntf.UserRef{NodeID: 4096, UserID: 1099})
	if err != nil {
		t.Fatalf("OpenStore: %v", err)
	}
	return store
}
