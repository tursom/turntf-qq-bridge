package qqbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	turntf "github.com/tursom/turntf-go"
)

type Runtime struct {
	cfg    Config
	logger *log.Logger

	store   *Store
	gateway QQGateway
	client  *turntf.Client
}

func Run(ctx context.Context, cfg Config, logger *log.Logger) error {
	if logger == nil {
		logger = log.Default()
	}
	store, err := OpenStore(cfg.Storage.SQLitePath, cfg.BridgeUserRef())
	if err != nil {
		return err
	}
	defer store.Close()

	gateway, err := newGateway(ctx, cfg, logger)
	if err != nil {
		return err
	}
	defer gateway.Close()

	runtime := &Runtime{
		cfg:     cfg,
		logger:  logger,
		store:   store,
		gateway: gateway,
	}
	client, err := turntf.NewClient(turntf.Config{
		BaseURL: cfg.TurnTF.BaseURL,
		Credentials: turntf.Credentials{
			NodeID:   cfg.TurnTF.BridgeUser.NodeID,
			UserID:   cfg.TurnTF.BridgeUser.UserID,
			Password: cfg.TurnTF.BridgeUser.Password.PasswordInput,
		},
		CursorStore:           store,
		Handler:               runtime,
		InitialReconnectDelay: time.Second,
		MaxReconnectDelay:     30 * time.Second,
		RequestTimeout:        10 * time.Second,
		PingInterval:          30 * time.Second,
	})
	if err != nil {
		return err
	}
	runtime.client = client
	defer client.Close()

	if err := client.Connect(ctx); err != nil {
		return err
	}

	runtime.logger.Printf("qqbridge connected to turntf as %d:%d", cfg.TurnTF.BridgeUser.NodeID, cfg.TurnTF.BridgeUser.UserID)

	var wg sync.WaitGroup
	wg.Add(3)
	go func() {
		defer wg.Done()
		runtime.runOutboundLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		runtime.runTurnTFDeliveryLoop(ctx)
	}()
	go func() {
		defer wg.Done()
		runtime.consumeGatewayEvents(ctx)
	}()

	<-ctx.Done()
	wg.Wait()
	return ctx.Err()
}

func (r *Runtime) OnLogin(_ context.Context, info turntf.LoginInfo) {
	r.logger.Printf("qqbridge turntf login ok: session=%d/%s protocol=%s", info.SessionRef.ServingNodeID, info.SessionRef.SessionID, info.ProtocolVersion)
}

func (r *Runtime) OnMessage(_ context.Context, msg turntf.Message) {
	r.logger.Printf("qqbridge observed turntf message: cursor=%d/%d sender=%d:%d recipient=%d:%d", msg.NodeID, msg.Seq, msg.Sender.NodeID, msg.Sender.UserID, msg.Recipient.NodeID, msg.Recipient.UserID)
}

func (r *Runtime) OnPacket(_ context.Context, packet turntf.Packet) {
	r.logger.Printf("qqbridge ignored transient packet: packet=%d target=%d:%d", packet.PacketID, packet.Recipient.NodeID, packet.Recipient.UserID)
}

func (r *Runtime) OnError(_ context.Context, err error) {
	r.logger.Printf("qqbridge turntf error: %v", err)
}

func (r *Runtime) OnDisconnect(_ context.Context, err error) {
	r.logger.Printf("qqbridge turntf disconnected: %v", err)
}

func (r *Runtime) consumeGatewayEvents(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case event, ok := <-r.gateway.Events():
			if !ok {
				return
			}
			if err := r.store.EnqueueInboundEvent(ctx, event); err != nil {
				r.logger.Printf("qqbridge enqueue inbound event failed: %v", err)
			}
		}
	}
}

func (r *Runtime) runOutboundLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		job, err := r.store.ClaimOutboundJob(ctx)
		if err != nil {
			r.logger.Printf("qqbridge claim outbound job failed: %v", err)
			r.sleepOrDone(ctx, time.Second)
			continue
		}
		if job == nil {
			r.sleepOrDone(ctx, 500*time.Millisecond)
			continue
		}

		result, err := r.gateway.Send(ctx, outboundMessage{
			ConversationRef: job.Conversation,
			Content:         job.Envelope.Content,
			MessageRef:      job.SourceMessage,
		})
		if err == nil {
			if err := r.store.MarkOutboundDelivered(ctx, job.ID, result.RemoteMessageID); err != nil {
				r.logger.Printf("qqbridge mark outbound delivered failed: %v", err)
			}
			continue
		}

		bridgeErr := classifyBridgeError(err)
		if bridgeErr.Retryable {
			if err := r.store.RetryOutbound(ctx, job.ID, string(bridgeErr.Code), bridgeErr.Error(), job.Attempts); err != nil {
				r.logger.Printf("qqbridge retry outbound failed: %v", err)
			}
			continue
		}
		if err := r.store.FailOutbound(ctx, job.ID, string(bridgeErr.Code), bridgeErr.Error()); err != nil {
			r.logger.Printf("qqbridge fail outbound failed: %v", err)
		}
		if err := r.store.QueueReceipt(
			ctx,
			job.SourceSender,
			job.Conversation,
			job.SourceMessage,
			bridgeErr.Code,
			bridgeErr.Error(),
			fmt.Sprintf("receipt:%d:%d:delivery", job.SourceCursor.NodeID, job.SourceCursor.Seq),
		); err != nil {
			r.logger.Printf("qqbridge enqueue receipt failed: %v", err)
		}
	}
}

func (r *Runtime) runTurnTFDeliveryLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}
		job, err := r.store.ClaimTurnTFDeliveryJob(ctx)
		if err != nil {
			r.logger.Printf("qqbridge claim turntf delivery job failed: %v", err)
			r.sleepOrDone(ctx, time.Second)
			continue
		}
		if job == nil {
			r.sleepOrDone(ctx, 500*time.Millisecond)
			continue
		}
		body, err := json.Marshal(job.Envelope)
		if err != nil {
			if err := r.store.FailTurnTFDelivery(ctx, job.ID, string(ReceiptCodeDeliveryFailed), err.Error()); err != nil {
				r.logger.Printf("qqbridge fail turntf delivery marshal failed: %v", err)
			}
			continue
		}
		_, err = r.client.SendMessage(ctx, turntf.SendMessageInput{
			Target: job.Target,
			Body:   body,
		})
		if err == nil {
			if err := r.store.MarkTurnTFDeliveryDelivered(ctx, job.ID); err != nil {
				r.logger.Printf("qqbridge mark turntf delivery delivered failed: %v", err)
			}
			continue
		}

		bridgeErr := classifyTurnTFSendError(err)
		if bridgeErr.Retryable {
			if err := r.store.RetryTurnTFDelivery(ctx, job.ID, string(bridgeErr.Code), bridgeErr.Error(), job.Attempts); err != nil {
				r.logger.Printf("qqbridge retry turntf delivery failed: %v", err)
			}
			continue
		}
		if err := r.store.FailTurnTFDelivery(ctx, job.ID, string(bridgeErr.Code), bridgeErr.Error()); err != nil {
			r.logger.Printf("qqbridge fail turntf delivery failed: %v", err)
		}
	}
}

func (r *Runtime) sleepOrDone(ctx context.Context, d time.Duration) {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func classifyBridgeError(err error) *BridgeError {
	var bridgeErr *BridgeError
	if errors.As(err, &bridgeErr) {
		return bridgeErr
	}
	return retryableBridgeError(ReceiptCodePlatformUnavailable, "qq gateway unavailable", err)
}

func classifyTurnTFSendError(err error) *BridgeError {
	if err == nil {
		return nil
	}
	if errors.Is(err, turntf.ErrClosed) || errors.Is(err, turntf.ErrDisconnected) || errors.Is(err, turntf.ErrNotConnected) {
		return retryableBridgeError(ReceiptCodePlatformUnavailable, "turntf bridge user is offline", err)
	}
	var serverErr *turntf.ServerError
	if errors.As(err, &serverErr) {
		switch serverErr.Code {
		case "not_found":
			return terminalBridgeError(ReceiptCodeTargetNotFound, "turntf target user not found", err)
		case "forbidden", "unauthorized":
			return terminalBridgeError(ReceiptCodePermissionDenied, "turntf delivery forbidden", err)
		default:
			return retryableBridgeError(ReceiptCodeDeliveryFailed, "turntf send_message failed", err)
		}
	}
	if strings.Contains(err.Error(), "unauthorized") {
		return terminalBridgeError(ReceiptCodePermissionDenied, "turntf delivery forbidden", err)
	}
	return retryableBridgeError(ReceiptCodePlatformUnavailable, "turntf send_message unavailable", err)
}
