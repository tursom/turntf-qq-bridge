package qqbridge

import (
	"context"
	"log"
)

type gatewayCapabilities struct {
	supported map[SegmentKind]struct{}
}

func newGatewayCapabilities(kinds ...SegmentKind) gatewayCapabilities {
	supported := make(map[SegmentKind]struct{}, len(kinds))
	for _, kind := range kinds {
		supported[kind] = struct{}{}
	}
	return gatewayCapabilities{supported: supported}
}

func (c gatewayCapabilities) Supports(kind SegmentKind) bool {
	_, ok := c.supported[kind]
	return ok
}

func (c gatewayCapabilities) SupportsAll(content Content) error {
	for _, segment := range content.Segments {
		if c.Supports(segment.Kind) {
			continue
		}
		return terminalBridgeError(
			ReceiptCodeUnsupportedContent,
			"segment kind is not supported by the configured qq gateway",
			nil,
		)
	}
	return nil
}

type gatewaySendResult struct {
	RemoteMessageID string
}

type outboundMessage struct {
	ConversationRef ConversationRef
	Content         Content
	MessageRef      MessageRef
}

type QQGateway interface {
	Send(context.Context, outboundMessage) (gatewaySendResult, error)
	Events() <-chan GatewayInboundEvent
	Capabilities() gatewayCapabilities
	Close() error
}

func newGateway(ctx context.Context, cfg Config, logger *log.Logger) (QQGateway, error) {
	switch cfg.Backend.Kind {
	case "napcat":
		return newNapCatGateway(ctx, cfg.Backend.NapCat, logger)
	case "qqbot":
		return newQQBotGateway(ctx, cfg.Backend.QQBot, logger)
	default:
		return nil, terminalBridgeError(ReceiptCodePlatformUnavailable, "unsupported gateway backend kind", nil)
	}
}
