package qqbridge

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/tencent-connect/botgo"
	"github.com/tencent-connect/botgo/dto"
	"github.com/tencent-connect/botgo/event"
	"github.com/tencent-connect/botgo/openapi"
	"github.com/tencent-connect/botgo/openapi/options"
	"github.com/tencent-connect/botgo/token"
	"golang.org/x/oauth2"
)

type qqBotAPI interface {
	PostGroupMessage(ctx context.Context, groupID string, msg dto.APIMessage, opt ...options.Option) (*dto.Message, error)
	PostC2CMessage(ctx context.Context, userID string, msg dto.APIMessage, opt ...options.Option) (*dto.Message, error)
	WS(ctx context.Context, params map[string]string, body string) (*dto.WebsocketAP, error)
}

type qqBotSessionManager interface {
	Start(apInfo *dto.WebsocketAP, tokenSource oauth2.TokenSource, intents *dto.Intent) error
}

type qqBotGateway struct {
	cfg          QQBotConfig
	logger       *log.Logger
	events       chan GatewayInboundEvent
	capabilities gatewayCapabilities
	api          qqBotAPI
	sessionMgr   qqBotSessionManager
	tokenSource  oauth2.TokenSource
	cancel       context.CancelFunc
}

func newQQBotGateway(parent context.Context, cfg QQBotConfig, logger *log.Logger) (QQGateway, error) {
	ctx, cancel := context.WithCancel(parent)
	credentials := &token.QQBotCredentials{
		AppID:     cfg.AppID,
		AppSecret: cfg.EffectiveSecret(),
	}
	tokenSource := token.NewQQBotTokenSource(credentials)
	if err := token.StartRefreshAccessToken(ctx, tokenSource); err != nil {
		cancel()
		return nil, err
	}

	var api openapi.OpenAPI
	if cfg.Sandbox {
		api = botgo.NewSandboxOpenAPI(cfg.AppID, tokenSource).WithTimeout(10 * time.Second)
	} else {
		api = botgo.NewOpenAPI(cfg.AppID, tokenSource).WithTimeout(10 * time.Second)
	}
	return newQQBotGatewayWithAPI(ctx, cancel, cfg, logger, api, botgo.NewSessionManager(), tokenSource)
}

func newQQBotGatewayWithAPI(ctx context.Context, cancel context.CancelFunc, cfg QQBotConfig, logger *log.Logger, api qqBotAPI, sessionMgr qqBotSessionManager, tokenSource oauth2.TokenSource) (QQGateway, error) {
	gateway := &qqBotGateway{
		cfg:    cfg,
		logger: logger,
		events: make(chan GatewayInboundEvent, 64),
		capabilities: newGatewayCapabilities(
			SegmentKindText,
			SegmentKindImage,
			SegmentKindAudio,
			SegmentKindVideo,
			SegmentKindReply,
		),
		api:         api,
		sessionMgr:  sessionMgr,
		tokenSource: tokenSource,
		cancel:      cancel,
	}

	intents, err := gateway.registerHandlers()
	if err != nil {
		cancel()
		return nil, err
	}

	apInfo, err := gateway.api.WS(ctx, nil, "")
	if err != nil {
		cancel()
		return nil, retryableBridgeError(ReceiptCodePlatformUnavailable, "qqbot websocket gateway lookup failed", err)
	}
	go func() {
		if err := gateway.sessionMgr.Start(apInfo, gateway.tokenSource, &intents); err != nil && ctx.Err() == nil {
			gateway.logger.Printf("qqbridge qqbot websocket manager stopped: %v", err)
		}
	}()
	gateway.logger.Printf("qqbridge qqbot websocket started: shards=%d url=%s", apInfo.Shards, apInfo.URL)
	return gateway, nil
}

func (g *qqBotGateway) registerHandlers() (dto.Intent, error) {
	intents := event.RegisterHandlers(
		event.GroupATMessageEventHandler(func(payload *dto.WSPayload, data *dto.WSGroupATMessageData) error {
			return g.handleGroupEvent(payload, (*dto.Message)(data))
		}),
		event.C2CMessageEventHandler(func(payload *dto.WSPayload, data *dto.WSC2CMessageData) error {
			return g.handleC2CEvent(payload, (*dto.Message)(data))
		}),
	)
	return intents, nil
}

func (g *qqBotGateway) Events() <-chan GatewayInboundEvent {
	return g.events
}

func (g *qqBotGateway) Capabilities() gatewayCapabilities {
	return g.capabilities
}

func (g *qqBotGateway) Close() error {
	g.cancel()
	return nil
}

func (g *qqBotGateway) Send(ctx context.Context, msg outboundMessage) (gatewaySendResult, error) {
	if err := msg.ConversationRef.NormalizeAndValidate(); err != nil {
		return gatewaySendResult{}, terminalBridgeError(ReceiptCodeUnsupportedContent, "conversation_ref is invalid", err)
	}
	if err := msg.Content.NormalizeAndValidate(); err != nil {
		return gatewaySendResult{}, terminalBridgeError(ReceiptCodeUnsupportedContent, "content is invalid", err)
	}
	if err := g.capabilities.SupportsAll(msg.Content); err != nil {
		return gatewaySendResult{}, err
	}

	apiMsg, err := buildQQBotAPIMessage(msg)
	if err != nil {
		return gatewaySendResult{}, err
	}
	switch msg.ConversationRef.Scene {
	case ConversationSceneGroup:
		resp, err := g.api.PostGroupMessage(ctx, msg.ConversationRef.ChatID, apiMsg)
		if err != nil {
			return gatewaySendResult{}, mapQQBotError(err)
		}
		return gatewaySendResult{RemoteMessageID: resp.ID}, nil
	case ConversationScenePrivate:
		resp, err := g.api.PostC2CMessage(ctx, msg.ConversationRef.ChatID, apiMsg)
		if err != nil {
			return gatewaySendResult{}, mapQQBotError(err)
		}
		return gatewaySendResult{RemoteMessageID: resp.ID}, nil
	default:
		return gatewaySendResult{}, terminalBridgeError(ReceiptCodeTargetNotFound, "unsupported qq conversation scene", nil)
	}
}

func (g *qqBotGateway) handleGroupEvent(_ *dto.WSPayload, message *dto.Message) error {
	return g.enqueueEvent(message, ConversationSceneGroup, message.GroupID)
}

func (g *qqBotGateway) handleC2CEvent(_ *dto.WSPayload, message *dto.Message) error {
	chatID := ""
	if message.Author != nil {
		chatID = message.Author.ID
	}
	return g.enqueueEvent(message, ConversationScenePrivate, chatID)
}

func (g *qqBotGateway) enqueueEvent(message *dto.Message, scene ConversationScene, chatID string) error {
	event, err := normalizeQQBotMessage(message, scene, chatID)
	if err != nil {
		return err
	}
	select {
	case g.events <- event:
		return nil
	default:
		return retryableBridgeError(ReceiptCodePlatformUnavailable, "qqbot inbound event channel is full", nil)
	}
}

func normalizeQQBotMessage(message *dto.Message, scene ConversationScene, chatID string) (GatewayInboundEvent, error) {
	conversation := ConversationRef{
		Platform: "qq",
		Scene:    scene,
		ChatID:   strings.TrimSpace(chatID),
	}
	if err := conversation.NormalizeAndValidate(); err != nil {
		return GatewayInboundEvent{}, err
	}

	segments := make([]Segment, 0, 1+len(message.Attachments))
	if text := strings.TrimSpace(message.Content); text != "" {
		segments = append(segments, Segment{Kind: SegmentKindText, Text: text})
	}
	for _, attachment := range message.Attachments {
		switch {
		case strings.HasPrefix(attachment.ContentType, "image/"):
			segments = append(segments, Segment{Kind: SegmentKindImage, URL: attachment.URL, FileName: attachment.FileName, MIME: attachment.ContentType})
		case strings.HasPrefix(attachment.ContentType, "video/"):
			segments = append(segments, Segment{Kind: SegmentKindVideo, URL: attachment.URL, FileName: attachment.FileName, MIME: attachment.ContentType})
		case strings.HasPrefix(attachment.ContentType, "voice"), strings.HasPrefix(attachment.ContentType, "audio/"):
			segments = append(segments, Segment{Kind: SegmentKindAudio, URL: attachment.URL, FileName: attachment.FileName, MIME: attachment.ContentType})
		default:
			segments = append(segments, Segment{Kind: SegmentKindFile, URL: attachment.URL, FileName: attachment.FileName, MIME: attachment.ContentType})
		}
	}
	if len(segments) == 0 {
		return GatewayInboundEvent{}, terminalBridgeError(ReceiptCodeUnsupportedContent, "qqbot message has no supported content", nil)
	}

	var remoteSender *RemoteSender
	if message.Author != nil {
		remoteSender = &RemoteSender{
			ID:        message.Author.ID,
			Nickname:  message.Author.Username,
			AvatarURL: message.Author.Avatar,
		}
	}
	if message.Member != nil && remoteSender != nil {
		remoteSender.Remark = message.Member.Nick
	}
	envelope := BridgeEnvelope{
		Version:         EnvelopeVersion,
		Kind:            EnvelopeKindChat,
		ConversationRef: conversation,
		Content:         Content{Segments: segments},
		RemoteSender:    remoteSender,
		MessageRef:      MessageRef{ID: message.ID},
		Metadata: Metadata{
			"platform": "qq",
			"scene":    string(scene),
		},
	}
	if err := envelope.NormalizeAndValidate(); err != nil {
		return GatewayInboundEvent{}, err
	}
	return GatewayInboundEvent{
		GatewayMessageID: message.ID,
		Envelope:         envelope,
	}, nil
}

func buildQQBotAPIMessage(msg outboundMessage) (dto.APIMessage, error) {
	var (
		replyTo string
		text    strings.Builder
		media   *Segment
	)
	for _, segment := range msg.Content.Segments {
		switch segment.Kind {
		case SegmentKindReply:
			if replyTo != "" {
				return nil, terminalBridgeError(ReceiptCodeUnsupportedContent, "multiple reply segments are not supported by qqbot", nil)
			}
			replyTo = segment.MessageID
		case SegmentKindText:
			text.WriteString(segment.Text)
		case SegmentKindImage, SegmentKindAudio, SegmentKindVideo:
			if media != nil {
				return nil, terminalBridgeError(ReceiptCodeUnsupportedContent, "multiple media segments are not supported by qqbot", nil)
			}
			copySegment := segment
			media = &copySegment
		default:
			return nil, terminalBridgeError(ReceiptCodeUnsupportedContent, "segment kind is not supported by qqbot", nil)
		}
	}

	if media != nil {
		if replyTo != "" {
			return nil, terminalBridgeError(ReceiptCodeUnsupportedContent, "reply with rich media is not supported by qqbot", nil)
		}
		fileType := uint64(1)
		switch media.Kind {
		case SegmentKindImage:
			fileType = 1
		case SegmentKindVideo:
			fileType = 2
		case SegmentKindAudio:
			fileType = 3
		}
		return dto.RichMediaMessage{
			FileType:   fileType,
			URL:        media.URL,
			SrvSendMsg: true,
			Content:    text.String(),
		}, nil
	}

	content := strings.TrimSpace(text.String())
	if content == "" {
		return nil, terminalBridgeError(ReceiptCodeUnsupportedContent, "qqbot text message content is empty", nil)
	}
	message := dto.MessageToCreate{
		Content: content,
		MsgType: dto.TextMsg,
	}
	if replyTo != "" {
		message.MessageReference = &dto.MessageReference{
			MessageID:             replyTo,
			IgnoreGetMessageError: true,
		}
	}
	return message, nil
}

func mapQQBotError(err error) error {
	if err == nil {
		return nil
	}
	message := err.Error()
	switch {
	case strings.Contains(message, "429"):
		return retryableBridgeError(ReceiptCodeRateLimited, "qqbot rate limited", err)
	case strings.Contains(message, "404"):
		return terminalBridgeError(ReceiptCodeTargetNotFound, "qq target not found", err)
	case strings.Contains(message, "403"):
		return terminalBridgeError(ReceiptCodePermissionDenied, "qqbot permission denied", err)
	case strings.Contains(message, "401"), strings.Contains(message, "token"):
		return retryableBridgeError(ReceiptCodePlatformUnavailable, "qqbot token unavailable", err)
	default:
		return terminalBridgeError(ReceiptCodeDeliveryFailed, "qqbot send failed", err)
	}
}
