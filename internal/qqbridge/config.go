package qqbridge

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	turntf "github.com/tursom/turntf-go"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Storage StorageConfig `yaml:"storage"`
	TurnTF  TurnTFConfig  `yaml:"turntf"`
	Backend BackendConfig `yaml:"backend"`
	Relay   RelayConfig   `yaml:"relay"`
}

type RelayConfig struct {
	PeerBridges []PeerBridgeConfig `yaml:"peer_bridges"`
}

type PeerBridgeConfig struct {
	PeerNodeID      int64                `yaml:"peer_node_id"`
	PeerUserID      int64                `yaml:"peer_user_id"`
	FromConversation ConversationRef     `yaml:"from_conversation"`
	ToConversation   ConversationRef     `yaml:"to_conversation"`
}

type StorageConfig struct {
	SQLitePath string `yaml:"sqlite_path"`
}

type TurnTFConfig struct {
	BaseURL    string           `yaml:"base_url"`
	BridgeUser BridgeUserConfig `yaml:"bridge_user"`
}

type BridgeUserConfig struct {
	NodeID   int64          `yaml:"node_id"`
	UserID   int64          `yaml:"user_id"`
	Password PasswordConfig `yaml:"password"`
}

type PasswordConfig struct {
	turntf.PasswordInput
}

type BackendConfig struct {
	Kind   string       `yaml:"kind"`
	NapCat NapCatConfig `yaml:"napcat"`
	QQBot  QQBotConfig  `yaml:"qqbot"`
}

type NapCatConfig struct {
	WSURL       string `yaml:"ws_url"`
	AccessToken string `yaml:"access_token"`
	SelfID      string `yaml:"self_id"`
}

type QQBotConfig struct {
	AppID   string `yaml:"app_id"`
	Token   string `yaml:"token"`
	Secret  string `yaml:"secret"`
	Sandbox bool   `yaml:"sandbox"`
}

func (p *PasswordConfig) UnmarshalYAML(node *yaml.Node) error {
	var raw struct {
		Source string `yaml:"source"`
		Value  string `yaml:"value"`
	}
	if err := node.Decode(&raw); err != nil {
		return err
	}
	switch strings.TrimSpace(raw.Source) {
	case string(turntf.PasswordSourcePlain):
		password, err := turntf.PlainPassword(raw.Value)
		if err != nil {
			return err
		}
		p.PasswordInput = password
	case string(turntf.PasswordSourceHashed):
		password := turntf.HashedPassword(raw.Value)
		if err := password.Validate(); err != nil {
			return err
		}
		p.PasswordInput = password
	default:
		return fmt.Errorf("password.source must be %q or %q", turntf.PasswordSourcePlain, turntf.PasswordSourceHashed)
	}
	return nil
}

func LoadConfig(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, err
	}
	var cfg Config
	decoder := yaml.NewDecoder(strings.NewReader(string(data)))
	decoder.KnownFields(true)
	if err := decoder.Decode(&cfg); err != nil {
		return Config{}, err
	}
	cfg.applyDefaults(path)
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c *Config) applyDefaults(path string) {
	if c == nil {
		return
	}
	if strings.TrimSpace(c.Storage.SQLitePath) != "" && !filepath.IsAbs(c.Storage.SQLitePath) && path != "" {
		c.Storage.SQLitePath = filepath.Join(filepath.Dir(path), c.Storage.SQLitePath)
	}
}

func (c Config) Validate() error {
	if strings.TrimSpace(c.Storage.SQLitePath) == "" {
		return fmt.Errorf("storage.sqlite_path is required")
	}
	if strings.TrimSpace(c.TurnTF.BaseURL) == "" {
		return fmt.Errorf("turntf.base_url is required")
	}
	if c.TurnTF.BridgeUser.NodeID == 0 || c.TurnTF.BridgeUser.UserID == 0 {
		return fmt.Errorf("turntf.bridge_user.node_id and user_id are required")
	}
	if err := c.TurnTF.BridgeUser.Password.Validate(); err != nil {
		return fmt.Errorf("turntf.bridge_user.password: %w", err)
	}

	switch strings.TrimSpace(c.Backend.Kind) {
	case "napcat":
		if strings.TrimSpace(c.Backend.NapCat.WSURL) == "" {
			return fmt.Errorf("backend.napcat.ws_url is required")
		}
	case "qqbot":
		if strings.TrimSpace(c.Backend.QQBot.AppID) == "" {
			return fmt.Errorf("backend.qqbot.app_id is required")
		}
		if strings.TrimSpace(c.Backend.QQBot.EffectiveSecret()) == "" {
			return fmt.Errorf("backend.qqbot.secret or token is required")
		}
	default:
		return fmt.Errorf("backend.kind must be napcat or qqbot")
	}

	for i, peer := range c.Relay.PeerBridges {
		if peer.PeerNodeID == 0 || peer.PeerUserID == 0 {
			return fmt.Errorf("relay.peer_bridges[%d].peer_node_id and peer_user_id are required", i)
		}
		if err := peer.FromConversation.NormalizeAndValidate(); err != nil {
			return fmt.Errorf("relay.peer_bridges[%d].from_conversation: %w", i, err)
		}
		if err := peer.ToConversation.NormalizeAndValidate(); err != nil {
			return fmt.Errorf("relay.peer_bridges[%d].to_conversation: %w", i, err)
		}
	}
	return nil
}

func (c Config) BridgeUserRef() turntf.UserRef {
	return turntf.UserRef{
		NodeID: c.TurnTF.BridgeUser.NodeID,
		UserID: c.TurnTF.BridgeUser.UserID,
	}
}

func (q QQBotConfig) EffectiveSecret() string {
	if strings.TrimSpace(q.Secret) != "" {
		return strings.TrimSpace(q.Secret)
	}
	return strings.TrimSpace(q.Token)
}
