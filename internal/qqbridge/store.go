package qqbridge

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	turntf "github.com/tursom/turntf-go"
)

const (
	jobStatusPending    = "pending"
	jobStatusProcessing = "processing"
	jobStatusDelivered  = "delivered"
	jobStatusFailed     = "failed"
)

var nowFunc = time.Now

type Store struct {
	db         *sql.DB
	bridgeUser turntf.UserRef
}

type GatewayInboundEvent struct {
	GatewayMessageID string
	Envelope         BridgeEnvelope
}

type OutboundJob struct {
	ID            int64
	SourceCursor  turntf.MessageCursor
	SourceSender  turntf.UserRef
	Conversation  ConversationRef
	Envelope      BridgeEnvelope
	Attempts      int
	SourceMessage MessageRef
	LastErrorCode string
	LastErrorText string
}

type TurnTFDeliveryJob struct {
	ID        int64
	JobKey    string
	Kind      string
	Target    turntf.UserRef
	Envelope  BridgeEnvelope
	Attempts  int
	LastError string
}

func OpenStore(path string, bridgeUser turntf.UserRef) (*Store, error) {
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("sqlite path is required")
	}
	if path != ":memory:" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}
	if err := initStore(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{
		db:         db,
		bridgeUser: bridgeUser,
	}, nil
}

func initStore(db *sql.DB) error {
	stmts := []string{
		`PRAGMA busy_timeout = 5000;`,
		`PRAGMA journal_mode = WAL;`,
		`CREATE TABLE IF NOT EXISTS seen_messages (
			node_id INTEGER NOT NULL,
			seq INTEGER NOT NULL,
			PRIMARY KEY(node_id, seq)
		);`,
		`CREATE TABLE IF NOT EXISTS turntf_messages (
			node_id INTEGER NOT NULL,
			seq INTEGER NOT NULL,
			recipient_node_id INTEGER NOT NULL,
			recipient_user_id INTEGER NOT NULL,
			sender_node_id INTEGER NOT NULL,
			sender_user_id INTEGER NOT NULL,
			body BLOB NOT NULL,
			created_at_hlc TEXT NOT NULL,
			saved_at_ms INTEGER NOT NULL,
			PRIMARY KEY(node_id, seq)
		);`,
		`CREATE TABLE IF NOT EXISTS session_bindings (
			bridge_node_id INTEGER NOT NULL,
			bridge_user_id INTEGER NOT NULL,
			local_node_id INTEGER NOT NULL,
			local_user_id INTEGER NOT NULL,
			conversation_key TEXT NOT NULL,
			conversation_json BLOB NOT NULL,
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL,
			PRIMARY KEY(bridge_node_id, bridge_user_id, local_node_id, local_user_id, conversation_key)
		);`,
		`CREATE TABLE IF NOT EXISTS outbound_jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			source_node_id INTEGER NOT NULL,
			source_seq INTEGER NOT NULL,
			source_sender_node_id INTEGER NOT NULL,
			source_sender_user_id INTEGER NOT NULL,
			conversation_key TEXT NOT NULL,
			envelope_json BLOB NOT NULL,
			status TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			next_attempt_at_ms INTEGER NOT NULL,
			remote_message_id TEXT NOT NULL DEFAULT '',
			last_error_code TEXT NOT NULL DEFAULT '',
			last_error_message TEXT NOT NULL DEFAULT '',
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL,
			UNIQUE(source_node_id, source_seq)
		);`,
		`CREATE INDEX IF NOT EXISTS idx_outbound_jobs_pending ON outbound_jobs(status, next_attempt_at_ms, id);`,
		`CREATE TABLE IF NOT EXISTS inbound_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			gateway_message_id TEXT NOT NULL UNIQUE,
			conversation_key TEXT NOT NULL,
			envelope_json BLOB NOT NULL,
			status TEXT NOT NULL,
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS orphan_inbound_events (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			gateway_message_id TEXT NOT NULL UNIQUE,
			conversation_key TEXT NOT NULL,
			envelope_json BLOB NOT NULL,
			created_at_ms INTEGER NOT NULL
		);`,
		`CREATE TABLE IF NOT EXISTS turntf_delivery_jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			job_key TEXT NOT NULL UNIQUE,
			kind TEXT NOT NULL,
			target_local_node_id INTEGER NOT NULL,
			target_local_user_id INTEGER NOT NULL,
			envelope_json BLOB NOT NULL,
			status TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			next_attempt_at_ms INTEGER NOT NULL,
			last_error_code TEXT NOT NULL DEFAULT '',
			last_error_message TEXT NOT NULL DEFAULT '',
			created_at_ms INTEGER NOT NULL,
			updated_at_ms INTEGER NOT NULL
		);`,
		`CREATE INDEX IF NOT EXISTS idx_turntf_delivery_jobs_pending ON turntf_delivery_jobs(status, next_attempt_at_ms, id);`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) LoadSeenMessages(context.Context) ([]turntf.MessageCursor, error) {
	rows, err := s.db.Query(`SELECT node_id, seq FROM seen_messages ORDER BY node_id, seq`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []turntf.MessageCursor
	for rows.Next() {
		var cursor turntf.MessageCursor
		if err := rows.Scan(&cursor.NodeID, &cursor.Seq); err != nil {
			return nil, err
		}
		out = append(out, cursor)
	}
	return out, rows.Err()
}

func (s *Store) SaveMessage(ctx context.Context, msg turntf.Message) error {
	nowMS := nowFunc().UnixMilli()
	return s.withTx(ctx, func(tx *sql.Tx) error {
		res, err := tx.ExecContext(
			ctx,
			`INSERT OR IGNORE INTO turntf_messages (
				node_id, seq, recipient_node_id, recipient_user_id, sender_node_id, sender_user_id, body, created_at_hlc, saved_at_ms
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			msg.NodeID,
			msg.Seq,
			msg.Recipient.NodeID,
			msg.Recipient.UserID,
			msg.Sender.NodeID,
			msg.Sender.UserID,
			msg.Body,
			msg.CreatedAtHLC,
			nowMS,
		)
		if err != nil {
			return err
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			return nil
		}
		if msg.Recipient != s.bridgeUser || msg.Sender == s.bridgeUser {
			return nil
		}

		env, err := ParseEnvelope(msg.Body)
		if err != nil {
			return s.enqueueReceiptTx(
				tx,
				msg.Sender,
				fallbackReceiptConversation(),
				MessageRef{ID: messageIDFromCursor(msg.Cursor())},
				ReceiptCodeUnsupportedContent,
				fmt.Sprintf("bridge message format invalid: %v", err),
				nowMS,
				fmt.Sprintf("receipt:%d:%d:invalid", msg.NodeID, msg.Seq),
			)
		}
		if env.Kind != EnvelopeKindChat {
			return s.enqueueReceiptTx(
				tx,
				msg.Sender,
				env.ConversationRef,
				env.MessageRef,
				ReceiptCodeUnsupportedContent,
				fmt.Sprintf("bridge message kind %q is not supported for outbound chat", env.Kind),
				nowMS,
				fmt.Sprintf("receipt:%d:%d:kind", msg.NodeID, msg.Seq),
			)
		}

		if err := s.upsertBindingTx(tx, msg.Sender, env.ConversationRef, nowMS); err != nil {
			return err
		}
		raw, err := json.Marshal(env)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(
			ctx,
			`INSERT OR IGNORE INTO outbound_jobs (
				source_node_id, source_seq, source_sender_node_id, source_sender_user_id, conversation_key, envelope_json,
				status, attempts, next_attempt_at_ms, created_at_ms, updated_at_ms
			) VALUES (?, ?, ?, ?, ?, ?, ?, 0, ?, ?, ?)`,
			msg.NodeID,
			msg.Seq,
			msg.Sender.NodeID,
			msg.Sender.UserID,
			env.ConversationRef.Key(),
			raw,
			jobStatusPending,
			nowMS,
			nowMS,
			nowMS,
		)
		return err
	})
}

func (s *Store) SaveCursor(ctx context.Context, cursor turntf.MessageCursor) error {
	_, err := s.db.ExecContext(
		ctx,
		`INSERT OR IGNORE INTO seen_messages (node_id, seq) VALUES (?, ?)`,
		cursor.NodeID,
		cursor.Seq,
	)
	return err
}

func (s *Store) EnqueueInboundEvent(ctx context.Context, event GatewayInboundEvent) error {
	nowMS := nowFunc().UnixMilli()
	return s.withTx(ctx, func(tx *sql.Tx) error {
		if err := event.Envelope.NormalizeAndValidate(); err != nil {
			return err
		}
		if event.Envelope.Kind != EnvelopeKindChat {
			return fmt.Errorf("gateway inbound event must use kind=chat")
		}
		gatewayMessageID := strings.TrimSpace(event.GatewayMessageID)
		if gatewayMessageID == "" {
			gatewayMessageID = event.Envelope.MessageRef.ID
		}
		if gatewayMessageID == "" {
			gatewayMessageID = "event:" + event.Envelope.ConversationRef.Hash() + ":" + messageIDFromCursor(turntf.MessageCursor{
				NodeID: 0,
				Seq:    nowMS,
			})
		}
		raw, err := json.Marshal(event.Envelope)
		if err != nil {
			return err
		}
		res, err := tx.ExecContext(
			ctx,
			`INSERT OR IGNORE INTO inbound_events (
				gateway_message_id, conversation_key, envelope_json, status, created_at_ms, updated_at_ms
			) VALUES (?, ?, ?, ?, ?, ?)`,
			gatewayMessageID,
			event.Envelope.ConversationRef.Key(),
			raw,
			jobStatusPending,
			nowMS,
			nowMS,
		)
		if err != nil {
			return err
		}
		rowsAffected, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if rowsAffected == 0 {
			return nil
		}

		var inboundEventID int64
		if err := tx.QueryRowContext(ctx, `SELECT id FROM inbound_events WHERE gateway_message_id = ?`, gatewayMessageID).Scan(&inboundEventID); err != nil {
			return err
		}

		bindings, err := s.listBindingsByConversationKeyTx(ctx, tx, event.Envelope.ConversationRef.Key())
		if err != nil {
			return err
		}
		if len(bindings) == 0 {
			if _, err := tx.ExecContext(
				ctx,
				`INSERT OR IGNORE INTO orphan_inbound_events (
					gateway_message_id, conversation_key, envelope_json, created_at_ms
				) VALUES (?, ?, ?, ?)`,
				gatewayMessageID,
				event.Envelope.ConversationRef.Key(),
				raw,
				nowMS,
			); err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `UPDATE inbound_events SET status = ?, updated_at_ms = ? WHERE id = ?`, jobStatusFailed, nowMS, inboundEventID)
			return err
		}

		for _, binding := range bindings {
			jobKey := fmt.Sprintf("inbound:%d:%d:%d", inboundEventID, binding.NodeID, binding.UserID)
			if err := s.enqueueTurnTFDeliveryTx(tx, jobKey, "inbound", binding, event.Envelope, nowMS); err != nil {
				return err
			}
		}
		_, err = tx.ExecContext(ctx, `UPDATE inbound_events SET status = ?, updated_at_ms = ? WHERE id = ?`, jobStatusDelivered, nowMS, inboundEventID)
		return err
	})
}

func (s *Store) QueueReceipt(ctx context.Context, target turntf.UserRef, conversation ConversationRef, messageRef MessageRef, code ReceiptCode, text, jobKey string) error {
	nowMS := nowFunc().UnixMilli()
	return s.withTx(ctx, func(tx *sql.Tx) error {
		return s.enqueueReceiptTx(tx, target, conversation, messageRef, code, text, nowMS, jobKey)
	})
}

func (s *Store) ClaimOutboundJob(ctx context.Context) (*OutboundJob, error) {
	nowMS := nowFunc().UnixMilli()
	return s.claimOutboundJob(ctx, nowMS)
}

func (s *Store) ClaimTurnTFDeliveryJob(ctx context.Context) (*TurnTFDeliveryJob, error) {
	nowMS := nowFunc().UnixMilli()
	return s.claimTurnTFDeliveryJob(ctx, nowMS)
}

func (s *Store) MarkOutboundDelivered(ctx context.Context, id int64, remoteMessageID string) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE outbound_jobs
		SET status = ?, remote_message_id = ?, updated_at_ms = ?
		WHERE id = ?`,
		jobStatusDelivered,
		remoteMessageID,
		nowFunc().UnixMilli(),
		id,
	)
	return err
}

func (s *Store) RetryOutbound(ctx context.Context, id int64, code, message string, attempts int) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE outbound_jobs
		SET status = ?, next_attempt_at_ms = ?, updated_at_ms = ?, last_error_code = ?, last_error_message = ?
		WHERE id = ?`,
		jobStatusPending,
		nowFunc().Add(backoffForAttempt(attempts)).UnixMilli(),
		nowFunc().UnixMilli(),
		code,
		message,
		id,
	)
	return err
}

func (s *Store) FailOutbound(ctx context.Context, id int64, code, message string) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE outbound_jobs
		SET status = ?, updated_at_ms = ?, last_error_code = ?, last_error_message = ?
		WHERE id = ?`,
		jobStatusFailed,
		nowFunc().UnixMilli(),
		code,
		message,
		id,
	)
	return err
}

func (s *Store) MarkTurnTFDeliveryDelivered(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE turntf_delivery_jobs
		SET status = ?, updated_at_ms = ?
		WHERE id = ?`,
		jobStatusDelivered,
		nowFunc().UnixMilli(),
		id,
	)
	return err
}

func (s *Store) RetryTurnTFDelivery(ctx context.Context, id int64, code, message string, attempts int) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE turntf_delivery_jobs
		SET status = ?, next_attempt_at_ms = ?, updated_at_ms = ?, last_error_code = ?, last_error_message = ?
		WHERE id = ?`,
		jobStatusPending,
		nowFunc().Add(backoffForAttempt(attempts)).UnixMilli(),
		nowFunc().UnixMilli(),
		code,
		message,
		id,
	)
	return err
}

func (s *Store) FailTurnTFDelivery(ctx context.Context, id int64, code, message string) error {
	_, err := s.db.ExecContext(
		ctx,
		`UPDATE turntf_delivery_jobs
		SET status = ?, updated_at_ms = ?, last_error_code = ?, last_error_message = ?
		WHERE id = ?`,
		jobStatusFailed,
		nowFunc().UnixMilli(),
		code,
		message,
		id,
	)
	return err
}

func (s *Store) claimOutboundJob(ctx context.Context, nowMS int64) (*OutboundJob, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(
		ctx,
		`SELECT id, source_node_id, source_seq, source_sender_node_id, source_sender_user_id, envelope_json, attempts, last_error_code, last_error_message
		FROM outbound_jobs
		WHERE status = ? AND next_attempt_at_ms <= ?
		ORDER BY id
		LIMIT 1`,
		jobStatusPending,
		nowMS,
	)
	var (
		job OutboundJob
		raw []byte
	)
	if err := row.Scan(
		&job.ID,
		&job.SourceCursor.NodeID,
		&job.SourceCursor.Seq,
		&job.SourceSender.NodeID,
		&job.SourceSender.UserID,
		&raw,
		&job.Attempts,
		&job.LastErrorCode,
		&job.LastErrorText,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(raw, &job.Envelope); err != nil {
		return nil, err
	}
	job.Conversation = job.Envelope.ConversationRef
	job.SourceMessage = job.Envelope.MessageRef

	if _, err := tx.ExecContext(
		ctx,
		`UPDATE outbound_jobs SET status = ?, attempts = attempts + 1, updated_at_ms = ? WHERE id = ?`,
		jobStatusProcessing,
		nowMS,
		job.ID,
	); err != nil {
		return nil, err
	}
	job.Attempts++
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &job, nil
}

func (s *Store) claimTurnTFDeliveryJob(ctx context.Context, nowMS int64) (*TurnTFDeliveryJob, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	row := tx.QueryRowContext(
		ctx,
		`SELECT id, job_key, kind, target_local_node_id, target_local_user_id, envelope_json, attempts, last_error_message
		FROM turntf_delivery_jobs
		WHERE status = ? AND next_attempt_at_ms <= ?
		ORDER BY id
		LIMIT 1`,
		jobStatusPending,
		nowMS,
	)
	var (
		job TurnTFDeliveryJob
		raw []byte
	)
	if err := row.Scan(
		&job.ID,
		&job.JobKey,
		&job.Kind,
		&job.Target.NodeID,
		&job.Target.UserID,
		&raw,
		&job.Attempts,
		&job.LastError,
	); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	if err := json.Unmarshal(raw, &job.Envelope); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(
		ctx,
		`UPDATE turntf_delivery_jobs SET status = ?, attempts = attempts + 1, updated_at_ms = ? WHERE id = ?`,
		jobStatusProcessing,
		nowMS,
		job.ID,
	); err != nil {
		return nil, err
	}
	job.Attempts++
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return &job, nil
}

func (s *Store) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) upsertBindingTx(tx *sql.Tx, sender turntf.UserRef, conversation ConversationRef, nowMS int64) error {
	rawConversation, err := json.Marshal(conversation)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		`INSERT INTO session_bindings (
			bridge_node_id, bridge_user_id, local_node_id, local_user_id, conversation_key, conversation_json, created_at_ms, updated_at_ms
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(bridge_node_id, bridge_user_id, local_node_id, local_user_id, conversation_key)
		DO UPDATE SET conversation_json = excluded.conversation_json, updated_at_ms = excluded.updated_at_ms`,
		s.bridgeUser.NodeID,
		s.bridgeUser.UserID,
		sender.NodeID,
		sender.UserID,
		conversation.Key(),
		rawConversation,
		nowMS,
		nowMS,
	)
	return err
}

func (s *Store) listBindingsByConversationKeyTx(ctx context.Context, tx *sql.Tx, conversationKey string) ([]turntf.UserRef, error) {
	rows, err := tx.QueryContext(
		ctx,
		`SELECT local_node_id, local_user_id
		FROM session_bindings
		WHERE bridge_node_id = ? AND bridge_user_id = ? AND conversation_key = ?
		ORDER BY local_node_id, local_user_id`,
		s.bridgeUser.NodeID,
		s.bridgeUser.UserID,
		conversationKey,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []turntf.UserRef
	for rows.Next() {
		var ref turntf.UserRef
		if err := rows.Scan(&ref.NodeID, &ref.UserID); err != nil {
			return nil, err
		}
		out = append(out, ref)
	}
	return out, rows.Err()
}

func (s *Store) enqueueReceiptTx(tx *sql.Tx, target turntf.UserRef, conversation ConversationRef, messageRef MessageRef, code ReceiptCode, text string, nowMS int64, jobKey string) error {
	if err := conversation.NormalizeAndValidate(); err != nil {
		conversation = fallbackReceiptConversation()
	}
	envelope := NewReceiptEnvelope(conversation, messageRef, code, text)
	return s.enqueueTurnTFDeliveryTx(tx, jobKey, "receipt", target, envelope, nowMS)
}

func (s *Store) enqueueTurnTFDeliveryTx(tx *sql.Tx, jobKey, kind string, target turntf.UserRef, envelope BridgeEnvelope, nowMS int64) error {
	if err := envelope.NormalizeAndValidate(); err != nil {
		return err
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		`INSERT OR IGNORE INTO turntf_delivery_jobs (
			job_key, kind, target_local_node_id, target_local_user_id, envelope_json,
			status, attempts, next_attempt_at_ms, created_at_ms, updated_at_ms
		) VALUES (?, ?, ?, ?, ?, ?, 0, ?, ?, ?)`,
		jobKey,
		kind,
		target.NodeID,
		target.UserID,
		raw,
		jobStatusPending,
		nowMS,
		nowMS,
		nowMS,
	)
	return err
}

func messageIDFromCursor(cursor turntf.MessageCursor) string {
	return fmt.Sprintf("%d:%d", cursor.NodeID, cursor.Seq)
}

func fallbackReceiptConversation() ConversationRef {
	return ConversationRef{
		Platform: "qq",
		Scene:    ConversationScenePrivate,
		ChatID:   "__bridge_receipt__",
	}
}

func backoffForAttempt(attempt int) time.Duration {
	if attempt <= 1 {
		return time.Second
	}
	if attempt > 6 {
		attempt = 6
	}
	return time.Duration(1<<uint(attempt-1)) * time.Second
}
