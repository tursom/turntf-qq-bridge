package qqbridge

import "fmt"

type ReceiptCode string

const (
	ReceiptCodePlatformUnavailable ReceiptCode = "platform_unavailable"
	ReceiptCodeTargetNotFound      ReceiptCode = "target_not_found"
	ReceiptCodePermissionDenied    ReceiptCode = "permission_denied"
	ReceiptCodeUnsupportedContent  ReceiptCode = "unsupported_content"
	ReceiptCodeRateLimited         ReceiptCode = "rate_limited"
	ReceiptCodeDeliveryFailed      ReceiptCode = "delivery_failed"
)

type BridgeError struct {
	Code      ReceiptCode
	Message   string
	Retryable bool
	Err       error
}

func (e *BridgeError) Error() string {
	if e == nil {
		return "<nil>"
	}
	if e.Message == "" && e.Err == nil {
		return string(e.Code)
	}
	if e.Message == "" {
		return fmt.Sprintf("%s: %v", e.Code, e.Err)
	}
	if e.Err == nil {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("%s: %s: %v", e.Code, e.Message, e.Err)
}

func (e *BridgeError) Unwrap() error {
	if e == nil {
		return nil
	}
	return e.Err
}

func retryableBridgeError(code ReceiptCode, message string, err error) *BridgeError {
	return &BridgeError{Code: code, Message: message, Retryable: true, Err: err}
}

func terminalBridgeError(code ReceiptCode, message string, err error) *BridgeError {
	return &BridgeError{Code: code, Message: message, Retryable: false, Err: err}
}
