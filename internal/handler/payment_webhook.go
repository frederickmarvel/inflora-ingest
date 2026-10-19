package handler

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"strings"

	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxPaymentWebhookBody = 1 << 20 // 1 MiB

type topUpSettler interface {
	SettleTopUp(context.Context, *pb.SettleTopUpRequest) (*pb.SettleTopUpResponse, error)
}

type withdrawalSettler interface {
	SettleWithdrawal(context.Context, *pb.SettleWithdrawalRequest) (*pb.SettleWithdrawalResponse, error)
}

// PaymentWebhook is the public, stateless provider-webhook edge. Signature
// verification and durable idempotency deliberately live in Palantir.
type PaymentWebhook struct{ Palantir interface{ topUpSettler; withdrawalSettler } }

func NewPaymentWebhook(p interface{ topUpSettler; withdrawalSettler }) *PaymentWebhook {
	return &PaymentWebhook{Palantir: p}
}

func (h *PaymentWebhook) Handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
	if err != nil || mediaType != "application/json" {
		writeJSON(w, http.StatusUnsupportedMediaType, map[string]any{"error": map[string]any{"code": "UNSUPPORTED_MEDIA_TYPE", "message": "Content-Type must be application/json"}})
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxPaymentWebhookBody)
	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			writeJSON(w, http.StatusRequestEntityTooLarge, map[string]any{"error": map[string]any{"code": "PAYLOAD_TOO_LARGE", "message": "webhook body exceeds 1 MiB"}})
			return
		}
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"code": "VALIDATION_FAILED", "message": "unable to read webhook body"}})
		return
	}
	providerName, eventID, chargeID, signature, err := extractPaymentWebhook(r.Header, body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"code": "VALIDATION_FAILED", "message": err.Error()}})
		return
	}
	if isPayoutWebhook(providerName, body) {
		h.settleWithdrawal(w, r, providerName, eventID, chargeID, signature, body)
		return
	}
	_, err = h.Palantir.SettleTopUp(r.Context(), &pb.SettleTopUpRequest{
		ProviderEventId: eventID,
		ProviderName:    providerName,
		ChargeId:        chargeID,
		RawPayload:      body,
		Signature:       signature,
		Headers:         flattenHeaders(r.Header),
	})
	if err != nil {
		code, httpStatus := "PROVIDER_ERROR", http.StatusBadGateway
		switch status.Code(err) {
		case codes.InvalidArgument, codes.NotFound:
			code, httpStatus = "VALIDATION_FAILED", http.StatusBadRequest
		case codes.Unauthenticated, codes.PermissionDenied:
			code, httpStatus = "INVALID_SIGNATURE", http.StatusUnauthorized
		case codes.AlreadyExists:
			code, httpStatus = "IDEMPOTENCY_CONFLICT", http.StatusConflict
		}
		writeJSON(w, httpStatus, map[string]any{"error": map[string]any{"code": code, "message": status.Convert(err).Message()}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// settleWithdrawal forwards a provider payout webhook to Palantir's
// SettleWithdrawal (T15). The mirror + webhook_events dedupe live in
// Palantir; this edge only routes and maps errors.
func (h *PaymentWebhook) settleWithdrawal(w http.ResponseWriter, r *http.Request, providerName, eventID, chargeID, signature string, body []byte) {
	var payload map[string]json.RawMessage
	if json.Unmarshal(body, &payload) != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"code": "VALIDATION_FAILED", "message": "webhook body must be a JSON object"}})
		return
	}
	get := func(name string) string {
		var value string
		_ = json.Unmarshal(payload[name], &value)
		return strings.TrimSpace(value)
	}
	providerPayoutID := firstNonEmpty(get("withdrawal_id"), get("transaction_id"), chargeID)
	sarumanPayoutID := get("payout_id")
	_, err := h.Palantir.SettleWithdrawal(r.Context(), &pb.SettleWithdrawalRequest{
		ProviderPayoutId:  providerPayoutID,
		ProviderEventId:   eventID,
		RawPayload:        body,
		ProviderName:      providerName,
		Signature:         signature,
		Headers:           flattenHeaders(r.Header),
		SarumanPayoutId:   sarumanPayoutID,
	})
	if err != nil {
		code, httpStatus := "PROVIDER_ERROR", http.StatusBadGateway
		switch status.Code(err) {
		case codes.InvalidArgument, codes.NotFound:
			code, httpStatus = "VALIDATION_FAILED", http.StatusBadRequest
		case codes.Unauthenticated, codes.PermissionDenied:
			code, httpStatus = "INVALID_SIGNATURE", http.StatusUnauthorized
		case codes.AlreadyExists:
			code, httpStatus = "IDEMPOTENCY_CONFLICT", http.StatusConflict
		}
		writeJSON(w, httpStatus, map[string]any{"error": map[string]any{"code": code, "message": status.Convert(err).Message()}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// isPayoutWebhook detects payout (withdrawal) webhook payloads: they carry a
// withdrawal_id (provider payout id) or the platform payout_id and are only
// produced by pivot at Phase C.
func isPayoutWebhook(providerName string, body []byte) bool {
	if !strings.EqualFold(providerName, "pivot") {
		return false
	}
	var payload map[string]json.RawMessage
	if json.Unmarshal(body, &payload) != nil {
		return false
	}
	for _, key := range []string{"withdrawal_id", "payout_id"} {
		var value string
		if json.Unmarshal(payload[key], &value) == nil && strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func extractPaymentWebhook(headers http.Header, body []byte) (providerName, eventID, chargeID, signature string, err error) {
	var payload map[string]json.RawMessage
	if len(bytes.TrimSpace(body)) == 0 || json.Unmarshal(body, &payload) != nil {
		return "", "", "", "", errors.New("webhook body must be a JSON object")
	}
	get := func(name string) string {
		var value string
		_ = json.Unmarshal(payload[name], &value)
		return strings.TrimSpace(value)
	}
	switch {
	// Pivot is the only provider whose payload carries an `event_id` field, so
	// it must be matched first: a well-formed pivot webhook also carries a
	// `transaction_id`, which the broader Midtrans case below would otherwise
	// swallow and misroute to an unconfigured provider.
	case get("event_id") != "":
		providerName = "pivot"
		eventID = get("event_id")
		// Payout webhooks identify themselves by withdrawal_id (no charge),
		// so fall back to those fields to keep the id gate below meaningful.
		chargeID = firstNonEmpty(get("transaction_id"), get("charge_id"), get("withdrawal_id"), get("payout_id"))
		signature = firstNonEmpty(headers.Get("X-Signature"), headers.Get("X-Pivot-Signature"))
	case headers.Get("X-Signature") != "" || get("transaction_id") != "" || get("transaction_status") != "":
		providerName = "midtrans"
		chargeID = get("transaction_id")
		eventID = providerEventID(chargeID, get("transaction_status"), get("fraud_status"))
		signature = firstNonEmpty(headers.Get("X-Signature"), get("signature_key"))
	case headers.Get("X-Callback-Token") != "" || (get("external_id") != "" && get("id") != ""):
		providerName = "xendit"
		chargeID, signature = get("id"), headers.Get("X-Callback-Token")
		eventID = providerEventID(chargeID, get("status"), "")
	default:
		return "", "", "", "", errors.New("unable to detect payment provider")
	}
	if eventID == "" || chargeID == "" {
		return "", "", "", "", errors.New("provider event id and charge id are required")
	}
	return providerName, eventID, chargeID, signature, nil
}

func providerEventID(chargeID, providerStatus, fraudStatus string) string {
	parts := []string{strings.TrimSpace(chargeID), strings.ToLower(strings.TrimSpace(providerStatus))}
	if fraudStatus != "" {
		parts = append(parts, strings.ToLower(strings.TrimSpace(fraudStatus)))
	}
	return strings.Join(parts, ":")
}

func flattenHeaders(headers http.Header) map[string]string {
	out := make(map[string]string, len(headers))
	for name, values := range headers {
		out[strings.ToLower(name)] = strings.Join(values, ",")
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
