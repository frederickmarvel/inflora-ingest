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

// PaymentWebhook is the public, stateless provider-webhook edge. Signature
// verification and durable idempotency deliberately live in Palantir.
type PaymentWebhook struct{ Palantir topUpSettler }

func NewPaymentWebhook(p topUpSettler) *PaymentWebhook { return &PaymentWebhook{Palantir: p} }

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
		chargeID = firstNonEmpty(get("transaction_id"), get("charge_id"))
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
