package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"
)

type recordingSettler struct{ req *pb.SettleTopUpRequest }

func (s *recordingSettler) SettleTopUp(_ context.Context, req *pb.SettleTopUpRequest) (*pb.SettleTopUpResponse, error) {
	s.req = req
	return &pb.SettleTopUpResponse{}, nil
}

func TestPaymentWebhookForwardsMidtransRawSignatureAndHeaders(t *testing.T) {
	settler := &recordingSettler{}
	h := NewPaymentWebhook(settler)
	body := `{"transaction_id":"charge-1","transaction_status":"settlement","order_id":"donation-1"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/webhooks/payment", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json; charset=utf-8")
	req.Header.Set("X-Signature", "signature-value")
	rec := httptest.NewRecorder()
	h.Handle(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if settler.req == nil || settler.req.ProviderName != "midtrans" || settler.req.ProviderEventId != "charge-1:settlement" || settler.req.ChargeId != "charge-1" {
		t.Fatalf("request=%+v", settler.req)
	}
	if string(settler.req.RawPayload) != body || settler.req.Signature != "signature-value" || settler.req.Headers["x-signature"] != "signature-value" {
		t.Fatalf("raw/signature/headers not preserved: %+v", settler.req)
	}
}

func TestPaymentWebhookRejectsContentTypeAndOversize(t *testing.T) {
	h := NewPaymentWebhook(&recordingSettler{})
	badType := httptest.NewRequest(http.MethodPost, "/v1/webhooks/payment", strings.NewReader(`{}`))
	badType.Header.Set("Content-Type", "text/plain")
	rec := httptest.NewRecorder()
	h.Handle(rec, badType)
	if rec.Code != http.StatusUnsupportedMediaType {
		t.Fatalf("content type status=%d", rec.Code)
	}
	large := httptest.NewRequest(http.MethodPost, "/v1/webhooks/payment", strings.NewReader(strings.Repeat("x", maxPaymentWebhookBody+1)))
	large.Header.Set("Content-Type", "application/json")
	rec = httptest.NewRecorder()
	h.Handle(rec, large)
	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("oversize status=%d", rec.Code)
	}
}

func TestExtractXenditWebhook(t *testing.T) {
	headers := make(http.Header)
	headers.Set("X-Callback-Token", "token")
	provider, eventID, chargeID, signature, err := extractPaymentWebhook(headers, []byte(`{"id":"invoice-1","external_id":"donation-1","status":"PAID"}`))
	if err != nil || provider != "xendit" || eventID != "invoice-1:paid" || chargeID != "invoice-1" || signature != "token" {
		t.Fatalf("got %q %q %q %q err=%v", provider, eventID, chargeID, signature, err)
	}
}

func TestExtractPivotWebhook(t *testing.T) {
	// Real Pivot callback: top-level event name, nested data with the charge id,
	// and the callback API key in X-API-Key. The charge id is folded into the
	// event id so two donations' PAYMENT.PAID callbacks never collide.
	headers := make(http.Header)
	headers.Set("X-API-Key", "pivot-callback-key")
	body := `{"event":"PAYMENT.PAID","data":{"clientReferenceId":"donation-1","status":"ACTIVE","chargeDetails":[{"id":"charge-1"}]}}`
	provider, eventID, chargeID, signature, err := extractPaymentWebhook(headers, []byte(body))
	if err != nil || provider != "pivot" || eventID != "charge-1:payment.paid" || chargeID != "charge-1" || signature != "pivot-callback-key" {
		t.Fatalf("got %q %q %q %q err=%v", provider, eventID, chargeID, signature, err)
	}
}

func TestExtractPivotWebhookStubFlatShape(t *testing.T) {
	// Legacy/stub flat shape keeps working: event_id + flat charge_id. The
	// pivot-specific signature header avoids the midtrans detection path.
	headers := make(http.Header)
	headers.Set("X-Pivot-Signature", "sig")
	provider, eventID, chargeID, sig, err := extractPaymentWebhook(headers, []byte(`{"event_id":"evt-1","charge_id":"charge-2"}`))
	if err != nil || provider != "pivot" || eventID != "charge-2:evt-1" || chargeID != "charge-2" || sig != "sig" {
		t.Fatalf("got %q %q %q %q err=%v", provider, eventID, chargeID, sig, err)
	}
}
