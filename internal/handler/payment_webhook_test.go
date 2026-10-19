package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"
)

type recordingSettler struct {
	req       *pb.SettleTopUpRequest
	wdReq     *pb.SettleWithdrawalRequest
}

func (s *recordingSettler) SettleTopUp(_ context.Context, req *pb.SettleTopUpRequest) (*pb.SettleTopUpResponse, error) {
	s.req = req
	return &pb.SettleTopUpResponse{}, nil
}

func (s *recordingSettler) SettleWithdrawal(_ context.Context, req *pb.SettleWithdrawalRequest) (*pb.SettleWithdrawalResponse, error) {
	s.wdReq = req
	return &pb.SettleWithdrawalResponse{}, nil
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

func TestPaymentWebhookRoutesPayoutWebhookToSettleWithdrawal(t *testing.T) {
	settler := &recordingSettler{}
	h := NewPaymentWebhook(settler)
	body := `{"event_id":"evt-1","withdrawal_id":"pivot_withdrawal_abc","payout_id":"22222222-2222-4222-8222-222222222222","status":"SUCCESS"}`
	req := httptest.NewRequest(http.MethodPost, "/v1/webhooks/payment", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Signature", "signature-value")
	rec := httptest.NewRecorder()
	h.Handle(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if settler.wdReq == nil {
		t.Fatal("payout webhook was not routed to SettleWithdrawal")
	}
	if settler.wdReq.ProviderName != "pivot" || settler.wdReq.ProviderEventId != "evt-1" ||
		settler.wdReq.ProviderPayoutId != "pivot_withdrawal_abc" ||
		settler.wdReq.SarumanPayoutId != "22222222-2222-4222-8222-222222222222" {
		t.Fatalf("request=%+v", settler.wdReq)
	}
	if settler.req != nil {
		t.Fatal("payout webhook must not be routed to SettleTopUp")
	}
}
