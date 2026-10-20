// Package handler contains the HTTP transport for Ingest.
package handler

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/frederickmarvel/inflora-ingest/internal/idempotency"
	"github.com/frederickmarvel/inflora-ingest/internal/palantir"
	"github.com/frederickmarvel/inflora-shared/events"
	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"
	"github.com/frederickmarvel/inflora-shared/outbox"
	"github.com/google/uuid"
)

// validPaymentMethods mirrors the frozen payment_method enum created in
// inflora-saruman migration 00001_init (CONTRACT_DRIFT D-034). Unknown methods
// are rejected at the edge with 422; an absent method is treated as legacy and
// passes through.
var validPaymentMethods = map[string]bool{
	"QRIS":            true,
	"VIRTUAL_ACCOUNT": true,
	"EWALLET_GOPAY":   true,
	"EWALLET_OVO":     true,
	"EWALLET_DANA":    true,
	"CREDIT_CARD":     true,
	"OTHER":           true,
}

// Donations handles donation-intent creation.
type Donations struct {
	DB       *sql.DB
	Idem     *idempotency.Store
	Palantir *palantir.Client
	// ProviderName keys the active mdr_rates lookup (D-061). It must agree
	// with palantir's PROVIDER_DEFAULT (both default to "pivot").
	ProviderName string
}

// NewDonations constructs a donations handler.
func NewDonations(db *sql.DB, idem *idempotency.Store, pal *palantir.Client, providerName string) *Donations {
	return &Donations{DB: db, Idem: idem, Palantir: pal, ProviderName: providerName}
}

// DonationIntentRequest is the JSON body for POST /v1/donations.
type DonationIntentRequest struct {
	StreamerID       string `json:"streamer_id"`
	AmountIDR        int64  `json:"amount_idr"`
	PaymentMethod    string `json:"payment_method"`
	DonorDisplayName string `json:"donor_display_name"`
	DonorEmail       string `json:"donor_email,omitempty"`
	Message          string `json:"message,omitempty"`
	IsAnonymous      bool   `json:"is_anonymous"`

	// Rich-intent fields (CONTRACT_DRIFT D-031/D-037). Optional: legacy
	// clients are unaffected. widget_id names the tapped widget, tier the
	// chosen tick tier, quantity the gift count; media/call carry the
	// media-share and call attachments.
	LiveSessionID string                   `json:"live_session_id,omitempty"`
	WidgetID      string                   `json:"widget_id,omitempty"`
	Quantity      int                      `json:"quantity,omitempty"`
	Tier          string                   `json:"tier,omitempty"`
	Media         *events.DonationMediaEvent `json:"media,omitempty"`
	Call          *events.DonationCallEvent  `json:"call,omitempty"`
}

// DonationIntentResponse is the JSON body returned to the donor.
type DonationIntentResponse struct {
	IntentID             string `json:"intent_id"`
	DonationID           string `json:"donation_id"`
	StreamerDisplayName  string `json:"streamer_display_name"`
	AmountIDR            int64  `json:"amount_idr"`
	MDRIDR               int64  `json:"mdr_idr"`
	MDRRateBPS           int    `json:"mdr_rate_bps"`
	GrossChargedIDR      int64  `json:"gross_charged_idr"`
	PlatformFeeIDR       int64  `json:"platform_fee_idr"`
	NetIDR               int64  `json:"net_idr"`
	Currency             string `json:"currency"`
	DisplayRateIDRPerSec int64  `json:"display_rate_idr_per_sec"`
	DisplayDurationSec   int    `json:"display_duration_sec"`
	PaymentURL           string `json:"payment_url"`
	ExpiresAt            string `json:"expires_at"`
}

type streamerPricing struct {
	DisplayName         string
	DisplayRateIDRPerSec int64
	DisplayMinSec        int
	DisplayMaxSec        int
	MinDonationIDR       int64
	MaxDonationIDR       int64
	// PlatformFeeBPS is the per-streamer fee (D-060, migration 00017),
	// snapshotted FOR SHARE inside the intent transaction.
	PlatformFeeBPS int
}

// intentFees carries the per-calculation fee snapshot (D-060/D-061). Each
// donation's fees are computed from configuration rows read inside its own
// intent transaction and frozen into the donations row; replay returns the
// stored values, never a recomputation.
type intentFees struct {
	PlatformFeeIDR  int64
	NetIDR          int64
	MDRIDR          int64
	MDRRateBPS      int
	GrossChargedIDR int64
}

// Handle is the http.Handler.
func (h *Donations) Handle(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	idemKey := strings.TrimSpace(r.Header.Get("Idempotency-Key"))
	if idemKey == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"code": "MISSING_IDEMPOTENCY_KEY", "message": "Idempotency-Key header is required"}})
		return
	}
	var req DonationIntentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"code": "VALIDATION_FAILED", "message": err.Error()}})
		return
	}
	if req.StreamerID == "" || req.AmountIDR <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"code": "VALIDATION_FAILED", "message": "positive amount and streamer required"}})
		return
	}
	streamerID, err := uuid.Parse(req.StreamerID)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"code": "VALIDATION_FAILED", "message": "streamer_id must be a uuid"}})
		return
	}
	// D-034: validate payment_method against the frozen enum (see map above).
	if req.PaymentMethod != "" && !validPaymentMethods[req.PaymentMethod] {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": map[string]any{"code": "VALIDATION_FAILED", "message": fmt.Sprintf("payment_method %q is not supported", req.PaymentMethod)}})
		return
	}
	if req.Quantity < 0 {
		writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": map[string]any{"code": "VALIDATION_FAILED", "message": "quantity must not be negative"}})
		return
	}
	var liveSessionUUID *uuid.UUID
	if req.LiveSessionID != "" {
		lsID, err := uuid.Parse(req.LiveSessionID)
		if err != nil {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]any{"error": map[string]any{"code": "VALIDATION_FAILED", "message": "live_session_id must be a uuid"}})
			return
		}
		liveSessionUUID = &lsID
	}
	bodyHash, err := hashBody(&req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
		return
	}
	endpoint := "POST /v1/donations"
	tx, err := h.DB.BeginTx(r.Context(), nil)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
		return
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()
	claimed, priorStatus, priorBody, err := h.Idem.Claim(r.Context(), tx, idempotency.Record{
		Key:         idemKey,
		Endpoint:    endpoint,
		RequestHash: bodyHash,
	})
	if err != nil {
		if errors.Is(err, idempotency.ErrDuplicate) {
			writeJSON(w, http.StatusConflict, map[string]any{"error": map[string]any{"code": "IDEMPOTENCY_CONFLICT", "message": "same key, different body"}})
			return
		}
		if errors.Is(err, idempotency.ErrInProgress) {
			writeJSON(w, http.StatusConflict, map[string]any{"error": map[string]any{"code": "IDEMPOTENCY_IN_PROGRESS", "message": "request with this key is still in progress"}})
			return
		}
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
		return
	}
	if !claimed {
		_ = tx.Commit()
		committed = true
		// Claim never returns an incomplete record as a replay.
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(priorStatus)
		_, _ = w.Write(priorBody)
		return
	}
	intentID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(endpoint+"\x00"+idemKey+"\x00intent"))
	donationID := uuid.NewSHA1(uuid.NameSpaceURL, []byte(endpoint+"\x00"+idemKey+"\x00donation"))
	var pricing streamerPricing
	var displayDurationSec int
	var fees intentFees
	existingIntent := true
	err = tx.QueryRowContext(r.Context(), `
		SELECT display_rate_idr_per_sec, display_min_sec, display_max_sec, display_duration_sec,
		       platform_fee_idr, mdr_idr, mdr_rate_bps, gross_charged_idr, net_idr
		FROM donations
		WHERE intent_id = $1
	`, intentID).Scan(
		&pricing.DisplayRateIDRPerSec,
		&pricing.DisplayMinSec,
		&pricing.DisplayMaxSec,
		&displayDurationSec,
		&fees.PlatformFeeIDR,
		&fees.MDRIDR,
		&fees.MDRRateBPS,
		&fees.GrossChargedIDR,
		&fees.NetIDR,
	)
	if errors.Is(err, sql.ErrNoRows) {
		existingIntent = false
		pricing, err = loadStreamerPricing(r.Context(), tx, streamerID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeJSON(w, http.StatusNotFound, map[string]any{"error": map[string]any{"code": "STREAMER_NOT_FOUND", "message": "active streamer not found"}})
				return
			}
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
			return
		}
		if req.AmountIDR < pricing.MinDonationIDR || req.AmountIDR > pricing.MaxDonationIDR {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"code": "VALIDATION_FAILED", "message": fmt.Sprintf("amount_idr must be between %d and %d for this streamer", pricing.MinDonationIDR, pricing.MaxDonationIDR)}})
			return
		}
		// D-060/D-061: compute the fee snapshot inside the intent tx. Platform
		// fee is deducted from the donation (net = amount - fee); MDR is
		// charged to the donor on top (gross = amount + mdr). The donation row
		// freezes both; capture validates gross == net + fee + mdr.
		fees.PlatformFeeIDR = req.AmountIDR * int64(pricing.PlatformFeeBPS) / 10000
		fees.NetIDR = req.AmountIDR - fees.PlatformFeeIDR
		// PlatformFeeBPS <= 9999 (00017 CHECK) guarantees fee < amount for any
		// amount >= 1, so net stays positive (frozen invariant).
		mdrBPS, mdrFlatIDR, err := loadActiveMDR(r.Context(), tx, h.ProviderName, req.PaymentMethod)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
			return
		}
		fees.MDRRateBPS = mdrBPS
		fees.MDRIDR = req.AmountIDR*int64(mdrBPS)/10000 + mdrFlatIDR
		fees.GrossChargedIDR = req.AmountIDR + fees.MDRIDR
		// D-037: when the tip form carries a live session, it must still be
		// open for this streamer. A closed session maps to 409 STREAM_ENDED so
		// the React form can surface "the stream has ended".
		if liveSessionUUID != nil {
			var one int
			// FOR SHARE serializes this gate against the end-stream UPDATE in
			// tolkien, so a concurrent "go offline" cannot commit between the
			// check and the intent INSERT (adversarial review finding).
			err = tx.QueryRowContext(r.Context(), `
				SELECT 1 FROM live_sessions
				WHERE id = $1 AND streamer_id = $2 AND ended_at IS NULL
				FOR SHARE
			`, *liveSessionUUID, streamerID).Scan(&one)
			if errors.Is(err, sql.ErrNoRows) {
				writeJSON(w, http.StatusConflict, map[string]any{"error": map[string]any{"code": "STREAM_ENDED", "message": "the stream has ended"}})
				return
			}
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
				return
			}
		}
		displayDurationSec, err = calculateDisplayDuration(req.AmountIDR, pricing.DisplayRateIDRPerSec, pricing.DisplayMinSec, pricing.DisplayMaxSec)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INVALID_STREAMER_SETTINGS", "message": err.Error()}})
			return
		}
	} else if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
		return
	}
	// The response carries the creator's display name (CONTRACT_DRIFT D-031).
	// Replays take it from streamers, since the pricing snapshot read above
	// does not include it.
	displayName := ""
	if existingIntent {
		err = tx.QueryRowContext(r.Context(), `
			SELECT display_name FROM streamers WHERE id = $1
		`, streamerID).Scan(&displayName)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
			return
		}
	} else {
		displayName = pricing.DisplayName
	}
	// 1. Insert a new donation row with stable intent_id. Reclaimed retries use
	// the immutable snapshot loaded above instead of current streamer settings.
	now := time.Now().UTC()
	expiresAt := now.Add(15 * time.Minute)
	inserted := int64(0)
	if !existingIntent {
		// gross_charged_idr and net_idr default to the gross amount because the
		// MVP pivot stub applies no MDR and the platform fee is not yet wired
		// up. Keeping them equal to amount_idr preserves the frozen-contract
		// invariant gross == net + platform_fee + mdr that Saruman verifies at
		// capture time.
		var mediaJSON, callJSON, liveSessionParam any
		if req.Media != nil {
			b, err := json.Marshal(req.Media)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
				return
			}
			mediaJSON = b
		}
		if req.Call != nil {
			b, err := json.Marshal(req.Call)
			if err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
				return
			}
			callJSON = b
		}
		if liveSessionUUID != nil {
			liveSessionParam = *liveSessionUUID
		}
		// D-060/D-061: gross_charged_idr/net_idr/mdr_idr/mdr_rate_bps/
		// platform_fee_idr freeze the computed fee snapshot (above) so capture
		// can validate the frozen-contract invariant gross == net + fee + mdr.
		insertResult, err := tx.ExecContext(r.Context(), `
			INSERT INTO donations (id, streamer_id, intent_id, status, settlement_status,
			    amount_idr, gross_charged_idr, net_idr, currency, donor_display_name, donor_email, message, is_anonymous,
			    display_rate_idr_per_sec, display_min_sec, display_max_sec, display_duration_sec,
			    live_session_id, widget_id, quantity, tier, media, call,
			    mdr_idr, mdr_rate_bps, platform_fee_idr,
			    expires_at, created_at)
			VALUES ($1,$2,$3,'INTENT_CREATED','PENDING',$4,$20,$21,'IDR',$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$22,$23,$24,$19,NOW())
			ON CONFLICT (intent_id) DO NOTHING
		`, donationID, streamerID, intentID, req.AmountIDR, req.DonorDisplayName, req.DonorEmail, req.Message, req.IsAnonymous,
			pricing.DisplayRateIDRPerSec, pricing.DisplayMinSec, pricing.DisplayMaxSec, displayDurationSec,
			liveSessionParam, optionalString(req.WidgetID), optionalInt(req.Quantity), optionalString(req.Tier), mediaJSON, callJSON,
			expiresAt, fees.GrossChargedIDR, fees.NetIDR, fees.MDRIDR, fees.MDRRateBPS, fees.PlatformFeeIDR)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
			return
		}
		inserted, err = insertResult.RowsAffected()
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
			return
		}
	}
	// 2. Outbox event for donation.intent.created.v1.
	streamerIDStr := streamerID.String()
	payload := events.DonationIntentCreatedEvent{
		IntentID:             intentID.String(),
		DonationID:           donationID.String(),
		StreamerID:           streamerIDStr,
		AmountIDR:            req.AmountIDR,
		Currency:             "IDR",
		PaymentMethod:        req.PaymentMethod,
		DonorDisplayName:     req.DonorDisplayName,
		DonorEmail:           req.DonorEmail,
		Message:              req.Message,
		IsAnonymous:          req.IsAnonymous,
		DisplayRateIDRPerSec: pricing.DisplayRateIDRPerSec,
		DisplayMinSec:        pricing.DisplayMinSec,
		DisplayMaxSec:        pricing.DisplayMaxSec,
		DisplayDurationSec:   displayDurationSec,
		LiveSessionID:        liveSessionIDStr(liveSessionUUID),
		WidgetID:             req.WidgetID,
		Quantity:             optionalIntPtr(req.Quantity),
		Tier:                 req.Tier,
		Media:                req.Media,
		Call:                 req.Call,
		ExpiresAt:            expiresAt,
		CreatedAt:            now,
	}
	envelope, err := events.NewEnvelope(events.DonationIntentCreated, "ingest", &streamerIDStr, payload)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
		return
	}
	if inserted == 1 {
		if _, err := outbox.Enqueue(r.Context(), tx, "ingest", "donation", donationID, &streamerID, envelope); err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
			return
		}
	}
	if err := tx.Commit(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
		return
	}
	committed = true
	// 3. Call Palantir CreateTopUp. Palantir's idempotency protects against
	// duplicate provider calls if this request retries. MDR/gross are
	// propagated (D-061) so the gateway charges the donor the gross amount;
	// on replay the stored snapshot reproduces the original request hash.
	palResp, err := h.Palantir.CreateTopUp(r.Context(), &pb.CreateTopUpRequest{
		SarumanDonationId: donationID.String(),
		StreamerId:        streamerID.String(),
		AmountIdr:         req.AmountIDR,
		MdrIdr:            fees.MDRIDR,
		GrossChargedIdr:   fees.GrossChargedIDR,
		Currency:          "IDR",
		DonorDisplayName:  req.DonorDisplayName,
		IdempotencyKey:    intentID.String(),
	})
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{"code": "PROVIDER_ERROR", "message": err.Error()}})
		return
	}
	respBody := DonationIntentResponse{
		IntentID:             intentID.String(),
		DonationID:           donationID.String(),
		StreamerDisplayName:  displayName,
		AmountIDR:            req.AmountIDR,
		MDRIDR:               fees.MDRIDR,
		MDRRateBPS:           fees.MDRRateBPS,
		GrossChargedIDR:      fees.GrossChargedIDR,
		PlatformFeeIDR:       fees.PlatformFeeIDR,
		NetIDR:               fees.NetIDR,
		Currency:             "IDR",
		DisplayRateIDRPerSec: pricing.DisplayRateIDRPerSec,
		DisplayDurationSec:   displayDurationSec,
		PaymentURL:           palResp.PaymentUrl,
		ExpiresAt:            time.Unix(palResp.ExpiresAtUnix, 0).UTC().Format(time.RFC3339),
	}
	respJSON, _ := json.Marshal(respBody)
	// Cache the successful response in idempotency.
	cacheTx, err := h.DB.BeginTx(r.Context(), nil)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
		return
	}
	if err := h.Idem.Finalize(r.Context(), cacheTx, idemKey, http.StatusCreated, respJSON); err != nil {
		_ = cacheTx.Rollback()
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
		return
	}
	if err := cacheTx.Commit(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
		return
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(respJSON)
}

func loadStreamerPricing(ctx context.Context, tx *sql.Tx, streamerID uuid.UUID) (streamerPricing, error) {
	var pricing streamerPricing
	err := tx.QueryRowContext(ctx, `
		SELECT ss.display_rate_idr_per_sec, ss.display_min_sec, ss.display_max_sec,
		       ss.min_donation_idr, ss.max_donation_idr, s.display_name,
		       ss.platform_fee_bps
		FROM streamer_settings ss
		JOIN streamers s ON s.id = ss.streamer_id
		WHERE ss.streamer_id = $1 AND s.is_active = TRUE
		FOR SHARE OF ss, s
	`, streamerID).Scan(
		&pricing.DisplayRateIDRPerSec,
		&pricing.DisplayMinSec,
		&pricing.DisplayMaxSec,
		&pricing.MinDonationIDR,
		&pricing.MaxDonationIDR,
		&pricing.DisplayName,
		&pricing.PlatformFeeBPS,
	)
	return pricing, err
}

// loadActiveMDR returns the active mdr_rates row for (provider, method)
// (D-061). An empty method (legacy request) or a missing row yields zeros —
// no rate charged — and only a real query error propagates.
func loadActiveMDR(ctx context.Context, tx *sql.Tx, providerName, method string) (int, int64, error) {
	if providerName == "" || method == "" {
		return 0, 0, nil
	}
	var bps int
	var flat int64
	err := tx.QueryRowContext(ctx, `
		SELECT mdr_bps, flat_idr
		FROM mdr_rates
		WHERE provider_name = $1 AND payment_method = $2::payment_method
		  AND is_active
		  AND effective_from <= NOW()
		  AND (effective_to IS NULL OR effective_to > NOW())
		ORDER BY effective_from DESC
		LIMIT 1
	`, providerName, method).Scan(&bps, &flat)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, 0, nil
	}
	if err != nil {
		return 0, 0, err
	}
	return bps, flat, nil
}

func optionalString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

func optionalInt(n int) any {
	if n == 0 {
		return nil
	}
	return n
}

func optionalIntPtr(n int) *int {
	if n == 0 {
		return nil
	}
	return &n
}

func liveSessionIDStr(id *uuid.UUID) *string {
	if id == nil {
		return nil
	}
	s := id.String()
	return &s
}

func calculateDisplayDuration(amountIDR, rateIDRPerSec int64, minSec, maxSec int) (int, error) {
	if amountIDR <= 0 || rateIDRPerSec <= 0 || minSec <= 0 || maxSec < minSec {
		return 0, errors.New("invalid amount or streamer display pricing")
	}
	duration := amountIDR / rateIDRPerSec
	if duration < int64(minSec) {
		return minSec, nil
	}
	if duration > int64(maxSec) {
		return maxSec, nil
	}
	return int(duration), nil
}

func hashBody(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("hash: %w", err)
	}
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:]), nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
