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

// Donations handles donation-intent creation.
type Donations struct {
	DB       *sql.DB
	Idem     *idempotency.Store
	Palantir *palantir.Client
}

// NewDonations constructs a donations handler.
func NewDonations(db *sql.DB, idem *idempotency.Store, pal *palantir.Client) *Donations {
	return &Donations{DB: db, Idem: idem, Palantir: pal}
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
}

// DonationIntentResponse is the JSON body returned to the donor.
type DonationIntentResponse struct {
	IntentID             string `json:"intent_id"`
	DonationID           string `json:"donation_id"`
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
	DisplayRateIDRPerSec int64
	DisplayMinSec        int
	DisplayMaxSec        int
	MinDonationIDR       int64
	MaxDonationIDR       int64
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
	existingIntent := true
	err = tx.QueryRowContext(r.Context(), `
		SELECT display_rate_idr_per_sec, display_min_sec, display_max_sec, display_duration_sec
		FROM donations
		WHERE intent_id = $1
	`, intentID).Scan(
		&pricing.DisplayRateIDRPerSec,
		&pricing.DisplayMinSec,
		&pricing.DisplayMaxSec,
		&displayDurationSec,
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
		displayDurationSec, err = calculateDisplayDuration(req.AmountIDR, pricing.DisplayRateIDRPerSec, pricing.DisplayMinSec, pricing.DisplayMaxSec)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INVALID_STREAMER_SETTINGS", "message": err.Error()}})
			return
		}
	} else if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"code": "INTERNAL", "message": err.Error()}})
		return
	}
	// 1. Insert a new donation row with stable intent_id. Reclaimed retries use
	// the immutable snapshot loaded above instead of current streamer settings.
	now := time.Now().UTC()
	expiresAt := now.Add(15 * time.Minute)
	inserted := int64(0)
	if !existingIntent {
		insertResult, err := tx.ExecContext(r.Context(), `
			INSERT INTO donations (id, streamer_id, intent_id, status, settlement_status,
			    amount_idr, currency, donor_display_name, donor_email, message, is_anonymous,
			    display_rate_idr_per_sec, display_min_sec, display_max_sec, display_duration_sec,
			    expires_at, created_at)
			VALUES ($1,$2,$3,'INTENT_CREATED','PENDING',$4,'IDR',$5,$6,$7,$8,$9,$10,$11,$12,$13, NOW())
			ON CONFLICT (intent_id) DO NOTHING
		`, donationID, streamerID, intentID, req.AmountIDR, req.DonorDisplayName, req.DonorEmail, req.Message, req.IsAnonymous,
			pricing.DisplayRateIDRPerSec, pricing.DisplayMinSec, pricing.DisplayMaxSec, displayDurationSec, expiresAt)
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
	// duplicate provider calls if this request retries.
	palResp, err := h.Palantir.CreateTopUp(r.Context(), &pb.CreateTopUpRequest{
		SarumanDonationId: donationID.String(),
		StreamerId:        streamerID.String(),
		AmountIdr:         req.AmountIDR,
		Currency:          "IDR",
		DonorDisplayName:  req.DonorDisplayName,
		DonorEmail:        req.DonorEmail,
		Description:       req.Message,
		IdempotencyKey:    intentID.String(),
	})
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": map[string]any{"code": "PROVIDER_ERROR", "message": err.Error()}})
		return
	}
	respBody := DonationIntentResponse{
		IntentID:             intentID.String(),
		DonationID:           donationID.String(),
		AmountIDR:            req.AmountIDR,
		MDRIDR:               0,
		MDRRateBPS:           0,
		GrossChargedIDR:      req.AmountIDR,
		PlatformFeeIDR:       0,
		NetIDR:               req.AmountIDR,
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
		       ss.min_donation_idr, ss.max_donation_idr
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
	)
	return pricing, err
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
