package handler

// Fee-matrix tests for donation intent creation (D-060 platform fee,
// D-061 MDR). These run against a per-test schema built from the pinned
// saruman migrations, mirroring the harness idiom of the idempotency store
// tests (inflora-ingest/internal/idempotency/store_test.go): skip unless
// TEST_DATABASE_URL is set, fresh schema per test via search_path, curated
// migration subset, schema dropped on cleanup.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/frederickmarvel/inflora-ingest/internal/idempotency"
	"github.com/frederickmarvel/inflora-ingest/internal/palantir"
	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"
	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"google.golang.org/grpc"
)

const (
	testProvider = "pivot"
	testMethod   = "QRIS"
	testAmount   = int64(10_000)
)

// recordingPalantir is an in-process gRPC TopUpService that records every
// CreateTopUp request, so tests can assert exactly what the handler
// propagates to the payment gateway (D-060/D-061).
type recordingPalantir struct {
	pb.UnimplementedTopUpServiceServer
	mu     sync.Mutex
	topUps []*pb.CreateTopUpRequest
}

func (s *recordingPalantir) CreateTopUp(_ context.Context, req *pb.CreateTopUpRequest) (*pb.CreateTopUpResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.topUps = append(s.topUps, req)
	return &pb.CreateTopUpResponse{
		ChargeId:      "charge-test",
		PaymentUrl:    "https://pay.example.test/topup",
		ExpiresAtUnix: time.Now().Add(time.Hour).Unix(),
	}, nil
}

func (s *recordingPalantir) requests() []*pb.CreateTopUpRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*pb.CreateTopUpRequest, len(s.topUps))
	copy(out, s.topUps)
	return out
}

// startRecordingPalantir dials a real in-process gRPC server (the same
// technique as inflora-ingest/internal/palantir/client_test.go), so the
// handler exercises its production palantir.Client while the test observes
// the RPC calls.
func startRecordingPalantir(t *testing.T) (*palantir.Client, *recordingPalantir) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	rec := &recordingPalantir{}
	server := grpc.NewServer()
	pb.RegisterTopUpServiceServer(server, rec)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	client, err := palantir.New(context.Background(), lis.Addr().String(), "test-engine-key")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client, rec
}

// openDonationsTestDB builds a fresh schema from the pinned migrations the
// intent flow needs and returns a single-connection handle pinned to it.
// streamer_settings.platform_fee_bps is added by 00017, whose remaining
// statements seed the 00015 RBAC tables this harness does not build; only
// the streamer_settings change is replicated here, verbatim from
// 00017_backoffice_fees_nightly.up.sql.
func openDonationsTestDB(t *testing.T) *sql.DB {
	t.Helper()
	rootDSN := os.Getenv("TEST_DATABASE_URL")
	if rootDSN == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	name := "test_fees_" + sanitize(t.Name())
	q := rootDSN
	sep := "?"
	if strings.Contains(q, "?") {
		sep = "&"
	}
	q = q + sep + "options=-c search_path%3D" + name
	db, err := sql.Open("pgx", q)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if _, err := db.Exec(`DROP SCHEMA IF EXISTS ` + name + ` CASCADE; CREATE SCHEMA ` + name); err != nil {
		t.Fatalf("create schema: %v", err)
	}
	dir := os.Getenv("SARUMAN_SCHEMA_DIR")
	if dir == "" {
		_, file, _, _ := runtime.Caller(0)
		dir = filepath.Join(filepath.Dir(file), "..", "..", "..", "inflora-saruman", "migrations")
	}
	for _, n := range []string{
		"00001_init.up.sql",
		"00003_reliability.up.sql",
		"00004_palantir_topup_idempotency.up.sql",
		"00005_runtime_reliability.up.sql",
		"00006_donation_display_pricing_snapshot.up.sql",
		"00010_donation_rich_intent.up.sql",
	} {
		path := filepath.Join(dir, n)
		schema, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", n, err)
		}
		if _, err := db.Exec(string(schema)); err != nil {
			t.Fatalf("apply %s: %v", n, err)
		}
	}
	const platformFeeDDL = `ALTER TABLE streamer_settings
  ADD COLUMN platform_fee_bps INTEGER NOT NULL DEFAULT 0
      CONSTRAINT chk_ss_platform_fee_bps CHECK (platform_fee_bps BETWEEN 0 AND 9999);`
	if _, err := db.Exec(platformFeeDDL); err != nil {
		t.Fatalf("apply 00017 streamer_settings change: %v", err)
	}
	t.Cleanup(func() {
		// Use a separate connection for cleanup so it can't be blocked by a
		// leaked tx on the test connection.
		c, err := sql.Open("pgx", rootDSN)
		if err != nil {
			return
		}
		defer c.Close()
		_, _ = c.Exec(`DROP SCHEMA IF EXISTS ` + name + ` CASCADE`)
		_ = db.Close()
	})
	return db
}

func sanitize(s string) string {
	out := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		if (s[i] >= 'a' && s[i] <= 'z') || (s[i] >= 'A' && s[i] <= 'Z') || (s[i] >= '0' && s[i] <= '9') {
			out = append(out, s[i])
		} else {
			out = append(out, '_')
		}
	}
	return string(out)
}

// seedStreamer inserts an active streamer plus settings. omitFeeBPS leaves
// platform_fee_bps unset so the schema DEFAULT (0, from 00017) applies —
// the baseline case relies on exactly that default.
func seedStreamer(t *testing.T, db *sql.DB, bps int, omitFeeBPS bool) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	email := sanitize(t.Name()) + "@example.test"
	err := db.QueryRow(`
		INSERT INTO streamers (email, display_name, password_hash)
		VALUES ($1, 'test-streamer', 'x')
		RETURNING id`, email).Scan(&id)
	if err != nil {
		t.Fatalf("insert streamer: %v", err)
	}
	if omitFeeBPS {
		_, err = db.Exec(`
			INSERT INTO streamer_settings
			  (streamer_id, display_rate_idr_per_sec, display_min_sec, display_max_sec,
			   min_donation_idr, max_donation_idr)
			VALUES ($1, 1000, 5, 60, 1000, 10000000)`, id)
	} else {
		_, err = db.Exec(`
			INSERT INTO streamer_settings
			  (streamer_id, display_rate_idr_per_sec, display_min_sec, display_max_sec,
			   min_donation_idr, max_donation_idr, platform_fee_bps)
			VALUES ($1, 1000, 5, 60, 1000, 10000000, $2)`, id, bps)
	}
	if err != nil {
		t.Fatalf("insert streamer_settings: %v", err)
	}
	return id
}

// seedMDR inserts one mdr_rates row. fromAgoMin/toAgoMin position the
// effective window relative to NOW(); toAgoMin <= 0 leaves effective_to
// NULL (still current). provider/method are interpolated literals so the
// enum column is fed by PostgreSQL's own coercion, not a driver cast.
func seedMDR(t *testing.T, db *sql.DB, provider, method string, bps int, flat int64, active bool, fromAgoMin, toAgoMin int) {
	t.Helper()
	var q string
	if toAgoMin > 0 {
		q = fmt.Sprintf(`
			INSERT INTO mdr_rates (provider_name, payment_method, mdr_bps, flat_idr, effective_from, effective_to, is_active)
			VALUES ('%s', '%s', $1, $2, NOW() - ($3 * INTERVAL '1 minute'), NOW() - ($4 * INTERVAL '1 minute'), $5)`,
			provider, method)
		if _, err := db.Exec(q, bps, flat, fromAgoMin, toAgoMin, active); err != nil {
			t.Fatalf("insert mdr_rates: %v", err)
		}
		return
	}
	q = fmt.Sprintf(`
		INSERT INTO mdr_rates (provider_name, payment_method, mdr_bps, flat_idr, effective_from, is_active)
		VALUES ('%s', '%s', $1, $2, NOW() - ($3 * INTERVAL '1 minute'), $4)`,
		provider, method)
	if _, err := db.Exec(q, bps, flat, fromAgoMin, active); err != nil {
		t.Fatalf("insert mdr_rates: %v", err)
	}
}

// postIntent drives a full POST /v1/donations through the handler and
// returns the recorder plus the decoded donation-intent response.
func postIntent(t *testing.T, h *Donations, body DonationIntentRequest, idemKey string) (*httptest.ResponseRecorder, DonationIntentResponse) {
	t.Helper()
	b, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/donations", strings.NewReader(string(b)))
	req.Header.Set("Idempotency-Key", idemKey)
	rec := httptest.NewRecorder()
	h.Handle(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp DonationIntentResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v (body=%s)", err, rec.Body.String())
	}
	return rec, resp
}

// wantFees is the expected fee snapshot for one intent.
type wantFees struct {
	amount, fee, net, mdr int64
	mdrBPS                int
	gross                 int64
}

func assertIntentFees(t *testing.T, resp DonationIntentResponse, want wantFees) {
	t.Helper()
	if resp.AmountIDR != want.amount ||
		resp.PlatformFeeIDR != want.fee ||
		resp.NetIDR != want.net ||
		resp.MDRIDR != want.mdr ||
		resp.MDRRateBPS != want.mdrBPS ||
		resp.GrossChargedIDR != want.gross {
		t.Fatalf("response fees = {amount=%d fee=%d net=%d mdr=%d mdr_bps=%d gross=%d}, want {amount=%d fee=%d net=%d mdr=%d mdr_bps=%d gross=%d}",
			resp.AmountIDR, resp.PlatformFeeIDR, resp.NetIDR, resp.MDRIDR, resp.MDRRateBPS, resp.GrossChargedIDR,
			want.amount, want.fee, want.net, want.mdr, want.mdrBPS, want.gross)
	}
}

// frozenFees reads the fee columns frozen into the donations row at INSERT.
type frozenFees struct {
	amount, fee, net, mdr int64
	mdrBPS                int
	gross                 int64
}

func fetchFrozenFees(t *testing.T, db *sql.DB, intentID uuid.UUID) frozenFees {
	t.Helper()
	var f frozenFees
	err := db.QueryRow(`
		SELECT amount_idr, platform_fee_idr, net_idr, mdr_idr, mdr_rate_bps, gross_charged_idr
		FROM donations WHERE intent_id = $1`, intentID).Scan(
		&f.amount, &f.fee, &f.net, &f.mdr, &f.mdrBPS, &f.gross)
	if err != nil {
		t.Fatalf("read frozen fees: %v", err)
	}
	return f
}

func assertFrozenFees(t *testing.T, db *sql.DB, intentID uuid.UUID, want wantFees) {
	t.Helper()
	f := fetchFrozenFees(t, db, intentID)
	if f.amount != want.amount || f.fee != want.fee || f.net != want.net ||
		f.mdr != want.mdr || f.mdrBPS != want.mdrBPS || f.gross != want.gross {
		t.Fatalf("frozen row = {amount=%d fee=%d net=%d mdr=%d mdr_bps=%d gross=%d}, want {amount=%d fee=%d net=%d mdr=%d mdr_bps=%d gross=%d}",
			f.amount, f.fee, f.net, f.mdr, f.mdrBPS, f.gross,
			want.amount, want.fee, want.net, want.mdr, want.mdrBPS, want.gross)
	}
}

// assertTopUp checks the nth recorded CreateTopUp call propagated exactly the
// given amounts (production always sends MdrIdr and GrossChargedIdr, even
// when they are zero — never omitted).
func assertTopUp(t *testing.T, rec *recordingPalantir, idx int, wantAmount, wantMDR, wantGross int64) {
	t.Helper()
	reqs := rec.requests()
	if len(reqs) <= idx {
		t.Fatalf("want at least %d CreateTopUp call(s), got %d", idx+1, len(reqs))
	}
	req := reqs[idx]
	if req.AmountIdr != wantAmount || req.MdrIdr != wantMDR || req.GrossChargedIdr != wantGross {
		t.Fatalf("CreateTopUp[%d] = {amount=%d mdr=%d gross=%d}, want {amount=%d mdr=%d gross=%d}",
			idx, req.AmountIdr, req.MdrIdr, req.GrossChargedIdr, wantAmount, wantMDR, wantGross)
	}
}

func newTestHandler(t *testing.T, idem *idempotency.Store, pal *palantir.Client) *Donations {
	t.Helper()
	return NewDonations(idem.DB, idem, pal, testProvider)
}

func uuidOf(t *testing.T, s string) uuid.UUID {
	t.Helper()
	id, err := uuid.Parse(s)
	if err != nil {
		t.Fatalf("parse uuid %q: %v", s, err)
	}
	return id
}

func intentRequest(streamerID uuid.UUID, amount int64, method string) DonationIntentRequest {
	return DonationIntentRequest{
		StreamerID:       streamerID.String(),
		AmountIDR:        amount,
		PaymentMethod:    method,
		DonorDisplayName: "fee-matrix-donor",
		IsAnonymous:      false,
	}
}

// 1. Baseline: platform_fee_bps DEFAULT 0 (unset at seed) and no mdr_rates
// rows — fee 0, net == amount, mdr 0, gross == amount. This is the exact
// pre-D-060/D-061 behavior: donor pays the amount, creator receives it all.
func TestDonationIntentFeeBaselineNoFeeNoMDR(t *testing.T) {
	db := openDonationsTestDB(t)
	streamerID := seedStreamer(t, db, 0, true) // omit platform_fee_bps -> DEFAULT 0
	client, rec := startRecordingPalantir(t)
	h := newTestHandler(t, idempotency.NewStore(db), client)

	respRec, resp := postIntent(t, h, intentRequest(streamerID, testAmount, testMethod), "fee-test-baseline-"+sanitize(t.Name()))
	if respRec.Code != http.StatusCreated {
		t.Fatalf("status=%d", respRec.Code)
	}
	if resp.Currency != "IDR" {
		t.Fatalf("currency = %q", resp.Currency)
	}
	if resp.DisplayRateIDRPerSec != 1000 || resp.DisplayDurationSec != 10 {
		t.Fatalf("display snapshot = rate %d dur %d", resp.DisplayRateIDRPerSec, resp.DisplayDurationSec)
	}
	if resp.PaymentURL == "" {
		t.Fatal("payment_url must come from palantir")
	}
	want := wantFees{amount: testAmount, fee: 0, net: testAmount, mdr: 0, mdrBPS: 0, gross: testAmount}
	assertIntentFees(t, resp, want)
	assertFrozenFees(t, db, uuidOf(t, resp.IntentID), want)
	assertTopUp(t, rec, 0, testAmount, 0, testAmount)
}

// 2. Platform fee only: bps=500 on amount 10000 -> fee 500, net 9500, no
// MDR rows -> CreateTopUp carries MdrIdr 0 and GrossChargedIdr == amount
// (production sends both fields, not "omitted when zero").
func TestDonationIntentPlatformFeeOnly(t *testing.T) {
	db := openDonationsTestDB(t)
	streamerID := seedStreamer(t, db, 500, false)
	client, rec := startRecordingPalantir(t)
	h := newTestHandler(t, idempotency.NewStore(db), client)

	_, resp := postIntent(t, h, intentRequest(streamerID, testAmount, testMethod), "fee-test-platform-"+sanitize(t.Name()))
	want := wantFees{amount: testAmount, fee: 500, net: 9_500, mdr: 0, mdrBPS: 0, gross: testAmount}
	assertIntentFees(t, resp, want)
	assertFrozenFees(t, db, uuidOf(t, resp.IntentID), want)
	assertTopUp(t, rec, 0, testAmount, 0, testAmount)
}

// 3. Maximum platform fee (bps=9999): fee = 9999, net = 1 — the frozen
// invariant net > 0 holds at the cap. The 10000-bps seed is impossible by
// the 00017 DB CHECK (chk_ss_platform_fee_bps BETWEEN 0 AND 9999); the cap
// is DB-side enforcement and is not re-tested here.
func TestDonationIntentMaxPlatformFeeKeepsNetPositive(t *testing.T) {
	db := openDonationsTestDB(t)
	streamerID := seedStreamer(t, db, 9999, false)
	client, rec := startRecordingPalantir(t)
	h := newTestHandler(t, idempotency.NewStore(db), client)

	_, resp := postIntent(t, h, intentRequest(streamerID, testAmount, testMethod), "fee-test-maxfee-"+sanitize(t.Name()))
	want := wantFees{amount: testAmount, fee: 9_999, net: 1, mdr: 0, mdrBPS: 0, gross: testAmount}
	assertIntentFees(t, resp, want)
	assertFrozenFees(t, db, uuidOf(t, resp.IntentID), want)
	assertTopUp(t, rec, 0, testAmount, 0, testAmount)
}

// 4. MDR applied: ACTIVE (pivot, QRIS) row at 70 bps + 1000 flat on amount
// 10000 -> mdr = 10000*70/10000 + 1000 = 1070, gross = 11070. The donation
// row freezes mdr_rate_bps=70 and CreateTopUp carries MdrIdr 1070 +
// GrossChargedIdr 11070.
func TestDonationIntentMDRAppliedAndFrozen(t *testing.T) {
	db := openDonationsTestDB(t)
	streamerID := seedStreamer(t, db, 0, false)
	seedMDR(t, db, testProvider, testMethod, 70, 1000, true, 60, 0)
	client, rec := startRecordingPalantir(t)
	h := newTestHandler(t, idempotency.NewStore(db), client)

	_, resp := postIntent(t, h, intentRequest(streamerID, testAmount, testMethod), "fee-test-mdr-"+sanitize(t.Name()))
	want := wantFees{amount: testAmount, fee: 0, net: testAmount, mdr: 1_070, mdrBPS: 70, gross: 11_070}
	assertIntentFees(t, resp, want)
	assertFrozenFees(t, db, uuidOf(t, resp.IntentID), want)
	assertTopUp(t, rec, 0, testAmount, 1_070, 11_070)
}

// 5. Effective windowing: the ACTIVE row with the latest effective_from
// wins; an is_active=false row and an expired-window row are never picked
// even when their effective_from is newer. Seeded (pivot, QRIS): 30 bps
// (3 days), 70 bps (2 days, active = winner), 999 bps (1 day, inactive),
// 800 bps (25h, active but effective_to 1h ago = expired window).
func TestDonationIntentLatestActiveMDRWins(t *testing.T) {
	db := openDonationsTestDB(t)
	streamerID := seedStreamer(t, db, 0, false)
	seedMDR(t, db, testProvider, testMethod, 30, 0, true, 3*1440, 0)     // oldest ACTIVE
	seedMDR(t, db, testProvider, testMethod, 70, 0, true, 2*1440, 0)     // winner
	seedMDR(t, db, testProvider, testMethod, 999, 0, false, 1*1440, 0)   // retired, newest from
	seedMDR(t, db, testProvider, testMethod, 800, 0, true, 25*60, 60)    // active but window ended
	client, rec := startRecordingPalantir(t)
	h := newTestHandler(t, idempotency.NewStore(db), client)

	_, resp := postIntent(t, h, intentRequest(streamerID, testAmount, testMethod), "fee-test-window-"+sanitize(t.Name()))
	// mdr = 10000*70/10000 + 0 = 70; gross = 10070.
	want := wantFees{amount: testAmount, fee: 0, net: testAmount, mdr: 70, mdrBPS: 70, gross: 10_070}
	assertIntentFees(t, resp, want)
	assertFrozenFees(t, db, uuidOf(t, resp.IntentID), want)
	assertTopUp(t, rec, 0, testAmount, 70, 10_070)
}

// 6. Methodless intents: an empty payment_method short-circuits the MDR
// lookup (a matching ACTIVE row must still yield mdr 0); a valid method
// with no mdr_rates row also yields mdr 0 and gross == amount.
func TestDonationIntentMethodlessNoMDR(t *testing.T) {
	db := openDonationsTestDB(t)
	streamerID := seedStreamer(t, db, 0, false)
	seedMDR(t, db, testProvider, testMethod, 70, 1000, true, 60, 0)
	client, rec := startRecordingPalantir(t)
	h := newTestHandler(t, idempotency.NewStore(db), client)

	// (a) empty method: no MDR regardless of the seeded QRIS row.
	_, resp := postIntent(t, h, intentRequest(streamerID, testAmount, ""), "fee-test-methodless-empty-"+sanitize(t.Name()))
	want := wantFees{amount: testAmount, fee: 0, net: testAmount, mdr: 0, mdrBPS: 0, gross: testAmount}
	assertIntentFees(t, resp, want)
	assertTopUp(t, rec, 0, testAmount, 0, testAmount)

	// (b) valid method with no configured row.
	_, resp = postIntent(t, h, intentRequest(streamerID, testAmount, "VIRTUAL_ACCOUNT"), "fee-test-methodless-va-"+sanitize(t.Name()))
	assertIntentFees(t, resp, want)
	assertTopUp(t, rec, 1, testAmount, 0, testAmount)
}

// 7a. Replay: the same Idempotency-Key submitted twice returns the cached
// stored response byte-for-byte. Mutating fee config between the calls must
// not change the second response, and no second CreateTopUp call or
// donations row happens.
func TestDonationIntentReplayReturnsCachedFrozenFees(t *testing.T) {
	db := openDonationsTestDB(t)
	streamerID := seedStreamer(t, db, 500, false)
	seedMDR(t, db, testProvider, testMethod, 70, 1000, true, 60, 0)
	client, rec := startRecordingPalantir(t)
	h := newTestHandler(t, idempotency.NewStore(db), client)
	key := "fee-test-replay-" + sanitize(t.Name())

	_, resp1 := postIntent(t, h, intentRequest(streamerID, testAmount, testMethod), key)
	wantStored := wantFees{amount: testAmount, fee: 500, net: 9_500, mdr: 1_070, mdrBPS: 70, gross: 11_070}
	assertIntentFees(t, resp1, wantStored)

	// Change the live configuration: a recomputation would now yield
	// different fees. The replay must still return what was frozen.
	if _, err := db.Exec(`UPDATE streamer_settings SET platform_fee_bps = 900 WHERE streamer_id = $1`, streamerID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM mdr_rates`); err != nil {
		t.Fatal(err)
	}

	_, resp2 := postIntent(t, h, intentRequest(streamerID, testAmount, testMethod), key)
	assertIntentFees(t, resp2, wantStored)
	// The cached response_body column is JSONB: Postgres stores it as a
	// normalized object, so replayed bytes are the jsonb text form (sorted
	// keys, spaced colons) rather than the original marshal bytes. Assert
	// semantic identity of the decoded responses instead of raw bytes.
	if resp1 != resp2 {
		t.Fatalf("replay response differs:\nfirst:  %+v\nsecond: %+v", resp1, resp2)
	}
	if got := len(rec.requests()); got != 1 {
		t.Fatalf("CreateTopUp calls = %d, want 1 (no provider call on replay)", got)
	}
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM donations WHERE intent_id = $1`, uuidOf(t, resp1.IntentID)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("donations rows for intent = %d, want 1", n)
	}
}

// 7b. Stale in-flight reclaim: when the idempotency row is reclaimed
// (claim data lost/expired), the handler re-reads the fees from the stored
// donations row instead of recomputing from (possibly changed) config.
// CreateTopUp is retried — production behavior on reclaim — but must carry
// the STORED MdrIdr/GrossChargedIdr, and no second intent row or outbox
// event is produced.
func TestDonationIntentReclaimReadsStoredFees(t *testing.T) {
	db := openDonationsTestDB(t)
	streamerID := seedStreamer(t, db, 500, false)
	seedMDR(t, db, testProvider, testMethod, 70, 1000, true, 60, 0)
	client, rec := startRecordingPalantir(t)
	idem := idempotency.NewStore(db)
	idem.StaleAfter = time.Minute
	h := newTestHandler(t, idem, client)
	key := "fee-test-reclaim-" + sanitize(t.Name())

	_, resp1 := postIntent(t, h, intentRequest(streamerID, testAmount, testMethod), key)
	wantStored := wantFees{amount: testAmount, fee: 500, net: 9_500, mdr: 1_070, mdrBPS: 70, gross: 11_070}
	assertIntentFees(t, resp1, wantStored)

	// Simulate a crashed first attempt: clear the completed cache and age
	// the in-progress claim beyond the stale window.
	if _, err := db.Exec(`
		UPDATE idempotency
		SET state = 'IN_PROGRESS', response_status = NULL, response_body = NULL,
		    processed_at = NULL, updated_at = NOW() - INTERVAL '10 minutes'
		WHERE key = $1`, key); err != nil {
		t.Fatal(err)
	}
	// A recomputation would now yield fee 900 / mdr 0; the reclaim must
	// return the stored snapshot instead.
	if _, err := db.Exec(`UPDATE streamer_settings SET platform_fee_bps = 900 WHERE streamer_id = $1`, streamerID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`DELETE FROM mdr_rates`); err != nil {
		t.Fatal(err)
	}

	_, resp2 := postIntent(t, h, intentRequest(streamerID, testAmount, testMethod), key)
	assertIntentFees(t, resp2, wantStored)
	if got := len(rec.requests()); got != 2 {
		t.Fatalf("CreateTopUp calls = %d, want 2 (reclaim retries the provider call)", got)
	}
	assertTopUp(t, rec, 1, testAmount, 1_070, 11_070)
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM donations WHERE intent_id = $1`, uuidOf(t, resp1.IntentID)).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("donations rows for intent = %d, want 1", n)
	}
	if err := db.QueryRow(`SELECT count(*) FROM outbox_events`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("outbox events = %d, want 1 (no second enqueue on reclaim)", n)
	}
}

// 8. Combination: per-streamer fee 500 bps and MDR (70 bps + 1000 flat)
// applied together on one intent. amount 10000 -> fee 500, net 9500,
// mdr 1070, gross 11070. The frozen-contract invariants hold:
// amount == net + fee and gross == net + fee + mdr.
func TestDonationIntentCombinedPlatformFeeAndMDR(t *testing.T) {
	db := openDonationsTestDB(t)
	streamerID := seedStreamer(t, db, 500, false)
	seedMDR(t, db, testProvider, testMethod, 70, 1000, true, 60, 0)
	client, rec := startRecordingPalantir(t)
	h := newTestHandler(t, idempotency.NewStore(db), client)

	_, resp := postIntent(t, h, intentRequest(streamerID, testAmount, testMethod), "fee-test-combined-"+sanitize(t.Name()))
	want := wantFees{amount: testAmount, fee: 500, net: 9_500, mdr: 1_070, mdrBPS: 70, gross: 11_070}
	assertIntentFees(t, resp, want)
	assertFrozenFees(t, db, uuidOf(t, resp.IntentID), want)
	assertTopUp(t, rec, 0, testAmount, 1_070, 11_070)

	if want.amount != want.net+want.fee {
		t.Fatalf("capture invariant broken: amount %d != net %d + fee %d", want.amount, want.net, want.fee)
	}
	if want.gross != want.net+want.fee+want.mdr {
		t.Fatalf("capture invariant broken: gross %d != net %d + fee %d + mdr %d", want.gross, want.net, want.fee, want.mdr)
	}
}