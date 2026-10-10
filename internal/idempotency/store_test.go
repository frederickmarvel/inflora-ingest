package idempotency_test

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/frederickmarvel/inflora-ingest/internal/idempotency"
	_ "github.com/jackc/pgx/v5/stdlib"
)

// TestClaimThenSameBodyReturnsCached verifies the basic claim → finalize →
// replay path.
func TestClaimThenSameBodyReturnsCached(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	db := openDB(t)
	store := idempotency.NewStore(db)
	key := "test-key-" + sanitize(t.Name())
	must := func(err error, msg string) {
		t.Helper()
		if err != nil {
			t.Fatal(msg, ":", err)
		}
	}

	tx, err := db.Begin()
	must(err, "begin")
	h := "hash-a-" + strings.Repeat("0", 64-7)
	claimed, _, _, err := store.Claim(context.Background(), tx, idempotency.Record{Key: key, Endpoint: "POST /v1/donations", RequestHash: h})
	if err != nil {
		_ = tx.Rollback()
		t.Fatal("claim:", err)
	}
	if !claimed {
		_ = tx.Rollback()
		t.Fatal("expected first claim to win")
	}
	if err := store.Finalize(context.Background(), tx, key, 201, []byte(`{"ok":true}`)); err != nil {
		_ = tx.Rollback()
		t.Fatal("finalize:", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal("commit:", err)
	}

	// Replay with same hash: returns cached.
	tx, err = db.Begin()
	must(err, "begin replay")
	defer func() { _ = tx.Rollback() }()
	claimed, prior, body, err := store.Claim(context.Background(), tx, idempotency.Record{Key: key, Endpoint: "POST /v1/donations", RequestHash: h})
	if err != nil {
		t.Fatal("replay claim:", err)
	}
	if claimed {
		t.Fatal("expected replay to not claim")
	}
	if prior != 201 || len(body) == 0 {
		t.Fatalf("cached response missing: prior=%d body=%s", prior, body)
	}
}

// TestClaimThenDifferentBodyReturnsErrDuplicate checks the conflicting-body
// rejection path.
func TestClaimThenDifferentBodyReturnsErrDuplicate(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	db := openDB(t)
	store := idempotency.NewStore(db)
	key := "test-key-" + sanitize(t.Name())

	hA := "hash-a-" + strings.Repeat("0", 64-7)
	hB := "hash-b-" + strings.Repeat("0", 64-7)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal("begin:", err)
	}
	if _, _, _, err := store.Claim(context.Background(), tx, idempotency.Record{Key: key, Endpoint: "POST /v1/donations", RequestHash: hA}); err != nil {
		_ = tx.Rollback()
		t.Fatal("first claim:", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal("commit:", err)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatal("begin 2:", err)
	}
	defer func() { _ = tx.Rollback() }()
	_, _, _, err = store.Claim(context.Background(), tx, idempotency.Record{Key: key, Endpoint: "POST /v1/donations", RequestHash: hB})
	if !errors.Is(err, idempotency.ErrDuplicate) {
		t.Fatalf("err = %v, want ErrDuplicate", err)
	}
}

// TestConcurrentClaimsSameKey verifies that concurrent requests for the same
// idempotency key with the same body produce exactly one winning claim.
func TestIncompleteRowReturnsInProgressNotStatusZero(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	db := openDB(t)
	store := idempotency.NewStore(db)
	key := "test-key-" + sanitize(t.Name())
	h := strings.Repeat("a", 64)

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	claimed, _, _, err := store.Claim(context.Background(), tx, idempotency.Record{Key: key, Endpoint: "POST /v1/donations", RequestHash: h})
	if err != nil || !claimed {
		t.Fatalf("first claim: claimed=%v err=%v", claimed, err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	tx, err = db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	claimed, status, body, err := store.Claim(context.Background(), tx, idempotency.Record{Key: key, Endpoint: "POST /v1/donations", RequestHash: h})
	if !errors.Is(err, idempotency.ErrInProgress) {
		t.Fatalf("err=%v, want ErrInProgress", err)
	}
	if claimed || status != 0 || len(body) != 0 {
		t.Fatalf("incomplete row replayed: claimed=%v status=%d body=%q", claimed, status, body)
	}
}

func TestStaleInProgressCanBeReclaimed(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	db := openDB(t)
	store := idempotency.NewStore(db)
	store.StaleAfter = time.Minute
	key := "test-key-" + sanitize(t.Name())
	h := strings.Repeat("b", 64)
	if _, err := db.Exec(`INSERT INTO idempotency (key, endpoint, request_hash, state, updated_at) VALUES ($1, $2, $3, 'IN_PROGRESS', NOW() - INTERVAL '2 minutes')`, key, "POST /v1/donations", h); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	claimed, status, body, err := store.Claim(context.Background(), tx, idempotency.Record{Key: key, Endpoint: "POST /v1/donations", RequestHash: h})
	if err != nil || !claimed || status != 0 || len(body) != 0 {
		t.Fatalf("stale claim: claimed=%v status=%d body=%q err=%v", claimed, status, body, err)
	}
}

func TestConcurrentClaimsSameKey(t *testing.T) {
	if os.Getenv("TEST_DATABASE_URL") == "" {
		t.Skip("TEST_DATABASE_URL is not set")
	}
	db := openDB(t)
	store := idempotency.NewStore(db)
	key := "test-key-" + sanitize(t.Name())
	var wg sync.WaitGroup
	var winners int64
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tx, err := db.Begin()
			if err != nil {
				t.Errorf("begin: %v", err)
				return
			}
			h := "hash-a-" + strings.Repeat("0", 64-7)
			claimed, _, _, err := store.Claim(context.Background(), tx, idempotency.Record{Key: key, Endpoint: "POST /v1/donations", RequestHash: h})
			if err != nil && !errors.Is(err, idempotency.ErrInProgress) {
				t.Errorf("claim: %v", err)
				_ = tx.Rollback()
				return
			}
			if claimed {
				_ = store.Finalize(context.Background(), tx, key, 201, []byte(`{}`))
				atomic.AddInt64(&winners, 1)
			}
			_ = tx.Commit()
		}()
	}
	wg.Wait()
	if winners != 1 {
		t.Fatalf("winners = %d, want 1", winners)
	}
}

// --- helpers ---

func openDB(t *testing.T) *sql.DB {
	t.Helper()
	rootDSN := os.Getenv("TEST_DATABASE_URL")
	name := "test_p5_idem_" + sanitize(t.Name())
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
	for _, n := range []string{"00001_init.up.sql", "00003_reliability.up.sql", "00004_palantir_topup_idempotency.up.sql", "00005_runtime_reliability.up.sql", "00006_donation_display_pricing_snapshot.up.sql"} {
		path := filepath.Join(dir, n)
		schema, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", n, err)
		}
		if _, err := db.Exec(string(schema)); err != nil {
			t.Fatalf("apply %s: %v", n, err)
		}
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
