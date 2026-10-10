// Package idempotency is the HTTP-layer idempotency record store.
package idempotency

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

const (
	StateInProgress = "IN_PROGRESS"
	StateCompleted  = "COMPLETED"

	defaultStaleAfter = 5 * time.Minute
)

var (
	// ErrDuplicate is returned when the same idempotency key has been seen with
	// a different request hash.
	ErrDuplicate = errors.New("idempotency: duplicate body hash")
	// ErrInProgress is returned when a non-stale request with the same key and
	// body is still being processed. Callers must not replay an empty response.
	ErrInProgress = errors.New("idempotency: request in progress")
)

// Record is the on-disk idempotency row.
type Record struct {
	Key            string
	Endpoint       string
	RequestHash    string
	ResponseStatus int
	ResponseBody   []byte
}

// Store is the idempotency table wrapper.
type Store struct {
	DB         *sql.DB
	StaleAfter time.Duration
}

// NewStore constructs the store.
func NewStore(db *sql.DB) *Store { return &Store{DB: db, StaleAfter: defaultStaleAfter} }

// Claim inserts an IN_PROGRESS record and returns whether this call owns it.
// Completed records replay their cached response. A live IN_PROGRESS record
// returns ErrInProgress; a stale one is atomically reclaimed.
func (s *Store) Claim(ctx context.Context, tx *sql.Tx, rec Record) (claimed bool, priorStatus int, priorBody []byte, err error) {
	if tx == nil {
		return false, 0, nil, fmt.Errorf("idempotency: nil tx")
	}
	staleAfter := s.StaleAfter
	if staleAfter <= 0 {
		staleAfter = defaultStaleAfter
	}
	row := tx.QueryRowContext(ctx, `
		INSERT INTO idempotency (key, endpoint, request_hash, state, updated_at)
		VALUES ($1, $2, $3, 'IN_PROGRESS', NOW())
		ON CONFLICT (key) DO UPDATE SET
			state = 'IN_PROGRESS', updated_at = NOW(), processed_at = NULL,
			response_status = NULL, response_body = NULL
		WHERE idempotency.request_hash = EXCLUDED.request_hash
		  AND idempotency.state = 'IN_PROGRESS'
		  AND idempotency.updated_at < NOW() - ($4 * INTERVAL '1 microsecond')
		RETURNING key
	`, rec.Key, rec.Endpoint, rec.RequestHash, staleAfter.Microseconds())
	var key string
	if err = row.Scan(&key); err == nil {
		return true, 0, nil, nil
	} else if !errors.Is(err, sql.ErrNoRows) {
		return false, 0, nil, fmt.Errorf("idempotency: claim: %w", err)
	}

	var hash, state string
	var status sql.NullInt64
	var body []byte
	if err := tx.QueryRowContext(ctx, `
		SELECT request_hash, state, response_status, response_body
		FROM idempotency WHERE key = $1
	`, rec.Key).Scan(&hash, &state, &status, &body); err != nil {
		return false, 0, nil, fmt.Errorf("idempotency: inspect claim: %w", err)
	}
	if hash != rec.RequestHash {
		return false, 0, nil, ErrDuplicate
	}
	if state == StateCompleted && status.Valid && status.Int64 > 0 && len(body) > 0 {
		return false, int(status.Int64), body, nil
	}
	return false, 0, nil, ErrInProgress
}

// Finalize writes the response status / body for a successful claim.
func (s *Store) Finalize(ctx context.Context, tx *sql.Tx, key string, status int, body []byte) error {
	if tx == nil {
		return fmt.Errorf("idempotency: nil tx")
	}
	if status < 100 || status > 599 || len(body) == 0 {
		return fmt.Errorf("idempotency: invalid completed response")
	}
	result, err := tx.ExecContext(ctx, `UPDATE idempotency SET state = 'COMPLETED', response_status = $2, response_body = $3, processed_at = NOW(), updated_at = NOW() WHERE key = $1 AND state = 'IN_PROGRESS'`, key, status, body)
	if err != nil {
		return fmt.Errorf("idempotency: finalize: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("idempotency: finalize rows: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("idempotency: finalize: no in-progress claim for key")
	}
	return nil
}
