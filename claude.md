# AI Repository Guide: inflora-ingest

## Purpose

`inflora-ingest` is Inflora's public HTTP ingress for donor-facing donation intents and payment-provider webhooks. It validates external requests, applies idempotency, coordinates internal services, and emits canonical events. The canonical HTTP port is `8081`.

This service does not own the ledger or database migrations. Financial state transitions belong to Saruman, provider operations belong to Palantir, and shared contracts belong to `inflora-shared`.

## Important paths

- `cmd/server/`: process composition and startup.
- `internal/handler/`: public donation and webhook handlers.
- `internal/idempotency/`: request claiming, replay, and recovery behavior.
- `internal/palantir/`: internal payment-broker client.
- `internal/saruman/`: internal donation-engine client.
- `internal/events/`: event publication integration.
- `internal/repo/`: PostgreSQL access.
- `internal/signature/`: webhook authentication/signature validation.

## Contract and correctness rules

- Read `/Users/frederickmarvel/Inflora/almanac/planning/WIRE_GUIDE.md` and relevant files under `almanac/planning/schemas/` before changing behavior.
- Money is integer IDR; never use floating-point arithmetic for amounts or pricing.
- Donation display duration is frozen when an intent is created: `clamp(floor(amount_idr / display_rate_idr_per_sec), display_min_sec, display_max_sec)`.
- Idempotent retries must return the original persisted snapshot, not recompute using current streamer settings.
- Keep database changes in `inflora-saruman`; keep reusable event/RPC definitions in `inflora-shared`.
- Preserve transaction, outbox, retry, and webhook replay guarantees.

## Commands

```sh
make tidy
make lint
make test
make build
make run
```

Run focused Go tests while iterating, then `go test ./...`, `go vet ./...`, and `git diff --check` before finishing. Some integration tests require external PostgreSQL/NATS configuration; report skipped infrastructure-dependent coverage explicitly.
