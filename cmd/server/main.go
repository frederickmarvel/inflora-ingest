// Package main is the Ingest composition root. Phase 5 wires:
//   - PostgreSQL connection
//   - HTTP server on :8081
//   - Palantir gRPC client
//   - Idempotency table helper
//   - Outbox writes
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	sharedconfig "github.com/frederickmarvel/inflora-shared/config"
	shareddb "github.com/frederickmarvel/inflora-shared/db"
	"github.com/frederickmarvel/inflora-shared/events"
	"github.com/frederickmarvel/inflora-shared/middleware"
	"github.com/frederickmarvel/inflora-shared/observability"
	"github.com/frederickmarvel/inflora-shared/outbox"
	sharedruntime "github.com/frederickmarvel/inflora-shared/runtime"

	"github.com/frederickmarvel/inflora-ingest/internal/handler"
	"github.com/frederickmarvel/inflora-ingest/internal/idempotency"
	"github.com/frederickmarvel/inflora-ingest/internal/palantir"

	"go.uber.org/zap"
)

func main() {
	base, err := sharedconfig.Load("ingest")
	if err != nil {
		fmt.Fprintln(os.Stderr, "config:", err)
		os.Exit(2)
	}
	logger, err := observability.NewLogger(base.ServiceName, base.LogLevel, base.LogFormat)
	if err != nil {
		fmt.Fprintln(os.Stderr, "logger:", err)
		os.Exit(2)
	}
	defer observability.Sync(logger)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if base.Database.DSN == "" {
		logger.Fatal("DB_DSN is required")
	}
	if base.NATS.URL == "" {
		logger.Fatal("NATS_URL is required")
	}
	database, err := shareddb.Open(ctx, shareddb.Config{DSN: base.Database.DSN, MaxOpenConns: base.Database.MaxOpenConns, MaxIdleConns: base.Database.MaxIdleConns, ConnMaxLifetime: base.Database.ConnMaxLifetime, PingTimeout: 5 * time.Second})
	if err != nil {
		logger.Fatal("connect database", zap.Error(err))
	}
	defer func() { _ = database.Close() }()
	palantirAddr := getenv("PALANTIR_GRPC_ADDR", "palantir:7001")
	palantirKey := os.Getenv("SM_GATEWAY_ENGINE_API_KEY")
	if palantirKey == "" {
		logger.Fatal("SM_GATEWAY_ENGINE_API_KEY is required")
	}
	pc, err := palantir.New(ctx, palantirAddr, palantirKey)
	if err != nil {
		logger.Fatal("connect palantir", zap.Error(err))
	}
	defer func() { _ = pc.Close() }()
	publisher, err := events.NewNATSPublisher(events.NATSConfig{URL: base.NATS.URL, Name: base.ServiceName, Stream: base.NATS.StreamName, Subjects: []string{">"}})
	if err != nil {
		logger.Fatal("connect nats", zap.Error(err))
	}
	defer func() { _ = publisher.Close() }()
	dispatcher, err := outbox.NewDispatcher(database, publisher, outbox.DispatcherConfig{WorkerName: "ingest-outbox", Producer: "ingest"})
	if err != nil {
		logger.Fatal("configure outbox", zap.Error(err))
	}
	idem := idempotency.NewStore(database)
	// PROVIDER_NAME keys the mdr_rates lookup inside donation-intent creation
	// (D-061). It must agree with palantir's PROVIDER_DEFAULT (both "pivot").
	providerName := getenv("PROVIDER_NAME", "pivot")
	dh := handler.NewDonations(database, idem, pc, providerName)
	wh := handler.NewPaymentWebhook(pc)
	muxH := http.NewServeMux()
	muxH.Handle("/v1/donations", middleware.RequestID(middleware.Logging(logger)(http.HandlerFunc(dh.Handle))))
	muxH.Handle("/v1/webhooks/payment", middleware.RequestID(middleware.Logging(logger)(http.HandlerFunc(wh.Handle))))
	muxH.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	corsHandler := middleware.CORS(middleware.CORSConfig{
		AllowedOrigins: corsOrigins(),
		AllowedMethods: []string{"GET", "POST"},
		AllowedHeaders: []string{"Authorization", "Content-Type", "Idempotency-Key", "X-Request-Id"},
		MaxAge:         600 * time.Second,
	})(muxH)
	srv := &http.Server{Addr: base.HTTPAddr, Handler: corsHandler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second}
	worker := &httpServer{srv: srv, logger: logger}
	probe := func(context.Context) (bool, error) { return database.PingContext(context.Background()) == nil, nil }
	logger.Info("ingest listening", zap.String("addr", base.HTTPAddr), zap.String("version", base.Version))
	if err := sharedruntime.Run(ctx, 5*time.Second, 10*time.Second, probe, worker, dispatcher); err != nil && !errors.Is(err, context.Canceled) {
		logger.Error("shutdown", zap.Error(err))
	}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// corsOrigins reads CORS_ALLOWED_ORIGINS as a CSV and falls back to the
// production default. Used by both the regular handler and the preflight
// reply path inside middleware.CORS.
func corsOrigins() []string {
	v := strings.TrimSpace(os.Getenv("CORS_ALLOWED_ORIGINS"))
	if v == "" {
		return []string{"https://app.inflora.app"}
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

type httpServer struct {
	srv    *http.Server
	logger *zap.Logger
}

func (h *httpServer) Name() string { return "ingest-http" }
func (h *httpServer) Run(ctx context.Context) error {
	errs := make(chan error, 1)
	go func() { errs <- h.srv.ListenAndServe() }()
	select {
	case <-ctx.Done():
		shCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = h.srv.Shutdown(shCtx)
		return nil
	case err := <-errs:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			h.logger.Error("http serve", zap.Error(err))
			return err
		}
		return nil
	}
}
