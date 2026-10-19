// Package palantir is the gRPC client wrapper for Ingest's calls to Palantir.
package palantir

import (
	"context"
	"time"

	sharedauth "github.com/frederickmarvel/inflora-shared/auth"
	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	dialTimeout = 5 * time.Second
	rpcTimeout  = 5 * time.Second
)

// Client is a thin wrapper around the generated TopUpServiceClient and
// WithdrawalServiceClient.
type Client struct {
	pb.TopUpServiceClient
	withdrawals pb.WithdrawalServiceClient
	conn        *grpc.ClientConn
}

// New dials the Palantir service and authenticates every unary RPC with the
// shared engine API key.
func New(ctx context.Context, addr, engineAPIKey string, opts ...grpc.DialOption) (*Client, error) {
	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	defer cancel()

	dialOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		sharedauth.GRPCBearerAuthClient(engineAPIKey),
		grpc.WithBlock(),
	}
	dialOpts = append(dialOpts, opts...)
	conn, err := grpc.DialContext(dialCtx, addr, dialOpts...)
	if err != nil {
		return nil, err
	}
	return &Client{TopUpServiceClient: pb.NewTopUpServiceClient(conn), withdrawals: pb.NewWithdrawalServiceClient(conn), conn: conn}, nil
}

// Close releases the underlying gRPC connection.
func (c *Client) Close() error {
	if c.conn != nil {
		return c.conn.Close()
	}
	return nil
}

// CreateTopUp calls Palantir with the supplied request. Phase 5 only uses
// this RPC; SettleTopUp is invoked by the webhook edge in Phase 6.
func (c *Client) CreateTopUp(ctx context.Context, req *pb.CreateTopUpRequest) (*pb.CreateTopUpResponse, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	return c.TopUpServiceClient.CreateTopUp(rpcCtx, req)
}

// SettleTopUp forwards a provider webhook to Palantir. RawPayload, Signature,
// and Headers must be preserved exactly enough for provider verification.
func (c *Client) SettleTopUp(ctx context.Context, req *pb.SettleTopUpRequest) (*pb.SettleTopUpResponse, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	return c.TopUpServiceClient.SettleTopUp(rpcCtx, req)
}

// SettleWithdrawal forwards a provider payout webhook to Palantir (T15).
func (c *Client) SettleWithdrawal(ctx context.Context, req *pb.SettleWithdrawalRequest) (*pb.SettleWithdrawalResponse, error) {
	rpcCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
	defer cancel()
	return c.withdrawals.SettleWithdrawal(rpcCtx, req)
}
