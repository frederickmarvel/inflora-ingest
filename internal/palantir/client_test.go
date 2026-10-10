package palantir

import (
	"context"
	"net"
	"testing"

	sharedauth "github.com/frederickmarvel/inflora-shared/auth"
	pb "github.com/frederickmarvel/inflora-shared/gen/go/palantir/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type authenticatedTopUpServer struct {
	pb.UnimplementedTopUpServiceServer
}

func (authenticatedTopUpServer) CreateTopUp(context.Context, *pb.CreateTopUpRequest) (*pb.CreateTopUpResponse, error) {
	return &pb.CreateTopUpResponse{ChargeId: "charge-authenticated"}, nil
}

func (authenticatedTopUpServer) SettleTopUp(context.Context, *pb.SettleTopUpRequest) (*pb.SettleTopUpResponse, error) {
	return &pb.SettleTopUpResponse{DonationId: "donation-authenticated"}, nil
}

func TestClientInstallsBearerMetadata(t *testing.T) {
	client := startAuthenticatedServer(t, "test-engine-key", "test-engine-key")
	resp, err := client.CreateTopUp(context.Background(), &pb.CreateTopUpRequest{})
	if err != nil {
		t.Fatalf("authenticated call: %v", err)
	}
	if resp.ChargeId != "charge-authenticated" {
		t.Fatalf("charge id = %q", resp.ChargeId)
	}
}

func TestSettleClientInstallsBearerMetadata(t *testing.T) {
	client := startAuthenticatedServer(t, "test-engine-key", "test-engine-key")
	resp, err := client.SettleTopUp(context.Background(), &pb.SettleTopUpRequest{})
	if err != nil || resp.DonationId != "donation-authenticated" {
		t.Fatalf("resp=%v err=%v", resp, err)
	}
}

func TestClientRejectsWrongBearer(t *testing.T) {
	client := startAuthenticatedServer(t, "expected", "wrong")
	_, err := client.CreateTopUp(context.Background(), &pb.CreateTopUpRequest{})
	if status.Code(err) != codes.Unauthenticated {
		t.Fatalf("code = %s, want %s (err=%v)", status.Code(err), codes.Unauthenticated, err)
	}
}

func startAuthenticatedServer(t *testing.T, serverKey, clientKey string) *Client {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.UnaryInterceptor(sharedauth.GRPCBearerAuth(serverKey)))
	pb.RegisterTopUpServiceServer(server, authenticatedTopUpServer{})
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	client, err := New(context.Background(), lis.Addr().String(), clientKey)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
