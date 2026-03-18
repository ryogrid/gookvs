package e2e

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// dialTikvClient creates a gRPC connection to the given address and returns a TikvClient.
func dialTikvClient(t *testing.T, addr string) (*grpc.ClientConn, tikvpb.TikvClient) {
	t.Helper()
	conn, err := grpc.Dial(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })
	return conn, tikvpb.NewTikvClient(conn)
}
