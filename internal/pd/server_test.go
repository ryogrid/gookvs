package pd

import (
	"context"
	"testing"
	"time"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/pingcap/kvproto/pkg/pdpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func startTestPDServer(t *testing.T) (*PDServer, pdpb.PDClient) {
	t.Helper()
	cfg := DefaultPDServerConfig()
	cfg.ListenAddr = "127.0.0.1:0"

	s, err := NewPDServer(cfg)
	require.NoError(t, err)
	require.NoError(t, s.Start())
	t.Cleanup(func() { s.Stop() })

	conn, err := grpc.Dial(s.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return s, pdpb.NewPDClient(conn)
}

func TestGetMembers(t *testing.T) {
	_, client := startTestPDServer(t)

	resp, err := client.GetMembers(context.Background(), &pdpb.GetMembersRequest{})
	require.NoError(t, err)
	assert.NotNil(t, resp.GetHeader())
	assert.NotNil(t, resp.GetLeader())
	assert.Equal(t, "gookv-pd-1", resp.GetLeader().GetName())
}

func TestBootstrapFlow(t *testing.T) {
	_, client := startTestPDServer(t)
	ctx := context.Background()

	// Not bootstrapped initially.
	isBootResp, err := client.IsBootstrapped(ctx, &pdpb.IsBootstrappedRequest{})
	require.NoError(t, err)
	assert.False(t, isBootResp.GetBootstrapped())

	// Bootstrap.
	bootResp, err := client.Bootstrap(ctx, &pdpb.BootstrapRequest{
		Store: &metapb.Store{Id: 1, Address: "127.0.0.1:20160"},
		Region: &metapb.Region{
			Id: 1,
			Peers: []*metapb.Peer{{Id: 1, StoreId: 1}},
		},
	})
	require.NoError(t, err)
	assert.Nil(t, bootResp.GetHeader().GetError())

	// Now bootstrapped.
	isBootResp, err = client.IsBootstrapped(ctx, &pdpb.IsBootstrappedRequest{})
	require.NoError(t, err)
	assert.True(t, isBootResp.GetBootstrapped())

	// Second bootstrap should return error.
	bootResp, err = client.Bootstrap(ctx, &pdpb.BootstrapRequest{})
	require.NoError(t, err)
	assert.NotNil(t, bootResp.GetHeader().GetError())
}

func TestAllocID(t *testing.T) {
	_, client := startTestPDServer(t)
	ctx := context.Background()

	ids := make(map[uint64]bool)
	for i := 0; i < 10; i++ {
		resp, err := client.AllocID(ctx, &pdpb.AllocIDRequest{})
		require.NoError(t, err)
		id := resp.GetId()
		assert.False(t, ids[id], "ID %d should be unique", id)
		ids[id] = true
	}
	assert.Len(t, ids, 10)
}

func TestTSO(t *testing.T) {
	_, client := startTestPDServer(t)

	stream, err := client.Tso(context.Background())
	require.NoError(t, err)

	var lastPhysical int64
	var lastLogical int64

	for i := 0; i < 5; i++ {
		require.NoError(t, stream.Send(&pdpb.TsoRequest{Count: 1}))
		resp, err := stream.Recv()
		require.NoError(t, err)

		ts := resp.GetTimestamp()
		assert.True(t, ts.GetPhysical() > 0)

		// Verify monotonicity.
		if ts.GetPhysical() > lastPhysical || (ts.GetPhysical() == lastPhysical && ts.GetLogical() > lastLogical) {
			// OK, monotonic.
		} else {
			t.Errorf("TSO not monotonic: (%d,%d) <= (%d,%d)",
				ts.GetPhysical(), ts.GetLogical(), lastPhysical, lastLogical)
		}
		lastPhysical = ts.GetPhysical()
		lastLogical = ts.GetLogical()
	}

	stream.CloseSend()
}

func TestPutGetStore(t *testing.T) {
	_, client := startTestPDServer(t)
	ctx := context.Background()

	// Put a store.
	_, err := client.PutStore(ctx, &pdpb.PutStoreRequest{
		Store: &metapb.Store{Id: 1, Address: "127.0.0.1:20160"},
	})
	require.NoError(t, err)

	// Get the store.
	getResp, err := client.GetStore(ctx, &pdpb.GetStoreRequest{StoreId: 1})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), getResp.GetStore().GetId())
	assert.Equal(t, "127.0.0.1:20160", getResp.GetStore().GetAddress())

	// Get nonexistent store.
	getResp, err = client.GetStore(ctx, &pdpb.GetStoreRequest{StoreId: 999})
	require.NoError(t, err)
	assert.Nil(t, getResp.GetStore())
}

func TestGetAllStores(t *testing.T) {
	_, client := startTestPDServer(t)
	ctx := context.Background()

	for i := uint64(1); i <= 3; i++ {
		_, err := client.PutStore(ctx, &pdpb.PutStoreRequest{
			Store: &metapb.Store{Id: i, Address: "addr"},
		})
		require.NoError(t, err)
	}

	resp, err := client.GetAllStores(ctx, &pdpb.GetAllStoresRequest{})
	require.NoError(t, err)
	assert.Len(t, resp.GetStores(), 3)
}

func TestGetRegionByID(t *testing.T) {
	_, client := startTestPDServer(t)
	ctx := context.Background()

	// Bootstrap with a region.
	_, err := client.Bootstrap(ctx, &pdpb.BootstrapRequest{
		Store:  &metapb.Store{Id: 1, Address: "addr"},
		Region: &metapb.Region{Id: 1, StartKey: nil, EndKey: nil},
	})
	require.NoError(t, err)

	// Get region by ID.
	resp, err := client.GetRegionByID(ctx, &pdpb.GetRegionByIDRequest{RegionId: 1})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), resp.GetRegion().GetId())
}

func TestGetRegionByKey(t *testing.T) {
	s, client := startTestPDServer(t)
	ctx := context.Background()

	// Add a region covering all keys.
	s.meta.PutRegion(&metapb.Region{Id: 1, StartKey: nil, EndKey: nil}, nil)

	resp, err := client.GetRegion(ctx, &pdpb.GetRegionRequest{RegionKey: []byte("somekey")})
	require.NoError(t, err)
	assert.Equal(t, uint64(1), resp.GetRegion().GetId())
}

func TestAskBatchSplit(t *testing.T) {
	_, client := startTestPDServer(t)
	ctx := context.Background()

	resp, err := client.AskBatchSplit(ctx, &pdpb.AskBatchSplitRequest{
		Region:     &metapb.Region{Id: 1},
		SplitCount: 2,
	})
	require.NoError(t, err)
	assert.Len(t, resp.GetIds(), 2)

	for _, splitID := range resp.GetIds() {
		assert.True(t, splitID.GetNewRegionId() > 0)
		assert.True(t, len(splitID.GetNewPeerIds()) > 0)
	}
}

func TestGCSafePoint(t *testing.T) {
	_, client := startTestPDServer(t)
	ctx := context.Background()

	// Initially 0.
	getResp, err := client.GetGCSafePoint(ctx, &pdpb.GetGCSafePointRequest{})
	require.NoError(t, err)
	assert.Equal(t, uint64(0), getResp.GetSafePoint())

	// Update to 100.
	updateResp, err := client.UpdateGCSafePoint(ctx, &pdpb.UpdateGCSafePointRequest{SafePoint: 100})
	require.NoError(t, err)
	assert.Equal(t, uint64(100), updateResp.GetNewSafePoint())

	// Verify.
	getResp, err = client.GetGCSafePoint(ctx, &pdpb.GetGCSafePointRequest{})
	require.NoError(t, err)
	assert.Equal(t, uint64(100), getResp.GetSafePoint())

	// No regression.
	updateResp, err = client.UpdateGCSafePoint(ctx, &pdpb.UpdateGCSafePointRequest{SafePoint: 50})
	require.NoError(t, err)
	assert.Equal(t, uint64(100), updateResp.GetNewSafePoint())
}

func TestTSOAllocator_Monotonic(t *testing.T) {
	alloc := NewTSOAllocator(0)

	var lastP, lastL int64
	for i := 0; i < 100; i++ {
		ts, err := alloc.Allocate(1)
		require.NoError(t, err)

		p := ts.GetPhysical()
		l := ts.GetLogical()

		if p < lastP || (p == lastP && l <= lastL) {
			t.Fatalf("TSO not monotonic at iteration %d: (%d,%d) <= (%d,%d)", i, p, l, lastP, lastL)
		}
		lastP = p
		lastL = l
	}
}

func TestIDAllocator(t *testing.T) {
	alloc := NewIDAllocator()

	ids := make(map[uint64]bool)
	for i := 0; i < 100; i++ {
		id := alloc.Alloc()
		assert.False(t, ids[id])
		ids[id] = true
	}
	assert.Len(t, ids, 100)
}

func TestMetadataStore(t *testing.T) {
	meta := NewMetadataStore(1, 30*time.Second, 30*time.Minute)

	assert.False(t, meta.IsBootstrapped())
	meta.SetBootstrapped(true)
	assert.True(t, meta.IsBootstrapped())

	meta.PutStore(&metapb.Store{Id: 1, Address: "addr1"})
	meta.PutStore(&metapb.Store{Id: 2, Address: "addr2"})
	assert.Equal(t, "addr1", meta.GetStore(1).GetAddress())
	assert.Nil(t, meta.GetStore(99))
	assert.Len(t, meta.GetAllStores(), 2)

	meta.PutRegion(&metapb.Region{Id: 1, StartKey: nil, EndKey: nil}, &metapb.Peer{Id: 1})
	region, leader := meta.GetRegionByID(1)
	assert.NotNil(t, region)
	assert.NotNil(t, leader)

	region, _ = meta.GetRegionByKey([]byte("test"))
	assert.NotNil(t, region)
}
