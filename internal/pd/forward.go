package pd

import (
	"context"
	"fmt"

	"github.com/pingcap/kvproto/pkg/pdpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// getLeaderClient returns a PDClient connected to the current Raft leader.
// It caches the connection and reuses it as long as the leader hasn't changed.
func (s *PDServer) getLeaderClient() (pdpb.PDClient, error) {
	s.leaderConnMu.Lock()
	defer s.leaderConnMu.Unlock()

	leaderID := s.raftPeer.LeaderID()
	if leaderID == 0 {
		return nil, status.Error(codes.Unavailable, "pd: no PD leader")
	}

	// Reuse cached connection if leader hasn't changed.
	if s.cachedLeaderConn != nil && s.cachedLeaderID == leaderID {
		return pdpb.NewPDClient(s.cachedLeaderConn), nil
	}

	// Leader changed: close old connection.
	if s.cachedLeaderConn != nil {
		s.cachedLeaderConn.Close()
		s.cachedLeaderConn = nil
		s.cachedLeaderID = 0
	}

	// Look up client address for the leader.
	addr, ok := s.raftCfg.ClientAddrs[leaderID]
	if !ok {
		return nil, fmt.Errorf("pd: no client address for leader %d", leaderID)
	}

	conn, err := grpc.NewClient(addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, fmt.Errorf("pd: dial leader %d at %s: %w", leaderID, addr, err)
	}

	s.cachedLeaderConn = conn
	s.cachedLeaderID = leaderID
	return pdpb.NewPDClient(conn), nil
}

// forwardBootstrap forwards a Bootstrap request to the current leader.
func (s *PDServer) forwardBootstrap(ctx context.Context, req *pdpb.BootstrapRequest) (*pdpb.BootstrapResponse, error) {
	client, err := s.getLeaderClient()
	if err != nil {
		return nil, err
	}
	return client.Bootstrap(ctx, req)
}

// forwardPutStore forwards a PutStore request to the current leader.
func (s *PDServer) forwardPutStore(ctx context.Context, req *pdpb.PutStoreRequest) (*pdpb.PutStoreResponse, error) {
	client, err := s.getLeaderClient()
	if err != nil {
		return nil, err
	}
	return client.PutStore(ctx, req)
}

// forwardAllocID forwards an AllocID request to the current leader.
func (s *PDServer) forwardAllocID(ctx context.Context, req *pdpb.AllocIDRequest) (*pdpb.AllocIDResponse, error) {
	client, err := s.getLeaderClient()
	if err != nil {
		return nil, err
	}
	return client.AllocID(ctx, req)
}

// forwardUpdateGCSafePoint forwards an UpdateGCSafePoint request to the current leader.
func (s *PDServer) forwardUpdateGCSafePoint(ctx context.Context, req *pdpb.UpdateGCSafePointRequest) (*pdpb.UpdateGCSafePointResponse, error) {
	client, err := s.getLeaderClient()
	if err != nil {
		return nil, err
	}
	return client.UpdateGCSafePoint(ctx, req)
}

// forwardStoreHeartbeat forwards a StoreHeartbeat request to the current leader.
func (s *PDServer) forwardStoreHeartbeat(ctx context.Context, req *pdpb.StoreHeartbeatRequest) (*pdpb.StoreHeartbeatResponse, error) {
	client, err := s.getLeaderClient()
	if err != nil {
		return nil, err
	}
	return client.StoreHeartbeat(ctx, req)
}

// forwardAskBatchSplit forwards an AskBatchSplit request to the current leader.
func (s *PDServer) forwardAskBatchSplit(ctx context.Context, req *pdpb.AskBatchSplitRequest) (*pdpb.AskBatchSplitResponse, error) {
	client, err := s.getLeaderClient()
	if err != nil {
		return nil, err
	}
	return client.AskBatchSplit(ctx, req)
}

// forwardReportBatchSplit forwards a ReportBatchSplit request to the current leader.
func (s *PDServer) forwardReportBatchSplit(ctx context.Context, req *pdpb.ReportBatchSplitRequest) (*pdpb.ReportBatchSplitResponse, error) {
	client, err := s.getLeaderClient()
	if err != nil {
		return nil, err
	}
	return client.ReportBatchSplit(ctx, req)
}

// forwardTso proxies a bidirectional Tso stream from a follower to the leader.
func (s *PDServer) forwardTso(stream pdpb.PD_TsoServer) error {
	client, err := s.getLeaderClient()
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}

	// Open a Tso stream to the leader.
	leaderStream, err := client.Tso(stream.Context())
	if err != nil {
		return status.Error(codes.Unavailable, "failed to connect to PD leader: "+err.Error())
	}

	// Proxy loop: read from client, forward to leader, read response, send to client.
	for {
		req, err := stream.Recv()
		if err != nil {
			// Client closed or error.
			leaderStream.CloseSend()
			return err
		}

		if err := leaderStream.Send(req); err != nil {
			return status.Error(codes.Unavailable, "leader stream send failed: "+err.Error())
		}

		resp, err := leaderStream.Recv()
		if err != nil {
			return status.Error(codes.Unavailable, "leader stream recv failed: "+err.Error())
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

// forwardRegionHeartbeat proxies a bidirectional RegionHeartbeat stream from a follower to the leader.
func (s *PDServer) forwardRegionHeartbeat(stream pdpb.PD_RegionHeartbeatServer) error {
	client, err := s.getLeaderClient()
	if err != nil {
		return status.Error(codes.Unavailable, err.Error())
	}

	leaderStream, err := client.RegionHeartbeat(stream.Context())
	if err != nil {
		return status.Error(codes.Unavailable, "failed to connect to PD leader: "+err.Error())
	}

	// Same proxy pattern as forwardTso.
	for {
		req, err := stream.Recv()
		if err != nil {
			leaderStream.CloseSend()
			return err
		}

		if err := leaderStream.Send(req); err != nil {
			return status.Error(codes.Unavailable, "leader stream send failed: "+err.Error())
		}

		resp, err := leaderStream.Recv()
		if err != nil {
			return status.Error(codes.Unavailable, "leader stream recv failed: "+err.Error())
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}
