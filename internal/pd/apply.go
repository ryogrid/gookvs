package pd

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

// applyCommand applies a single PDCommand to the server's in-memory state.
// It returns an optional result byte slice (for commands that produce output)
// and an error if the command fails.
func (s *PDServer) applyCommand(cmd PDCommand) ([]byte, error) {
	switch cmd.Type {
	case CmdSetBootstrapped:
		if cmd.Bootstrapped == nil {
			return nil, fmt.Errorf("applyCommand: CmdSetBootstrapped: Bootstrapped field is nil")
		}
		s.meta.SetBootstrapped(*cmd.Bootstrapped)
		// Atomic bootstrap: if Store and Region are included, apply them
		// in the same proposal to avoid partially-bootstrapped state.
		if cmd.Store != nil {
			s.meta.PutStore(cmd.Store)
		}
		if cmd.Region != nil {
			s.meta.PutRegion(cmd.Region, cmd.Leader)
		}
		return nil, nil

	case CmdPutStore:
		s.meta.PutStore(cmd.Store)
		return nil, nil

	case CmdPutRegion:
		s.meta.PutRegion(cmd.Region, cmd.Leader)
		return nil, nil

	case CmdUpdateStoreStats:
		s.meta.UpdateStoreStats(cmd.StoreID, cmd.StoreStats)
		return nil, nil

	case CmdSetStoreState:
		if cmd.StoreState == nil {
			return nil, fmt.Errorf("applyCommand: CmdSetStoreState: StoreState field is nil")
		}
		s.meta.SetStoreState(cmd.StoreID, *cmd.StoreState)
		return nil, nil

	case CmdTSOAllocate:
		ts, err := s.tso.Allocate(cmd.TSOBatchSize)
		if err != nil {
			return nil, fmt.Errorf("applyCommand: CmdTSOAllocate: %w", err)
		}
		data, err := json.Marshal(ts)
		if err != nil {
			return nil, fmt.Errorf("applyCommand: CmdTSOAllocate: marshal timestamp: %w", err)
		}
		return data, nil

	case CmdIDAlloc:
		batchSize := cmd.IDBatchSize
		if batchSize <= 0 {
			batchSize = 1
		}
		var id uint64
		for i := 0; i < batchSize; i++ {
			id = s.idAlloc.Alloc()
		}
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, id)
		return buf, nil

	case CmdUpdateGCSafePoint:
		newSP := s.gcMgr.UpdateSafePoint(cmd.GCSafePoint)
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf, newSP)
		return buf, nil

	case CmdStartMove:
		s.moveTracker.StartMove(cmd.MoveRegionID, cmd.MoveSourcePeer, cmd.MoveTargetStoreID)
		return nil, nil

	case CmdAdvanceMove:
		// Advance returns a *ScheduleCommand which we intentionally discard;
		// the apply path only updates internal state.
		s.moveTracker.Advance(cmd.MoveRegionID, cmd.AdvanceRegion, cmd.AdvanceLeader)
		return nil, nil

	case CmdCleanupStaleMove:
		s.moveTracker.CleanupStale(cmd.CleanupTimeout)
		return nil, nil

	case CmdCompactLog:
		as := s.raftStorage.GetApplyState()
		if cmd.CompactIndex <= as.TruncatedIndex {
			return nil, nil // already compacted
		}
		as.TruncatedIndex = cmd.CompactIndex
		as.TruncatedTerm = cmd.CompactTerm
		s.raftStorage.SetApplyState(as)
		s.raftStorage.CompactTo(cmd.CompactIndex + 1)
		// Schedule background engine deletion.
		go s.raftStorage.DeleteEntriesTo(cmd.CompactIndex + 1)
		return nil, nil

	default:
		return nil, fmt.Errorf("applyCommand: unknown command type %d", cmd.Type)
	}
}
