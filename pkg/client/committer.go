package client

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"github.com/ryogrid/gookv/pkg/txntypes"
	"golang.org/x/sync/errgroup"
)

// mutationWithKey is a mutation paired with its key for commit processing.
type mutationWithKey struct {
	key   []byte
	op    kvrpcpb.Op
	value []byte
}

// twoPhaseCommitter executes the two-phase commit protocol.
type twoPhaseCommitter struct {
	client       *TxnKVClient
	startTS      txntypes.TimeStamp
	commitTS     txntypes.TimeStamp
	mutations    []mutationWithKey
	primary              []byte
	opts                 TxnOptions
	prewriteDone         bool
	commitStatusUnknown  bool // set when primary commit status is indeterminate
}

// newTwoPhaseCommitter creates a committer from a TxnHandle's buffered mutations.
// Must be called with t.mu held.
func newTwoPhaseCommitter(t *TxnHandle) *twoPhaseCommitter {
	c := &twoPhaseCommitter{
		client:  t.client,
		startTS: t.startTS,
		opts:    t.opts,
	}

	for k, entry := range t.membuf {
		c.mutations = append(c.mutations, mutationWithKey{
			key:   []byte(k),
			op:    entry.op,
			value: entry.value,
		})
	}

	c.selectPrimary()
	return c
}

// selectPrimary sorts mutations by key and selects the first as primary.
func (c *twoPhaseCommitter) selectPrimary() {
	sort.Slice(c.mutations, func(i, j int) bool {
		return string(c.mutations[i].key) < string(c.mutations[j].key)
	})
	if len(c.mutations) > 0 {
		c.primary = c.mutations[0].key
	}
}

// execute runs the full 2PC protocol.
func (c *twoPhaseCommitter) execute(ctx context.Context) error {
	if err := c.prewrite(ctx); err != nil {
		// Attempt cleanup on prewrite failure.
		_ = c.rollback(context.Background())
		return err
	}
	c.prewriteDone = true

	// Get commit timestamp.
	ts, err := c.client.pdClient.GetTS(ctx)
	if err != nil {
		_ = c.rollback(context.Background())
		return fmt.Errorf("get commit timestamp: %w", err)
	}
	c.commitTS = txntypes.TimeStamp(ts.ToUint64())

	// Commit primary synchronously — must succeed.
	if err := c.commitPrimary(ctx); err != nil {
		// The primary commit may have succeeded at the Raft level but the
		// response was lost (e.g., timeout during leader change after split).
		// Check the actual transaction status before rolling back.
		status, checkErr := c.checkPrimaryStatus(ctx)
		if checkErr != nil {
			// Cannot determine primary status — don't rollback (might be committed).
			// Mark as unknown so that the caller's Rollback() is a no-op,
			// leaving locks in place for lock resolution to handle correctly.
			slog.Warn("checkPrimaryStatus failed, not rolling back",
				"startTS", c.startTS, "commitErr", err, "checkErr", checkErr)
			c.commitStatusUnknown = true
			return err
		}
		switch status {
		case primaryCommitted:
			// Primary IS committed — proceed with secondaries.
			slog.Info("commitPrimary returned error but primary is committed, proceeding",
				"startTS", c.startTS, "err", err)
			c.commitSecondaries(context.Background())
			return nil
		case primaryStillLocked:
			// Lock still present but commit Raft entry may be in the pipeline
			// (proposed but not yet applied). Do NOT rollback — if the entry
			// is committed at Raft level, rolling back secondaries while the
			// primary commit is pending causes balance inconsistency.
			// Let lock resolution handle the correct outcome.
			slog.Warn("primary still locked after commit attempt, not rolling back",
				"startTS", c.startTS, "commitErr", err)
			c.commitStatusUnknown = true
			return err
		default:
			// Primary is definitely not committed (rolled back or not found).
			_ = c.rollback(context.Background())
			return err
		}
	}

	// Commit secondaries synchronously to ensure no orphan locks.
	c.commitSecondaries(context.Background())

	return nil
}

// prewrite sends PrewriteRequests grouped by region.
func (c *twoPhaseCommitter) prewrite(ctx context.Context) error {
	groups, err := c.groupMutationsByRegion(ctx)
	if err != nil {
		return err
	}

	// Find the primary's region group and prewrite it first (synchronously).
	var primaryGroup uint64
	for regionID, muts := range groups {
		for _, m := range muts {
			if string(m.key) == string(c.primary) {
				primaryGroup = regionID
				break
			}
		}
		if primaryGroup != 0 {
			break
		}
	}

	if primaryGroup != 0 {
		if err := c.prewriteRegion(ctx, groups[primaryGroup]); err != nil {
			return err
		}
	}

	// Prewrite secondary regions in parallel.
	g, gCtx := errgroup.WithContext(ctx)
	for regionID, muts := range groups {
		if regionID == primaryGroup {
			continue
		}
		muts := muts
		g.Go(func() error {
			return c.prewriteRegion(gCtx, muts)
		})
	}
	return g.Wait()
}

// prewriteRegion sends a PrewriteRequest for mutations in a single region.
func (c *twoPhaseCommitter) prewriteRegion(ctx context.Context, muts []mutationWithKey) error {
	protoMuts := make([]*kvrpcpb.Mutation, len(muts))
	for i, m := range muts {
		protoMuts[i] = &kvrpcpb.Mutation{
			Op:    m.op,
			Key:   m.key,
			Value: m.value,
		}
	}

	req := &kvrpcpb.PrewriteRequest{
		Mutations:    protoMuts,
		PrimaryLock:  c.primary,
		StartVersion: uint64(c.startTS),
		LockTtl:      c.opts.LockTTL,
		TxnSize:      uint64(len(c.mutations)),
	}

	if c.opts.Try1PC && len(c.mutations) == len(muts) {
		req.TryOnePc = true
	}
	if c.opts.UseAsyncCommit {
		req.UseAsyncCommit = true
		// Populate secondaries on primary's request.
		for _, m := range c.mutations {
			if string(m.key) != string(c.primary) {
				req.Secondaries = append(req.Secondaries, m.key)
			}
		}
	}

	return c.client.sender.SendToRegion(ctx, muts[0].key, func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
		req.Context = buildContext(info)
		resp, err := client.KvPrewrite(ctx, req)
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return resp.GetRegionError(), nil
		}
		if len(resp.GetErrors()) > 0 {
			// Return first error.
			keyErr := resp.GetErrors()[0]
			if keyErr.GetConflict() != nil {
				return nil, ErrWriteConflict
			}
			if keyErr.GetLocked() != nil {
				// Resolve the conflicting lock, then signal retry.
				if lockInfo := keyErr.GetLocked(); lockInfo.GetLockVersion() > 0 {
					_ = c.client.resolver.ResolveLocks(ctx, []*kvrpcpb.LockInfo{lockInfo})
				}
				return nil, ErrWriteConflict
			}
			return nil, fmt.Errorf("prewrite error: %s", keyErr.String())
		}
		// Handle 1PC success.
		if resp.GetOnePcCommitTs() > 0 {
			c.commitTS = txntypes.TimeStamp(resp.GetOnePcCommitTs())
		}
		return nil, nil
	})
}

// isPrimaryCommitted checks whether the primary key's transaction was actually
// committed. Used after commitPrimary returns an error — the Raft proposal may
// have succeeded but the response was lost (e.g., timeout during leader change).
// Returns (true, nil) if committed, (false, nil) if not committed,
// or (false, error) if the status check itself failed.
// primaryStatus represents the result of checking the primary key's status.
type primaryStatus int

const (
	primaryNotCommitted primaryStatus = iota // definitely not committed (rolled back or not found)
	primaryCommitted                         // definitely committed
	primaryStillLocked                       // lock still present — commit may be in Raft pipeline
)

func (c *twoPhaseCommitter) checkPrimaryStatus(ctx context.Context) (primaryStatus, error) {
	var status primaryStatus
	err := c.client.sender.SendToRegion(ctx, c.primary, func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
		resp, err := client.KvCheckTxnStatus(ctx, &kvrpcpb.CheckTxnStatusRequest{
			Context:    buildContext(info),
			PrimaryKey: c.primary,
			LockTs:     uint64(c.startTS),
		})
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return resp.GetRegionError(), nil
		}
		if resp.GetCommitVersion() != 0 {
			status = primaryCommitted
		} else if resp.GetLockTtl() > 0 {
			status = primaryStillLocked
		} else {
			status = primaryNotCommitted
		}
		return nil, nil
	})
	if err != nil {
		return primaryNotCommitted, err
	}
	return status, nil
}

// commitPrimary commits the primary key synchronously.
func (c *twoPhaseCommitter) commitPrimary(ctx context.Context) error {
	// If 1PC already committed, skip.
	if c.commitTS != 0 && c.opts.Try1PC {
		return nil
	}

	return c.client.sender.SendToRegion(ctx, c.primary, func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
		resp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
			Context:       buildContext(info),
			StartVersion:  uint64(c.startTS),
			Keys:          [][]byte{c.primary},
			CommitVersion: uint64(c.commitTS),
		})
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return resp.GetRegionError(), nil
		}
		if resp.GetError() != nil {
			return nil, fmt.Errorf("commit primary error: %s", resp.GetError().String())
		}
		return nil, nil
	})
}

// commitSecondaries commits all secondary keys grouped by region in parallel.
func (c *twoPhaseCommitter) commitSecondaries(ctx context.Context) {
	// If 1PC, there are no secondaries to commit.
	if c.opts.Try1PC && len(c.mutations) <= 1 {
		return
	}

	var secondaryKeys [][]byte
	for _, m := range c.mutations {
		if string(m.key) != string(c.primary) {
			secondaryKeys = append(secondaryKeys, m.key)
		}
	}
	if len(secondaryKeys) == 0 {
		return
	}

	groups, err := c.client.cache.GroupKeysByRegion(ctx, secondaryKeys)
	if err != nil {
		// Fallback: commit each key individually if grouping fails.
		slog.Warn("commitSecondaries: GroupKeysByRegion failed, falling back to per-key", "err", err)
		c.commitSecondariesPerKey(ctx, secondaryKeys)
		return
	}

	var wg sync.WaitGroup
	for _, group := range groups {
		group := group
		wg.Add(1)
		go func() {
			defer wg.Done()
			lockNotFoundRetries := 0
			err := c.client.sender.SendToRegion(ctx, group.Keys[0], func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
				resp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
					Context:       buildContext(info),
					StartVersion:  uint64(c.startTS),
					Keys:          group.Keys,
					CommitVersion: uint64(c.commitTS),
				})
				if err != nil {
					return nil, err
				}
				if resp.GetRegionError() != nil {
					return resp.GetRegionError(), nil
				}
				if resp.GetError() != nil {
					if resp.GetError().GetTxnLockNotFound() != nil {
						lockNotFoundRetries++
						if lockNotFoundRetries <= 5 {
							return &errorpb.Error{
								Message:   "lock not found, retrying for replication",
								NotLeader: &errorpb.NotLeader{RegionId: info.Region.GetId()},
							}, nil
						}
						// One key's lock was resolved but the server rejected
						// the entire batch. Fall back to per-key commit.
						return nil, fmt.Errorf("batch TxnLockNotFound after retries")
					}
				}
				return nil, nil
			})
			if err != nil {
				slog.Warn("commitSecondaries batch failed, falling back to per-key",
					"regionKeys", len(group.Keys), "startTS", c.startTS, "err", err)
				c.commitSecondariesPerKey(ctx, group.Keys)
			}
		}()
	}
	wg.Wait()
}

// commitSecondariesPerKey commits each key individually as a fallback.
// Used when a batched commit fails (e.g., after a region split causes some
// keys in the batch to no longer belong to the same region).
func (c *twoPhaseCommitter) commitSecondariesPerKey(ctx context.Context, keys [][]byte) {
	var wg sync.WaitGroup
	for _, key := range keys {
		key := key
		wg.Add(1)
		go func() {
			defer wg.Done()
			lockNotFoundRetries := 0
			err := c.client.sender.SendToRegion(ctx, key, func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
				resp, err := client.KvCommit(ctx, &kvrpcpb.CommitRequest{
					Context:       buildContext(info),
					StartVersion:  uint64(c.startTS),
					Keys:          [][]byte{key},
					CommitVersion: uint64(c.commitTS),
				})
				if err != nil {
					return nil, err
				}
				if resp.GetRegionError() != nil {
					return resp.GetRegionError(), nil
				}
				if resp.GetError() != nil {
					if resp.GetError().GetTxnLockNotFound() != nil {
						lockNotFoundRetries++
						if lockNotFoundRetries <= 5 {
							return &errorpb.Error{
								Message:   "lock not found, retrying for replication",
								NotLeader: &errorpb.NotLeader{RegionId: info.Region.GetId()},
							}, nil
						}
						slog.Error("commitSecondary: TxnLockNotFound after retries", "key", key, "startTS", c.startTS)
						return nil, nil
					}
				}
				return nil, nil
			})
			if err != nil {
				slog.Warn("commitSecondary per-key failed", "key", key, "startTS", c.startTS, "err", err)
			}
		}()
	}
	wg.Wait()
}

// rollback rolls back all prewritten/locked keys.
func (c *twoPhaseCommitter) rollback(ctx context.Context) error {
	var keys [][]byte
	for _, m := range c.mutations {
		keys = append(keys, m.key)
	}
	if len(keys) == 0 {
		return nil
	}

	groups, err := c.client.cache.GroupKeysByRegion(ctx, keys)
	if err != nil {
		return err
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, group := range groups {
		group := group
		g.Go(func() error {
			return c.client.sender.SendToRegion(gCtx, group.Keys[0], func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
				resp, err := client.KvBatchRollback(gCtx, &kvrpcpb.BatchRollbackRequest{
					Context:      buildContext(info),
					StartVersion: uint64(c.startTS),
					Keys:         group.Keys,
				})
				if err != nil {
					return nil, err
				}
				if resp.GetRegionError() != nil {
					return resp.GetRegionError(), nil
				}
				if resp.GetError() != nil {
					return nil, fmt.Errorf("rollback error: %s", resp.GetError().String())
				}
				return nil, nil
			})
		})
	}
	return g.Wait()
}

// groupMutationsByRegion groups mutations by their target region.
func (c *twoPhaseCommitter) groupMutationsByRegion(ctx context.Context) (map[uint64][]mutationWithKey, error) {
	keys := make([][]byte, len(c.mutations))
	for i, m := range c.mutations {
		keys[i] = m.key
	}

	regionGroups, err := c.client.cache.GroupKeysByRegion(ctx, keys)
	if err != nil {
		return nil, err
	}

	// Build mutation groups keyed by regionID.
	mutByKey := make(map[string]mutationWithKey, len(c.mutations))
	for _, m := range c.mutations {
		mutByKey[string(m.key)] = m
	}

	result := make(map[uint64][]mutationWithKey, len(regionGroups))
	for regionID, group := range regionGroups {
		muts := make([]mutationWithKey, 0, len(group.Keys))
		for _, k := range group.Keys {
			muts = append(muts, mutByKey[string(k)])
		}
		result[regionID] = muts
	}
	return result, nil
}
