package client

import (
	"context"
	"fmt"
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
	primary      []byte
	opts         TxnOptions
	prewriteDone bool
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
		_ = c.rollback(context.Background())
		return err
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
				return nil, fmt.Errorf("key locked during prewrite: %s", keyErr.String())
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

// commitSecondaries commits all secondary keys in parallel (best-effort).
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
		return
	}

	var wg sync.WaitGroup
	for _, group := range groups {
		group := group
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.client.sender.SendToRegion(ctx, group.Keys[0], func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
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
				return nil, nil
			})
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
