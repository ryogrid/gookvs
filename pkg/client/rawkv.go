package client

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sync"

	"github.com/pingcap/kvproto/pkg/errorpb"
	"github.com/pingcap/kvproto/pkg/kvrpcpb"
	"github.com/pingcap/kvproto/pkg/tikvpb"
	"golang.org/x/sync/errgroup"
)

// KvPair represents a key-value pair.
type KvPair struct {
	Key   []byte
	Value []byte
}

// RawKVClient provides a high-level Raw KV API with automatic region routing.
type RawKVClient struct {
	sender *RegionRequestSender
	cache  *RegionCache
	cf     string
}

// Get retrieves the value for a key.
func (c *RawKVClient) Get(ctx context.Context, key []byte) ([]byte, bool, error) {
	var value []byte
	var notFound bool
	err := c.sender.SendToRegion(ctx, key, func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
		slog.Debug("rawkv.Get", "key", fmt.Sprintf("%x", key))
		resp, err := client.RawGet(ctx, &kvrpcpb.RawGetRequest{
			Context: buildContext(info),
			Key:     key,
			Cf:      c.cf,
		})
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return resp.GetRegionError(), nil
		}
		if resp.GetError() != "" {
			return nil, errFromString(resp.GetError())
		}
		notFound = resp.GetNotFound()
		value = resp.GetValue()
		return nil, nil
	})
	return value, notFound, err
}

// Put stores a key-value pair.
func (c *RawKVClient) Put(ctx context.Context, key, value []byte) error {
	return c.sender.SendToRegion(ctx, key, func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
		slog.Debug("rawkv.Put", "key", fmt.Sprintf("%x", key))
		resp, err := client.RawPut(ctx, &kvrpcpb.RawPutRequest{
			Context: buildContext(info),
			Key:     key,
			Value:   value,
			Cf:      c.cf,
		})
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return resp.GetRegionError(), nil
		}
		if resp.GetError() != "" {
			return nil, errFromString(resp.GetError())
		}
		return nil, nil
	})
}

// PutWithTTL stores a key-value pair with a TTL in seconds.
func (c *RawKVClient) PutWithTTL(ctx context.Context, key, value []byte, ttl uint64) error {
	return c.sender.SendToRegion(ctx, key, func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
		slog.Debug("rawkv.Put", "key", fmt.Sprintf("%x", key), "ttl", ttl)
		resp, err := client.RawPut(ctx, &kvrpcpb.RawPutRequest{
			Context: buildContext(info),
			Key:     key,
			Value:   value,
			Cf:      c.cf,
			Ttl:     ttl,
		})
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return resp.GetRegionError(), nil
		}
		if resp.GetError() != "" {
			return nil, errFromString(resp.GetError())
		}
		return nil, nil
	})
}

// Delete removes a key.
func (c *RawKVClient) Delete(ctx context.Context, key []byte) error {
	return c.sender.SendToRegion(ctx, key, func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
		slog.Debug("rawkv.Delete", "key", fmt.Sprintf("%x", key))
		resp, err := client.RawDelete(ctx, &kvrpcpb.RawDeleteRequest{
			Context: buildContext(info),
			Key:     key,
			Cf:      c.cf,
		})
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return resp.GetRegionError(), nil
		}
		if resp.GetError() != "" {
			return nil, errFromString(resp.GetError())
		}
		return nil, nil
	})
}

// GetKeyTTL returns the remaining TTL for a key.
func (c *RawKVClient) GetKeyTTL(ctx context.Context, key []byte) (uint64, error) {
	var ttl uint64
	err := c.sender.SendToRegion(ctx, key, func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
		slog.Debug("rawkv.GetKeyTTL", "key", fmt.Sprintf("%x", key))
		resp, err := client.RawGetKeyTTL(ctx, &kvrpcpb.RawGetKeyTTLRequest{
			Context: buildContext(info),
			Key:     key,
			Cf:      c.cf,
		})
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return resp.GetRegionError(), nil
		}
		if resp.GetError() != "" {
			return nil, errFromString(resp.GetError())
		}
		ttl = resp.GetTtl()
		return nil, nil
	})
	return ttl, err
}

// BatchGet retrieves values for multiple keys, routing each to its region.
func (c *RawKVClient) BatchGet(ctx context.Context, keys [][]byte) ([]KvPair, error) {
	groups, err := c.cache.GroupKeysByRegion(ctx, keys)
	if err != nil {
		return nil, err
	}

	var mu sync.Mutex
	var allPairs []KvPair
	g, gCtx := errgroup.WithContext(ctx)

	for _, group := range groups {
		group := group
		g.Go(func() error {
			return c.sender.SendToRegion(gCtx, group.Keys[0], func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
				slog.Debug("rawkv.BatchGet", "keys", len(group.Keys))
				resp, err := client.RawBatchGet(gCtx, &kvrpcpb.RawBatchGetRequest{
					Context: buildContext(info),
					Keys:    group.Keys,
					Cf:      c.cf,
				})
				if err != nil {
					return nil, err
				}
				if resp.GetRegionError() != nil {
					return resp.GetRegionError(), nil
				}
				mu.Lock()
				for _, p := range resp.GetPairs() {
					allPairs = append(allPairs, KvPair{Key: p.GetKey(), Value: p.GetValue()})
				}
				mu.Unlock()
				return nil, nil
			})
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}
	return allPairs, nil
}

// BatchPut stores multiple key-value pairs, routing each to its region.
func (c *RawKVClient) BatchPut(ctx context.Context, pairs []KvPair) error {
	// Group keys by region.
	keys := make([][]byte, len(pairs))
	pairMap := make(map[string][]byte, len(pairs))
	for i, p := range pairs {
		keys[i] = p.Key
		pairMap[string(p.Key)] = p.Value
	}

	groups, err := c.cache.GroupKeysByRegion(ctx, keys)
	if err != nil {
		return err
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, group := range groups {
		group := group
		g.Go(func() error {
			kvPairs := make([]*kvrpcpb.KvPair, len(group.Keys))
			for i, k := range group.Keys {
				kvPairs[i] = &kvrpcpb.KvPair{Key: k, Value: pairMap[string(k)]}
			}
			return c.sender.SendToRegion(gCtx, group.Keys[0], func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
				slog.Debug("rawkv.BatchPut", "pairs", len(kvPairs))
				resp, err := client.RawBatchPut(gCtx, &kvrpcpb.RawBatchPutRequest{
					Context: buildContext(info),
					Pairs:   kvPairs,
					Cf:      c.cf,
				})
				if err != nil {
					return nil, err
				}
				if resp.GetRegionError() != nil {
					return resp.GetRegionError(), nil
				}
				if resp.GetError() != "" {
					return nil, errFromString(resp.GetError())
				}
				return nil, nil
			})
		})
	}
	return g.Wait()
}

// BatchDelete removes multiple keys, routing each to its region.
func (c *RawKVClient) BatchDelete(ctx context.Context, keys [][]byte) error {
	groups, err := c.cache.GroupKeysByRegion(ctx, keys)
	if err != nil {
		return err
	}

	g, gCtx := errgroup.WithContext(ctx)
	for _, group := range groups {
		group := group
		g.Go(func() error {
			return c.sender.SendToRegion(gCtx, group.Keys[0], func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
				slog.Debug("rawkv.BatchDelete", "keys", len(group.Keys))
				resp, err := client.RawBatchDelete(gCtx, &kvrpcpb.RawBatchDeleteRequest{
					Context: buildContext(info),
					Keys:    group.Keys,
					Cf:      c.cf,
				})
				if err != nil {
					return nil, err
				}
				if resp.GetRegionError() != nil {
					return resp.GetRegionError(), nil
				}
				if resp.GetError() != "" {
					return nil, errFromString(resp.GetError())
				}
				return nil, nil
			})
		})
	}
	return g.Wait()
}

// Scan retrieves key-value pairs in a range, transparently crossing region boundaries.
func (c *RawKVClient) Scan(ctx context.Context, startKey, endKey []byte, limit int) ([]KvPair, error) {
	var result []KvPair
	currentKey := startKey

	for {
		info, err := c.cache.LocateKey(ctx, currentKey)
		if err != nil {
			return nil, err
		}

		regionEnd := info.Region.GetEndKey()
		scanEnd := endKey
		if len(regionEnd) > 0 && (len(scanEnd) == 0 || bytes.Compare(regionEnd, scanEnd) < 0) {
			scanEnd = regionEnd
		}

		remaining := limit - len(result)
		if remaining <= 0 {
			break
		}

		var pairs []KvPair
		slog.Debug("rawkv.Scan", "start", fmt.Sprintf("%x", currentKey), "limit", remaining)
		err = c.sender.SendToRegion(ctx, currentKey, func(client tikvpb.TikvClient, rInfo *RegionInfo) (*errorpb.Error, error) {
			resp, err := client.RawScan(ctx, &kvrpcpb.RawScanRequest{
				Context:  buildContext(rInfo),
				StartKey: currentKey,
				EndKey:   scanEnd,
				Limit:    uint32(remaining),
				Cf:       c.cf,
			})
			if err != nil {
				return nil, err
			}
			if resp.GetRegionError() != nil {
				return resp.GetRegionError(), nil
			}
			for _, kv := range resp.GetKvs() {
				pairs = append(pairs, KvPair{Key: kv.GetKey(), Value: kv.GetValue()})
			}
			return nil, nil
		})
		if err != nil {
			return nil, err
		}

		result = append(result, pairs...)

		if len(result) >= limit {
			break
		}
		if len(regionEnd) == 0 {
			break // last region
		}
		if len(endKey) > 0 && bytes.Compare(regionEnd, endKey) >= 0 {
			break
		}
		currentKey = regionEnd
	}

	if len(result) > limit {
		result = result[:limit]
	}
	return result, nil
}

// DeleteRange deletes all keys in a range, spanning region boundaries.
func (c *RawKVClient) DeleteRange(ctx context.Context, startKey, endKey []byte) error {
	currentKey := startKey
	for {
		info, err := c.cache.LocateKey(ctx, currentKey)
		if err != nil {
			return err
		}

		regionEnd := info.Region.GetEndKey()
		rangeEnd := endKey
		if len(regionEnd) > 0 && (len(rangeEnd) == 0 || bytes.Compare(regionEnd, rangeEnd) < 0) {
			rangeEnd = regionEnd
		}

		err = c.sender.SendToRegion(ctx, currentKey, func(client tikvpb.TikvClient, rInfo *RegionInfo) (*errorpb.Error, error) {
			slog.Debug("rawkv.DeleteRange", "start", fmt.Sprintf("%x", currentKey), "end", fmt.Sprintf("%x", rangeEnd))
			resp, err := client.RawDeleteRange(ctx, &kvrpcpb.RawDeleteRangeRequest{
				Context:  buildContext(rInfo),
				StartKey: currentKey,
				EndKey:   rangeEnd,
				Cf:       c.cf,
			})
			if err != nil {
				return nil, err
			}
			if resp.GetRegionError() != nil {
				return resp.GetRegionError(), nil
			}
			if resp.GetError() != "" {
				return nil, errFromString(resp.GetError())
			}
			return nil, nil
		})
		if err != nil {
			return err
		}

		if len(regionEnd) == 0 {
			break
		}
		if len(endKey) > 0 && bytes.Compare(regionEnd, endKey) >= 0 {
			break
		}
		currentKey = regionEnd
	}
	return nil
}

// CompareAndSwap atomically compares and swaps a value.
func (c *RawKVClient) CompareAndSwap(ctx context.Context, key, value, prevValue []byte, prevNotExist bool) (bool, []byte, error) {
	var succeed bool
	var previousValue []byte
	err := c.sender.SendToRegion(ctx, key, func(client tikvpb.TikvClient, info *RegionInfo) (*errorpb.Error, error) {
		slog.Debug("rawkv.CompareAndSwap", "key", fmt.Sprintf("%x", key))
		resp, err := client.RawCompareAndSwap(ctx, &kvrpcpb.RawCASRequest{
			Context:          buildContext(info),
			Key:              key,
			Value:            value,
			PreviousValue:    prevValue,
			PreviousNotExist: prevNotExist,
			Cf:               c.cf,
		})
		if err != nil {
			return nil, err
		}
		if resp.GetRegionError() != nil {
			return resp.GetRegionError(), nil
		}
		if resp.GetError() != "" {
			return nil, errFromString(resp.GetError())
		}
		succeed = resp.GetSucceed()
		previousValue = resp.GetPreviousValue()
		return nil, nil
	})
	return succeed, previousValue, err
}

// Checksum computes a checksum over a key range, spanning region boundaries.
func (c *RawKVClient) Checksum(ctx context.Context, startKey, endKey []byte) (uint64, uint64, uint64, error) {
	var checksum, totalKvs, totalBytes uint64
	currentKey := startKey

	for {
		info, err := c.cache.LocateKey(ctx, currentKey)
		if err != nil {
			return 0, 0, 0, err
		}

		regionEnd := info.Region.GetEndKey()
		rangeEnd := endKey
		if len(regionEnd) > 0 && (len(rangeEnd) == 0 || bytes.Compare(regionEnd, rangeEnd) < 0) {
			rangeEnd = regionEnd
		}

		err = c.sender.SendToRegion(ctx, currentKey, func(client tikvpb.TikvClient, rInfo *RegionInfo) (*errorpb.Error, error) {
			slog.Debug("rawkv.Checksum", "start", fmt.Sprintf("%x", currentKey), "end", fmt.Sprintf("%x", rangeEnd))
			resp, err := client.RawChecksum(ctx, &kvrpcpb.RawChecksumRequest{
				Context: buildContext(rInfo),
				Ranges: []*kvrpcpb.KeyRange{
					{StartKey: currentKey, EndKey: rangeEnd},
				},
			})
			if err != nil {
				return nil, err
			}
			if resp.GetRegionError() != nil {
				return resp.GetRegionError(), nil
			}
			if resp.GetError() != "" {
				return nil, errFromString(resp.GetError())
			}
			checksum ^= resp.GetChecksum()
			totalKvs += resp.GetTotalKvs()
			totalBytes += resp.GetTotalBytes()
			return nil, nil
		})
		if err != nil {
			return 0, 0, 0, err
		}

		if len(regionEnd) == 0 {
			break
		}
		if len(endKey) > 0 && bytes.Compare(regionEnd, endKey) >= 0 {
			break
		}
		currentKey = regionEnd
	}
	return checksum, totalKvs, totalBytes, nil
}

// Close is a no-op for RawKVClient; the parent Client manages lifecycle.
func (c *RawKVClient) Close() error {
	return nil
}

// errFromString wraps a string error.
func errFromString(s string) error {
	if s == "" {
		return nil
	}
	return &stringError{s}
}

type stringError struct {
	msg string
}

func (e *stringError) Error() string { return e.msg }
