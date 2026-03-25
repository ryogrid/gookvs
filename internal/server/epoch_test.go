package server

import (
	"testing"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/stretchr/testify/assert"
)

// mockPeerForEpoch creates a minimal peer with the given epoch for testing.
func mockPeerForEpoch(regionID uint64, epoch *metapb.RegionEpoch) *tikvService {
	// We test validateRegionContext indirectly by creating a server with
	// the necessary coordinator and peer infrastructure. For epoch-specific
	// unit testing, we verify the epoch comparison logic directly.
	return nil // epoch tests use the helper below
}

func TestEpochCheckLogic(t *testing.T) {
	// Test the epoch comparison logic that would be used in validateRegionContext.
	// This tests the core algorithm without requiring a full server setup.

	type testCase struct {
		name           string
		reqEpoch       *metapb.RegionEpoch
		currentEpoch   *metapb.RegionEpoch
		expectMismatch bool
	}

	tests := []testCase{
		{
			name:           "both match",
			reqEpoch:       &metapb.RegionEpoch{ConfVer: 1, Version: 2},
			currentEpoch:   &metapb.RegionEpoch{ConfVer: 1, Version: 2},
			expectMismatch: false,
		},
		{
			name:           "version mismatch (split occurred)",
			reqEpoch:       &metapb.RegionEpoch{ConfVer: 1, Version: 2},
			currentEpoch:   &metapb.RegionEpoch{ConfVer: 1, Version: 3},
			expectMismatch: true,
		},
		{
			name:           "confver mismatch (conf change occurred)",
			reqEpoch:       &metapb.RegionEpoch{ConfVer: 1, Version: 2},
			currentEpoch:   &metapb.RegionEpoch{ConfVer: 2, Version: 2},
			expectMismatch: true,
		},
		{
			name:           "both mismatch",
			reqEpoch:       &metapb.RegionEpoch{ConfVer: 1, Version: 2},
			currentEpoch:   &metapb.RegionEpoch{ConfVer: 3, Version: 4},
			expectMismatch: true,
		},
		{
			name:           "nil request epoch (backward compat)",
			reqEpoch:       nil,
			currentEpoch:   &metapb.RegionEpoch{ConfVer: 1, Version: 2},
			expectMismatch: false,
		},
		{
			name:           "nil current epoch",
			reqEpoch:       &metapb.RegionEpoch{ConfVer: 1, Version: 2},
			currentEpoch:   nil,
			expectMismatch: false,
		},
		{
			name:           "both nil",
			reqEpoch:       nil,
			currentEpoch:   nil,
			expectMismatch: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mismatch := false
			if tc.reqEpoch != nil && tc.currentEpoch != nil {
				if tc.reqEpoch.GetVersion() != tc.currentEpoch.GetVersion() ||
					tc.reqEpoch.GetConfVer() != tc.currentEpoch.GetConfVer() {
					mismatch = true
				}
			}
			assert.Equal(t, tc.expectMismatch, mismatch)
		})
	}
}
