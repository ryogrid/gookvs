// Package cfnames defines column family name constants for gookvs.
// These names are byte-identical to TiKV's for storage compatibility.
package cfnames

const (
	// CFDefault is the default column family for large values.
	CFDefault = "default"
	// CFLock is the column family for active transaction locks.
	CFLock = "lock"
	// CFWrite is the column family for commit metadata.
	CFWrite = "write"
	// CFRaft is the column family for Raft state.
	CFRaft = "raft"
)

// DataCFs are the column families used for transaction data.
var DataCFs = []string{CFDefault, CFLock, CFWrite}

// AllCFs are all column families used by gookvs.
var AllCFs = []string{CFDefault, CFLock, CFWrite, CFRaft}
