package txntypes

// MutationOp represents the operation type of a client-submitted mutation.
type MutationOp byte

const (
	MutationPut            MutationOp = iota // Write a value
	MutationDelete                           // Delete a key
	MutationLock                             // Acquire read lock (no data change)
	MutationInsert                           // Put with key-not-exists constraint
	MutationCheckNotExists                   // Assert key does not exist
)

// Assertion represents a constraint on a key's existence.
type Assertion byte

const (
	AssertionNone     Assertion = iota
	AssertionExist              // Key must exist
	AssertionNotExist           // Key must not exist
)

// Mutation represents a client-submitted key mutation.
type Mutation struct {
	Op        MutationOp
	Key       []byte
	Value     []byte // nil for Delete/Lock/CheckNotExists
	Assertion Assertion
}
