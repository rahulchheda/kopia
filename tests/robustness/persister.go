package robustness

// IndexOperation defines several operations implemented on indices associated with the Store.
type IndexOperation string

const (
	// AddToIndexOperation adds value to an index.
	AddToIndexOperation IndexOperation = "addToIndex"
	// RemoveFromIndexOperation removes value from an index.
	RemoveFromIndexOperation IndexOperation = "removeFromIndex"
)

// Store describes the ability to store and retrieve
// a buffer of metadata, indexed by a string key.
// Updates to multiple search indices can be performed with
// Store and Delete operations if desired.
type Store interface {
	Store(key string, val []byte, indexUpdates map[string]IndexOperation) error
	Load(key string) ([]byte, error)
	Delete(key string, indexUpdates map[string]IndexOperation)
	GetKeys(indexName string) []string
}

// Persister describes the ability to flush metadata
// to, and load it again, from a repository.
type Persister interface {
	Store
	RepoManager // TBD: may not be needed once initialization refactored
	LoadMetadata() error
	FlushMetadata() error
	GetPersistDir() string
	Cleanup()
}
