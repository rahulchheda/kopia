package robustness

// Store describes the ability to store and retrieve
// a buffer of metadata, indexed by a string key.
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
	LoadMetadata() error
	FlushMetadata() error
	GetPersistDir() string
}

// IndexOperation defines several operations implemented on Index.
type IndexOperation string

const (
	// AddToIndexOperation add value in Index.
	AddToIndexOperation IndexOperation = "addToIndex"
	// RemoveFromIndexOperation removes value from Index.
	RemoveFromIndexOperation IndexOperation = "removeFromIndex"
)
