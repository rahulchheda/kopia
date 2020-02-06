package snapstore

import "github.com/kopia/kopia/tests/robustness/snapif"

// Storer describes the ability to store and retrieve
// a buffer of metadata, indexed by a string key.
type Storer interface {
	Store(key string, val []byte) error
	Load(key string) ([]byte, error)
	Delete(key string)
	GetKeys() []string
}

// DataPersister describes the ability to flush metadata
// to, and load it again, from a repository.
type DataPersister interface {
	snapif.RepoManager
	LoadMetadata() error
	FlushMetadata() error
}
