package snapstore

import "github.com/kopia/kopia/tests/robustness/snapif"

type Storer interface {
	Store(key string, val []byte) error
	Load(key string) ([]byte, error)
	Delete(key string)
	GetKeys() []string
}

type DataPersister interface {
	snapif.RepoManager
	LoadMetadata() error
	FlushMetadata() error
}
