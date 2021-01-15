package snapmeta

import (
	"sync"

	"github.com/pkg/errors"
)

// ErrKeyNotFound is returned when the store can't find the key provided.
var ErrKeyNotFound = errors.New("key not found")

var _ Store = &Simple{}

// Simple is a snapstore implementation that stores
// snapshot metadata as a byte slice in a map in memory.
// A Simple should not be copied.
type Simple struct {
	Data map[string][]byte `json:"data"`
	Idx  Index             `json:"idx"`
	mu   sync.Mutex
}

// NewSimple instantiates a new Simple snapstore and
// returns its pointer.
func NewSimple() *Simple {
	return &Simple{
		Data: make(map[string][]byte),
		Idx:  Index(make(map[string]map[string]struct{})),
	}
}

// IndexOperation defines several operations implemented on Index.
type IndexOperation string

const (
	// AddToIndexOperation add value in Index.
	AddToIndexOperation IndexOperation = "addToIndex"
	// RemoveFromIndexOperation removes value from Index.
	RemoveFromIndexOperation IndexOperation = "removeFromIndex"
)

// Store implements the Storer interface Store method.
func (s *Simple) Store(key string, val []byte, indexUpdates map[string]IndexOperation) error {
	buf := make([]byte, len(val))
	_ = copy(buf, val)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.Data[key] = buf

	s.processIndexUpdates(key, indexUpdates)

	return nil
}

func (s *Simple) processIndexUpdates(key string, indexUpdates map[string]IndexOperation) {
	for indexName, indexOp := range indexUpdates {
		switch indexOp {
		case AddToIndexOperation:
			s.Idx.AddToIndex(key, indexName)
		case RemoveFromIndexOperation:
			s.Idx.RemoveFromIndex(key, indexName)
		}
	}
}

// Load implements the Storer interface Load method.
func (s *Simple) Load(key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if buf, found := s.Data[key]; found {
		retBuf := make([]byte, len(buf))
		_ = copy(retBuf, buf)

		return retBuf, nil
	}

	return nil, ErrKeyNotFound
}

// Delete implements the Storer interface Delete method.
func (s *Simple) Delete(key string, indexUpdates map[string]IndexOperation) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.Data, key)

	s.processIndexUpdates(key, indexUpdates)
}

// GetKeys implements the Indexer interface GetKeys method.
func (s *Simple) GetKeys(indexName string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.Idx.GetKeys(indexName)
}
