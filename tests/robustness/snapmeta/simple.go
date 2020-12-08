package snapmeta

import (
	"errors"
	"sync"
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
		Idx: Index{
			index: (make(map[string]map[string]struct{})),
		},
	}
}

type Operation string

const (
	StoreOperation           Operation = "store"
	DeleteOperation          Operation = "delete"
	AddToIndexOperation      Operation = "addToIndex"
	RemoveFromIndexOperation Operation = "removeFromIndex"
)

type OperationEntry struct {
	Operation Operation
	Key       string
	Data      interface{}
}

// Store implements the Storer interface Store method.
func (s *Simple) Store(key string, val []byte) error {
	buf := make([]byte, len(val))
	_ = copy(buf, val)

	s.Data[key] = buf

	return nil
}

// Load implements the Storer interface Load method.
func (s *Simple) Load(key string) ([]byte, error) {
	if buf, found := s.Data[key]; found {
		retBuf := make([]byte, len(buf))
		_ = copy(retBuf, buf)

		return retBuf, nil
	}

	return nil, ErrKeyNotFound
}

// Delete implements the Storer interface Delete method.
func (s *Simple) Delete(key string) {
	delete(s.Data, key)
}

// AddToIndex implements the Storer interface AddToIndex method
func (s *Simple) AddToIndex(key, indexName string) {
	s.Idx.AddToIndex(key, indexName)
}

// RemoveFromIndex implements the Indexer interface RemoveFromIndex method.
func (s *Simple) RemoveFromIndex(key, indexName string) {
	s.Idx.RemoveFromIndex(key, indexName)
}

// GetKeys implements the Indexer interface GetKeys method.
func (s *Simple) GetKeys(indexName string) []string {
	return s.Idx.GetKeys(indexName)
}

// IndexOperation implements the Indexer interface IndexOperation method
// To add a particular indexKey use true, and to remove use false
func (s *Simple) IndexOperation(operationEntrys ...OperationEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, op := range operationEntrys {
		switch op.Operation {

		case StoreOperation:
			// StoreOperation value is []byte
			storeValue, ok := op.Data.([]byte)
			if !ok {
				return errors.New("Unknown value for Store Operation")
			}
			s.Store(op.Key, storeValue)
		case DeleteOperation:
			//DeleteOperation value is string
			s.Delete(op.Key)
		case AddToIndexOperation:
			// AddToIndexOperation value is string
			addToIndexValue, ok := op.Data.(string)
			if !ok {
				return errors.New("Unknown value for AddToIndex Operation")
			}
			s.AddToIndex(op.Key, addToIndexValue)
		case RemoveFromIndexOperation:
			// RemoveToIndexOperation value is string
			removeFromIndexValue, ok := op.Data.(string)
			if !ok {
				return errors.New("Unknown value for Remove From Index Operation")
			}
			s.RemoveFromIndex(op.Key, removeFromIndexValue)
		}
	}

	return nil
}
