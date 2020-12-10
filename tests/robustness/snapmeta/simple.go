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

// Operation defines several operations implemented on Index.
type Operation string

const (
	// GetKeysOperation returns the list of keys associated with the given index name.
	GetKeysOperation Operation = "getkeys"
	// LoadOperation implements the Storer interface Load method.
	LoadOperation Operation = "load"
	// StoreOperation stores the value in Index.
	StoreOperation Operation = "store"
	// DeleteOperation deletes value from Index.
	DeleteOperation Operation = "delete"
	// AddToIndexOperation add value in Index.
	AddToIndexOperation Operation = "addToIndex"
	// RemoveFromIndexOperation removes value from Index.
	RemoveFromIndexOperation Operation = "removeFromIndex"
)

var (
	errStoreOperation           error = errors.New("Unknown value for Store Operation")
	errAddToIndexOperation      error = errors.New("Unknown value for AddToIndex Operation")
	errRemoveFromIndexOperation error = errors.New("Unknown value for Remove From Index Operation")
)

// OperationEntry defines operations on Index.
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

// AddToIndex implements the Storer interface AddToIndex method.
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
// to add a particular indexKey use true, and to remove use false.
func (s *Simple) IndexOperation(operationEntrys ...OperationEntry) (interface{}, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, op := range operationEntrys {
		switch op.Operation {
		case GetKeysOperation:
			indexName := op.Data.(string)

			return s.Idx.GetKeys(indexName), nil

		case LoadOperation:
			return s.Load(op.Key)

		case StoreOperation:
			// StoreOperation value is []byte
			storeValue, ok := op.Data.([]byte)

			if !ok {
				return nil, errStoreOperation
			}

			if err := s.Store(op.Key, storeValue); err != nil {
				return nil, err
			}

		case DeleteOperation:
			// DeleteOperation value is string
			s.Delete(op.Key)
		case AddToIndexOperation:
			// AddToIndexOperation value is string
			addToIndexValue, ok := op.Data.(string)
			if !ok {
				return nil, errAddToIndexOperation
			}

			s.Idx.AddToIndex(op.Key, addToIndexValue)

		case RemoveFromIndexOperation:
			// RemoveToIndexOperation value is string
			removeFromIndexValue, ok := op.Data.(string)
			if !ok {
				return nil, errRemoveFromIndexOperation
			}

			s.Idx.RemoveFromIndex(op.Key, removeFromIndexValue)
		}
	}

	return nil, nil
}
