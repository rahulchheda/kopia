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

// Store implements the Storer interface Store method.
func (s *Simple) Store(key string, val []byte) error {
	buf := make([]byte, len(val))
	_ = copy(buf, val)

	s.mu.Lock()
	defer s.mu.Unlock()

	s.Data[key] = buf

	return nil
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
func (s *Simple) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.Data, key)
}

// AddToIndex implements the Storer interface AddToIndex method
func (s *Simple) AddToIndex(key, indexName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Idx.AddToIndex(key, indexName)
}

// RemoveFromIndex implements the Indexer interface RemoveFromIndex method
func (s *Simple) RemoveFromIndex(key, indexName string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.Idx.RemoveFromIndex(key, indexName)
}

// GetKeys implements the Indexer interface GetKeys method
func (s *Simple) GetKeys(indexName string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.Idx.GetKeys(indexName)
}

// IndexOperation implements the Indexer interface IndexOperation method
// To add a particular indexKey use true, and to remove use false
func (s *Simple) IndexOperation(key string, indexMap map[Operation]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	for indexOperation, value := range indexMap {
		switch indexOperation {
		case StoreOperation:
			// StoreOperation value is map[string][]byte
			storeValue, ok := value.(map[string][]byte)
			if !ok {
				return errors.New("Unknown value for Store Operation")
			}
			for i, v := range storeValue {
				if err := s.Store(i, v); err != nil {
					return err
				}
			}

		case DeleteOperation:
			// DeleteOperation value is []string
			deleteValue, ok := value.([]string)
			if !ok {
				return errors.New("Unknown value for Delete Operation")
			}
			for _, v := range deleteValue {
				s.Delete(v)
			}

		case AddToIndexOperation:
			// AddToIndexOperation value is map[string]string
			addToIndexValue, ok := value.(map[string]string)
			if !ok {
				return errors.New("Unknown value for AddToIndex Operation")
			}
			for i, v := range addToIndexValue {
				s.AddToIndex(i, v)
			}

		case RemoveFromIndexOperation:
			// RemoveToIndexOperation value is map[string]string
			removeFromIndexValue, ok := value.(map[string]string)
			if !ok {
				return errors.New("Unknown value for Remove From Index Operation")
			}
			for i, v := range removeFromIndexValue {
				s.RemoveFromIndex(i, v)
			}
		}

	}

	return nil
}
