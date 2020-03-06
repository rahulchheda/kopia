package snapmeta

import (
	"errors"
	"sync"
)

// ErrKeyNotFound is returned when the store can't find the key provided
var ErrKeyNotFound = errors.New("key not found")

var _ Store = &Simple{}

// Simple is a snapstore implementation that stores
// snapshot metadata as a byte slice in a map in memory
type Simple struct {
	Data map[string][]byte
	Idx  Index
	l    sync.Mutex
}

// NewSimple instantiates a new Simple snapstore and
// returns its pointer
func NewSimple() *Simple {
	return &Simple{
		Data: make(map[string][]byte),
	}
}

// Store implements the Storer interface Store method
func (s *Simple) Store(key string, val []byte) error {
	buf := make([]byte, len(val))
	_ = copy(buf, val)

	s.l.Lock()
	defer s.l.Unlock()

	s.Data[key] = buf

	return nil
}

// Load implements the Storer interface Load method
func (s *Simple) Load(key string) ([]byte, error) {
	s.l.Lock()
	defer s.l.Unlock()

	if buf, found := s.Data[key]; found {
		retBuf := make([]byte, len(buf))
		_ = copy(retBuf, buf)

		return retBuf, nil
	}

	return nil, ErrKeyNotFound
}

// Delete implements the Storer interface Delete method
func (s *Simple) Delete(key string) {
	s.l.Lock()
	defer s.l.Unlock()

	delete(s.Data, key)
}

// AddToIndex implements the Storer interface AddToIndex method
func (s *Simple) AddToIndex(key, indexName string) {
	s.l.Lock()
	defer s.l.Unlock()

	s.Idx.AddToIndex(key, indexName)
}

// RemoveFromIndex implements the Indexer interface RemoveFromIndex method
func (s *Simple) RemoveFromIndex(key, indexName string) {
	s.l.Lock()
	defer s.l.Unlock()

	s.Idx.RemoveFromIndex(key, indexName)
}

// GetKeys implements the Indexer interface GetKeys method
func (s *Simple) GetKeys(indexName string) []string {
	s.l.Lock()
	defer s.l.Unlock()

	return s.Idx.GetKeys(indexName)
}
