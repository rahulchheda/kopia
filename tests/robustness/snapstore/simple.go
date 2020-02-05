package snapstore

import "sync"

var _ Storer = &Simple{}

// Simple is a snapstore implementation that stores
// snapshot metadata as a byte slice in a map in memory
type Simple struct {
	s map[string][]byte
	l *sync.Mutex
}

// NewSimple instantiates a new Simple snapstore and
// returns its pointer
func NewSimple() *Simple {
	return &Simple{
		s: make(map[string][]byte),
		l: new(sync.Mutex),
	}
}

// Store implements the Storer interface Store method
func (store *Simple) Store(key string, val []byte) error {
	buf := make([]byte, len(val), len(val))
	_ = copy(buf, val)
	store.l.Lock()
	store.s[key] = buf
	store.l.Unlock()
	return nil
}

// Load implements the Storer interface Load method
func (store *Simple) Load(key string) ([]byte, error) {
	store.l.Lock()
	defer store.l.Unlock()
	if buf, found := store.s[key]; found {
		retBuf := make([]byte, len(buf), len(buf))
		_ = copy(retBuf, buf)
		return retBuf, nil
	}
	return nil, nil
}

// Delete implements the Storer interface Delete method
func (store *Simple) Delete(key string) {
	store.l.Lock()
	defer store.l.Unlock()
	delete(store.s, key)
}

// GetKeys implements the Storer interface GetKeys method
func (store *Simple) GetKeys() []string {
	ret := make([]string, 0, len(store.s))
	for k := range store.s {
		ret = append(ret, k)
	}
	return ret
}
