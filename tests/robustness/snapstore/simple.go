package snapstore

import "sync"

type Simple struct {
	s map[string][]byte
	l *sync.Mutex
}

func NewSimple() *Simple {
	return &Simple{
		s: make(map[string][]byte),
		l: new(sync.Mutex),
	}
}

func (store *Simple) Store(key string, val []byte) error {
	buf := make([]byte, len(val), len(val))
	_ = copy(buf, val)
	store.l.Lock()
	store.s[key] = buf
	store.l.Unlock()
	return nil
}

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
