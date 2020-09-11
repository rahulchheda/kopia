package snapmeta

import "sync"

type Index struct {
	index map[string]map[string]struct{}
	mux   sync.Mutex
}

// AddToIndex adds a key to the index of the given name.
func (idx Index) AddToIndex(key, indexName string) {
	if _, ok := idx.index[indexName]; !ok {
		idx.index[indexName] = make(map[string]struct{})
	}

	idx.index[indexName][key] = struct{}{}
}

// RemoveFromIndex removes a key from the index of the given name.
func (idx Index) RemoveFromIndex(key, indexName string) {
	if _, ok := idx.index[indexName]; !ok {
		return
	}

	delete(idx.index[indexName], key)
}

// GetKeys returns the list of keys associated with the given index name.
func (idx Index) GetKeys(indexName string) (ret []string) {
	if _, ok := idx.index[indexName]; !ok {
		return ret
	}

	for k := range idx.index[indexName] {
		ret = append(ret, k)
	}

	return ret
}

// To add a particulat index Key use true, and to remove use false
func (idx Index) IndexOperation(key string, indexMap map[string]bool) {
	idx.mux.Lock()
	defer idx.mux.Unlock()

	for indexKey, op := range indexMap {
		if op {
			idx.AddToIndex(key, indexKey)
		} else {
			idx.RemoveFromIndex(key, indexKey)
		}
	}
}
