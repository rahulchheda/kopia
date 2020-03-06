package snapmeta

type Index map[string]map[string]struct{}

func (idx Index) AddToIndex(key, indexName string) {
	if _, ok := idx[indexName]; !ok {
		idx[indexName] = make(map[string]struct{})
	}

	idx[indexName][key] = struct{}{}
}

func (idx Index) RemoveFromIndex(key, indexName string) {
	if _, ok := idx[indexName]; !ok {
		return
	}

	delete(idx[indexName], key)
}

func (idx Index) GetKeys(indexName string) (ret []string) {
	if _, ok := idx[indexName]; !ok {
		return ret
	}

	for k := range idx[indexName] {
		ret = append(ret, k)
	}

	return ret
}
