package snapmeta

import (
	"testing"
)

func TestSimpleWithIndex(t *testing.T) {
	simple := NewSimple()

	storeKey := "key-to-store"
	data := []byte("some stored data")
	simple.Store(storeKey, data, nil)

	idxName := "index-name"
	indexUpdates := map[string]IndexOperation{
		idxName: AddToIndexOperation,
	}
	simple.Store(storeKey, nil, indexUpdates)

	idxKeys := simple.GetKeys(idxName)
	if got, want := len(idxKeys), 1; got != want {
		t.Fatalf("expected %v keys but got %v", want, got)
	}

	if got, want := idxKeys[0], storeKey; got != want {
		t.Fatalf("expected key %v but got %v", want, got)
	}
}
