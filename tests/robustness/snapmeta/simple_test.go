package snapmeta

import (
	"reflect"
	"testing"
)

func TestSimpleWithIndex(t *testing.T) {
	simple := NewSimple()

	storeKey := "key-to-store"
	data := []byte("some stored data")
	simple.Store(storeKey, data, nil)

	checkStoredData, err := simple.Load(storeKey)
	if err != nil {
		t.Fatalf("expected %v keys but got %v", nil, err)
	}

	if !reflect.DeepEqual(checkStoredData, data) {
		t.Fatalf("expected datas %v but got %v", data, checkStoredData)
	}

	idxName := "index-name"
	indexUpdates := map[string]IndexOperation{
		idxName: AddToIndexOperation,
	}

	simple.Store(storeKey, nil, indexUpdates)

	checkStoredData, err = simple.Load(storeKey)
	if err != nil {
		t.Fatalf("expected error %v but got %v", nil, err)
	}

	if !reflect.DeepEqual(checkStoredData, []byte{}) {
		t.Fatalf("expected data %v but got %v", data, checkStoredData)
	}

	idxKeys := simple.GetKeys(idxName)
	if got, want := len(idxKeys), 1; got != want {
		t.Fatalf("expected %v keys but got %v", want, got)
	}

	if got, want := idxKeys[0], storeKey; got != want {
		t.Fatalf("expected key %v but got %v", want, got)
	}
}
