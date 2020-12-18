package snapmeta

import (
	"reflect"
	"testing"
)

func TestSimple(t *testing.T) { // nolint: gocyclo
	storeKey := "key-to-store"
	data := []byte("some stored data")
	idxName := "index-name"

	type storeOperation struct {
		todo         bool
		key          string
		val          []byte
		indexUpdates map[string]IndexOperation
	}

	type deleteOperation struct {
		todo         bool
		key          string
		indexUpdates map[string]IndexOperation
	}

	type testcase struct {
		storeOperations  storeOperation
		deleteOperations deleteOperation
	}

	testcases := []testcase{
		{
			storeOperations: storeOperation{
				todo:         true,
				key:          storeKey,
				val:          []byte("some stored data"),
				indexUpdates: nil,
			},
			deleteOperations: deleteOperation{
				todo: false,
			},
		},
		{
			storeOperations: storeOperation{
				todo: true,
				key:  storeKey,
				val:  []byte("some stored data"),
				indexUpdates: map[string]IndexOperation{
					idxName: AddToIndexOperation,
				},
			},
			deleteOperations: deleteOperation{
				todo: false,
			},
		},
		{
			storeOperations: storeOperation{
				todo: true,
				key:  storeKey,
				val:  []byte("some stored data"),
				indexUpdates: map[string]IndexOperation{
					idxName: AddToIndexOperation,
				},
			},
			deleteOperations: deleteOperation{
				todo: true,
				key:  storeKey,
				indexUpdates: map[string]IndexOperation{
					idxName: RemoveFromIndexOperation,
				},
			},
		},
	}

	for _, v := range testcases {
		simple := NewSimple()

		if v.storeOperations.todo == true {
			simple.Store(v.storeOperations.key, v.storeOperations.val, v.storeOperations.indexUpdates)
		}

		// Check if the value is loaded correctly
		checkStoredData, err := simple.Load(v.storeOperations.key)
		if err != nil {
			t.Fatalf("expected %v keys but got %v", nil, err)
		}

		if !reflect.DeepEqual(checkStoredData, data) {
			t.Fatalf("expected datas %v but got %v", data, checkStoredData)
		}

		if v.storeOperations.indexUpdates != nil {
			for idx := range v.storeOperations.indexUpdates {
				idxKeys := simple.GetKeys(idx)
				if got, want := len(idxKeys), 1; got != want {
					t.Fatalf("expected %v keys but got %v", want, got)
				}

				if got, want := idxKeys[0], v.storeOperations.key; got != want {
					t.Fatalf("expected key %v but got %v", want, got)
				}
			}
		}

		if v.deleteOperations.todo == true {
			simple.Delete(v.storeOperations.key, v.storeOperations.indexUpdates)
		}

		// Check if the value is deleted correctly
		_, err = simple.Load(v.deleteOperations.key)
		if err == nil {
			t.Fatalf("expected %v keys but got %v", nil, err)
		}

		if !reflect.DeepEqual(checkStoredData, data) {
			t.Fatalf("expected datas %v but got %v", data, checkStoredData)
		}

		if v.deleteOperations.indexUpdates != nil {
			for idx := range v.deleteOperations.indexUpdates {
				idxKeys := simple.GetKeys(idx)
				if got, want := len(idxKeys), 1; got != want {
					t.Fatalf("expected %v keys but got %v", want, got)
				}

				if got, want := idxKeys[0], v.deleteOperations.key; got != want {
					t.Fatalf("expected key %v but got %v", want, got)
				}
			}
		}
	}
}

// func TestSimpleWithIndex(t *testing.T) {
// 	simple := NewSimple()

// 	storeKey := "key-to-store"
// 	data := []byte("some stored data")
// 	simple.Store(storeKey, data, nil)

// 	checkStoredData, err := simple.Load(storeKey)
// 	if err != nil {
// 		t.Fatalf("expected %v keys but got %v", nil, err)
// 	}

// 	if !reflect.DeepEqual(checkStoredData, data) {
// 		t.Fatalf("expected datas %v but got %v", data, checkStoredData)
// 	}

// 	idxName := "index-name"
// 	indexUpdates := map[string]IndexOperation{
// 		idxName: AddToIndexOperation,
// 	}

// 	simple.Store(storeKey, nil, indexUpdates)

// 	checkStoredData, err = simple.Load(storeKey)
// 	if err != nil {
// 		t.Fatalf("expected error %v but got %v", nil, err)
// 	}

// 	if !reflect.DeepEqual(checkStoredData, []byte{}) {
// 		t.Fatalf("expected data %v but got %v", data, checkStoredData)
// 	}

// 	idxKeys := simple.GetKeys(idxName)
// 	if got, want := len(idxKeys), 1; got != want {
// 		t.Fatalf("expected %v keys but got %v", want, got)
// 	}

// 	if got, want := idxKeys[0], storeKey; got != want {
// 		t.Fatalf("expected key %v but got %v", want, got)
// 	}
// }
