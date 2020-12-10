// +build darwin,amd64 linux,amd64

package engine

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/kopia/kopia/tests/robustness/snapmeta"
)

const (
	engineStatsStoreKey = "cumulative-engine-stats"
	engineLogsStoreKey  = "engine-logs"
)

// SaveLog saves the engine Log in the metadata store.
func (e *Engine) SaveLog() error {
	b, err := json.Marshal(e.EngineLog)
	if err != nil {
		return err
	}

	_, operationErr := e.MetaStore.IndexOperation(snapmeta.OperationEntry{
		Operation: snapmeta.StoreOperation,
		Key:       engineLogsStoreKey,
		Data:      b,
	})

	return operationErr
}

// LoadLog loads the engine log from the metadata store.
func (e *Engine) LoadLog() error {
	b, err := e.MetaStore.IndexOperation(snapmeta.OperationEntry{
		Operation: snapmeta.LoadOperation,
		Key:       engineLogsStoreKey,
	})
	if err != nil {
		if errors.Is(err, snapmeta.ErrKeyNotFound) {
			// Swallow key-not-found error. May not have historical logs
			return nil
		}

		return err
	}

	err = json.Unmarshal(b.([]byte), &e.EngineLog)
	if err != nil {
		return err
	}

	e.EngineLog.runOffset = len(e.EngineLog.Log)

	return err
}

// SaveStats saves the engine Stats in the metadata store.
func (e *Engine) SaveStats() error {
	cumulStatRaw, err := json.Marshal(e.CumulativeStats)
	if err != nil {
		return err
	}

	_, operationErr := e.MetaStore.IndexOperation(snapmeta.OperationEntry{
		Operation: snapmeta.StoreOperation,
		Key:       engineStatsStoreKey,
		Data:      cumulStatRaw,
	})

	return operationErr
}

// LoadStats loads the engine Stats from the metadata store.
func (e *Engine) LoadStats() error {
	b, err := e.MetaStore.IndexOperation(snapmeta.OperationEntry{
		Operation: snapmeta.LoadOperation,
		Key:       engineStatsStoreKey,
		Data:      nil,
	})
	if err != nil {
		if errors.Is(err, snapmeta.ErrKeyNotFound) {
			// Swallow key-not-found error. We may not have historical
			// stats data. Initialize the action map for the cumulative stats
			e.CumulativeStats.PerActionStats = make(map[ActionKey]*ActionStats)
			e.CumulativeStats.CreationTime = time.Now()

			return nil
		}

		return err
	}

	return json.Unmarshal(b.([]byte), &e.CumulativeStats)
}
