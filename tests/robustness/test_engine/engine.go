package engine

import (
	"github.com/kopia/kopia/tests/robustness/snapif"
	"github.com/kopia/kopia/tests/robustness/snapstore"
	"github.com/kopia/kopia/tests/tools/fio"
)

type Engine struct {
	FileWriter  *fio.Runner
	Snapshotter snapif.Snapshotter
	MetaStore   MetadataStorer
	cleanup     []func()
}

type MetadataStorer interface {
	snapstore.Storer
	snapif.RepoManager
	LoadMetadata() error
	FlushMetadata() error
}

func NewEngine() (*Engine, error) {
	e := new(Engine)

	var err error

	// Fill the file writer
	e.FileWriter, err = fio.NewRunner()
	e.cleanup = append(e.cleanup, e.FileWriter.Cleanup)
	if err != nil {
		e.Cleanup()
		return nil, err
	}

	// Fill Snapshotter interface
	kopiaSnapper, err := snapif.NewKopiaSnapshotter()
	e.cleanup = append(e.cleanup, kopiaSnapper.Cleanup)
	if err != nil {
		e.Cleanup()
		return nil, err
	}

	e.Snapshotter = kopiaSnapper

	// Fill the snapshot store interface
	snapStore, err := snapstore.NewKopiaMetadata()
	e.cleanup = append(e.cleanup, snapStore.Cleanup)
	if err != nil {
		e.Cleanup()
		return nil, err
	}

	e.MetaStore = snapStore

	return e, nil
}

func (e *Engine) Cleanup() {
	for _, f := range e.cleanup {
		f()
	}
}
