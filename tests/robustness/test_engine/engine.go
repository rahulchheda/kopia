package engine

import (
	"github.com/kopia/kopia/tests/robustness/snapif"
	"github.com/kopia/kopia/tests/robustness/snapstore"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/fswalker"
)

type Engine struct {
	FileWriter *fio.Runner
	TestRepo   snapif.RepoManager
	MetaStore  MetadataStorer
	Checker    fswalker.CheckerIF
	cleanup    []func()
}

type MetadataStorer interface {
	snapif.RepoManager
	LoadMetadata() error
	FlushMetadata() error
}

func NewEngine() (*Engine, error) {
	e := new(Engine)

	var err error

	// Fill the file writer
	e.FileWriter, err = fio.NewRunner()
	if err != nil {
		e.Cleanup()
		return nil, err
	}

	e.cleanup = append(e.cleanup, e.FileWriter.Cleanup)

	// Fill Snapshotter interface
	kopiaSnapper, err := snapif.NewKopiaSnapshotter()
	if err != nil {
		e.Cleanup()
		return nil, err
	}

	e.cleanup = append(e.cleanup, kopiaSnapper.Cleanup)
	e.TestRepo = kopiaSnapper

	// Fill the snapshot store interface
	snapStore, err := snapstore.NewKopiaMetadata()
	if err != nil {
		e.Cleanup()
		return nil, err
	}
	e.cleanup = append(e.cleanup, snapStore.Cleanup)

	e.MetaStore = snapStore

	checker, err := fswalker.NewChecker(kopiaSnapper, snapStore)
	e.cleanup = append(e.cleanup, checker.Cleanup)
	if err != nil {
		e.Cleanup()
		return nil, err
	}

	e.Checker = checker

	return e, nil
}

func (e *Engine) Cleanup() {
	for _, f := range e.cleanup {
		f()
	}
}
