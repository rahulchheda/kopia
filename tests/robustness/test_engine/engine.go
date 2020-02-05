package engine

import (
	"context"
	"fmt"
	"math/rand"
	"os"

	"github.com/kopia/kopia/tests/robustness/checker"
	"github.com/kopia/kopia/tests/robustness/snapif"
	"github.com/kopia/kopia/tests/robustness/snapstore"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/fswalker"
)

const (
	S3BucketNameEnvKey = "S3_BUCKET_NAME"
)

var ErrS3BucketNameEnvUnset = fmt.Errorf("Environment variable required: %v", S3BucketNameEnvKey)

type Engine struct {
	FileWriter *fio.Runner
	TestRepo   snapif.Snapshotter
	MetaStore  snapstore.DataPersister
	Checker    *checker.Checker
	cleanup    []func()
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

	checker, err := checker.NewChecker(kopiaSnapper, snapStore, fswalker.NewWalkChecker())
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

func (e *Engine) InitS3(ctx context.Context, testRepoPath, metaRepoPath string) error {
	bucketName := os.Getenv(S3BucketNameEnvKey)
	if bucketName == "" {
		return ErrS3BucketNameEnvUnset
	}

	err := e.MetaStore.ConnectOrCreateS3(bucketName, metaRepoPath)
	if err != nil {
		return err
	}

	err = e.MetaStore.LoadMetadata()
	if err != nil {
		return err
	}

	err = e.TestRepo.ConnectOrCreateS3(bucketName, testRepoPath)
	if err != nil {
		return err
	}

	err = e.Checker.VerifySnapshotMetadata()
	if err != nil {
		return err
	}

	snapIDs := e.Checker.GetLiveSnapIDs()
	if len(snapIDs) > 0 {
		randSnapID := snapIDs[rand.Intn(len(snapIDs))]
		err = e.Checker.RestoreSnapshotToPath(ctx, randSnapID, e.FileWriter.DataDir, os.Stdout)
		if err != nil {
			return err
		}
	}

	return nil
}

func (e *Engine) InitFilesystem(ctx context.Context, testRepoPath, metaRepoPath string) error {
	err := e.MetaStore.ConnectOrCreateFilesystem(metaRepoPath)
	if err != nil {
		return err
	}

	err = e.MetaStore.LoadMetadata()
	if err != nil {
		return err
	}

	err = e.TestRepo.ConnectOrCreateFilesystem(testRepoPath)
	if err != nil {
		return err
	}

	err = e.Checker.VerifySnapshotMetadata()
	if err != nil {
		return err
	}

	snapIDs := e.Checker.GetSnapIDs()
	if len(snapIDs) > 0 {
		randSnapID := snapIDs[rand.Intn(len(snapIDs))]
		err = e.Checker.RestoreSnapshotToPath(ctx, randSnapID, e.FileWriter.DataDir, os.Stdout)
		if err != nil {
			return err
		}
	}

	return nil
}
