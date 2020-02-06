package robustness_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	engine "github.com/kopia/kopia/tests/robustness/test_engine"
	"github.com/kopia/kopia/tests/testenv"
	"github.com/kopia/kopia/tests/tools/fio"
)

var (
	fsMetadataRepoPath = filepath.Join("/tmp", "metadata-repo")
	s3MetadataRepoPath = filepath.Join("some/path", "metadata-repo")
	fsDataRepoPath     = filepath.Join("/tmp", "data-repo")
	s3DataRepoPath     = filepath.Join("some/path", "data-repo")
)

func TestWriteFilesBasicFS(t *testing.T) {
	eng, err := engine.NewEngine()
	testenv.AssertNoError(t, err)

	defer func() {
		err := eng.Cleanup()
		testenv.AssertNoError(t, err)
	}()

	ctx := context.TODO()
	err = eng.InitFilesystem(ctx, fsDataRepoPath, fsMetadataRepoPath)
	testenv.AssertNoError(t, err)

	fileSize := int64(256 * 1024 * 1024)
	numFiles := 10
	eng.FileWriter.WriteFiles("", fileSize, numFiles, fio.Options{})

	snapIDs := eng.Checker.GetSnapIDs()

	snapID, err := eng.Checker.TakeSnapshot(ctx, eng.FileWriter.DataDir)
	testenv.AssertNoError(t, err)

	err = eng.Checker.RestoreSnapshot(ctx, snapID, os.Stdout)
	testenv.AssertNoError(t, err)

	for _, sID := range snapIDs {
		err = eng.Checker.RestoreSnapshot(ctx, sID, os.Stdout)
		testenv.AssertNoError(t, err)
	}
}

func TestWriteFilesBasicS3(t *testing.T) {
	eng, err := engine.NewEngine()
	testenv.AssertNoError(t, err)

	defer func() {
		err := eng.Cleanup()
		testenv.AssertNoError(t, err)
	}()

	ctx := context.TODO()
	err = eng.InitS3(ctx, s3DataRepoPath, s3MetadataRepoPath)
	testenv.AssertNoError(t, err)

	fileSize := int64(256 * 1024 * 1024)
	numFiles := 10
	eng.FileWriter.WriteFiles("", fileSize, numFiles, fio.Options{})

	snapIDs := eng.Checker.GetLiveSnapIDs()

	snapID, err := eng.Checker.TakeSnapshot(ctx, eng.FileWriter.DataDir)
	testenv.AssertNoError(t, err)

	err = eng.Checker.RestoreSnapshot(ctx, snapID, os.Stdout)
	testenv.AssertNoError(t, err)

	for _, sID := range snapIDs {
		err = eng.Checker.RestoreSnapshot(ctx, sID, os.Stdout)
		testenv.AssertNoError(t, err)
	}
}

func TestDeleteSnapshotS3(t *testing.T) {
	eng, err := engine.NewEngine()
	testenv.AssertNoError(t, err)

	defer func() {
		err := eng.Cleanup()
		testenv.AssertNoError(t, err)
	}()

	ctx := context.TODO()
	err = eng.InitS3(ctx, s3DataRepoPath, s3MetadataRepoPath)
	testenv.AssertNoError(t, err)

	fileSize := int64(256 * 1024 * 1024)
	numFiles := 10
	eng.FileWriter.WriteFiles("", fileSize, numFiles, fio.Options{})

	snapID, err := eng.Checker.TakeSnapshot(ctx, eng.FileWriter.DataDir)
	testenv.AssertNoError(t, err)

	err = eng.Checker.RestoreSnapshot(ctx, snapID, os.Stdout)
	testenv.AssertNoError(t, err)

	err = eng.Checker.DeleteSnapshot(ctx, snapID)
	testenv.AssertNoError(t, err)

	err = eng.Checker.RestoreSnapshot(ctx, snapID, os.Stdout)
	if err == nil {
		t.Fatalf("Expected an error when trying to restore a deleted snapshot")
	}
}
