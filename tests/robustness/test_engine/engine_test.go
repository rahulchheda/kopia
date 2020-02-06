// Package engine provides the framework for a snapshot repository testing engine
package engine

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
	"github.com/kopia/kopia/tests/tools/fio"
)

var (
	fsMetadataRepoPath = filepath.Join("/tmp", "metadata-repo")
	s3MetadataRepoPath = filepath.Join("some/path", "metadata-repo")
	fsDataRepoPath     = filepath.Join("/tmp", "data-repo")
	s3DataRepoPath     = filepath.Join("some/path", "data-repo")
)

func TestEngineWritefilesBasicFS(t *testing.T) {
	eng, err := NewEngine()
	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)
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
	eng, err := NewEngine()
	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)
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
	eng, err := NewEngine()
	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)
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

func TestSnapshotVerificationFail(t *testing.T) {
	eng, err := NewEngine()
	testenv.AssertNoError(t, err)

	defer func() {
		cleanupErr := eng.Cleanup()
		testenv.AssertNoError(t, cleanupErr)
	}()

	ctx := context.TODO()
	err = eng.InitS3(ctx, s3DataRepoPath, s3MetadataRepoPath)
	testenv.AssertNoError(t, err)

	// Perform writes
	fileSize := int64(256 * 1024 * 1024)
	numFiles := 10
	eng.FileWriter.WriteFiles("", fileSize, numFiles, fio.Options{})

	// Take a first snapshot
	snapID1, err := eng.Checker.TakeSnapshot(ctx, eng.FileWriter.DataDir)
	testenv.AssertNoError(t, err)

	// Get the metadata collected on that snapshot
	ssMeta1, err := eng.Checker.GetSnapshotMetadata(snapID1)
	testenv.AssertNoError(t, err)

	// Do additional writes, writing 1 extra byte than before
	eng.FileWriter.WriteFiles("", fileSize, numFiles, fio.Options{
		"io_size": strconv.Itoa(int(fileSize + 1)),
	})

	// Take a second snapshot
	snapID2, err := eng.Checker.TakeSnapshot(ctx, eng.FileWriter.DataDir)
	testenv.AssertNoError(t, err)

	// Get the second snapshot's metadata
	ssMeta2, err := eng.Checker.GetSnapshotMetadata(snapID2)
	testenv.AssertNoError(t, err)

	// Swap second snapshot's validation data into the first's metadata
	ssMeta1.ValidationData = ssMeta2.ValidationData

	restoreDir, err := ioutil.TempDir(eng.Checker.RestoreDir, fmt.Sprintf("restore-snap-%v", snapID1))
	testenv.AssertNoError(t, err)

	defer os.RemoveAll(restoreDir) //nolint:errcheck

	// Restore snapshot ID 1 with snapshot 2's validation data in metadata, expect error
	err = eng.Checker.RestoreVerifySnapshot(ctx, snapID1, restoreDir, ssMeta1, os.Stdout)
	if err == nil {
		t.Fatalf("Expected an integrity error when trying to restore a snapshot with incorrect metadata")
	}
}
