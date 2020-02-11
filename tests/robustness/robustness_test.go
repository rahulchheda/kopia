package robustness

import (
	"context"
	"fmt"
	"io/ioutil"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
	"github.com/kopia/kopia/tests/tools/fio"
)

func TestManySmallFiles(t *testing.T) {
	fileSize := int64(4096)
	numFiles := 100

	err := eng.FileWriter.WriteFiles("", fileSize*int64(numFiles), numFiles, fio.Options{
		"blocksize": "4096",
	})
	testenv.AssertNoError(t, err)

	ctx := context.TODO()
	snapID, err := eng.Checker.TakeSnapshot(ctx, eng.FileWriter.DataDir)
	testenv.AssertNoError(t, err)

	output, err := ioutil.TempFile("", t.Name())
	testenv.AssertNoError(t, err)

	defer output.Close() //nolint:errcheck

	err = eng.Checker.RestoreSnapshot(ctx, snapID, output)
	testenv.AssertNoError(t, err)
}

func TestModifyWorkload(t *testing.T) {
	const (
		numSnapshots = 10
		numDirs      = 10
		maxOpsPerMod = 5
	)

	numFiles := 10
	writeSize := int64(65536 * numFiles)
	fioOpt := fio.Options{
		"dedupe_percentage": "35",
		"randrepeat":        "0",
		"blocksize":         "4096",
	}

	var resultIDs []string

	ctx := context.Background()

	for snapNum := 0; snapNum < numSnapshots; snapNum++ {
		opsThisLoop := rand.Intn(maxOpsPerMod) + 1
		for mod := 0; mod < opsThisLoop; mod++ {
			dirIdxToMod := rand.Intn(numDirs)
			writeToDir := filepath.Join(t.Name(), fmt.Sprintf("dir%d", dirIdxToMod))

			err := eng.FileWriter.WriteFiles(writeToDir, writeSize, numFiles, fioOpt)
			testenv.AssertNoError(t, err)
		}

		snapID, err := eng.Checker.TakeSnapshot(ctx, eng.FileWriter.DataDir)
		testenv.AssertNoError(t, err)

		resultIDs = append(resultIDs, snapID)
	}

	for _, snapID := range resultIDs {
		err := eng.Checker.RestoreSnapshot(ctx, snapID, nil)
		testenv.AssertNoError(t, err)
	}
}
