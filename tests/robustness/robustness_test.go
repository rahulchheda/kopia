package robustness

import (
	"context"
	"io/ioutil"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
	"github.com/kopia/kopia/tests/tools/fio"
)

func TestManySmallFiles(t *testing.T) {
	fileSize := int64(4096)
	numFiles := 100000
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
	const numSnapshots = 10
	const numDirs = 10
	const maxOpsPerMod = 5

	for snapNum := 0; snapNum < numSnapshots; snapNum++ {

	}
}
