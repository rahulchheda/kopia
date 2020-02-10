package robustness

import (
	"testing"

	"github.com/kopia/kopia/tests/testenv"
	"github.com/kopia/kopia/tests/tools/fio"
)

func TestManySmallFiles(t *testing.T) {
	fileSize := 64
	numFiles := 1000
	err := eng.FileWriter.WriteFiles(t.Name(), fileSize, numFiles, fio.Options{})
	testenv.AssertNoError(t, err)
}
