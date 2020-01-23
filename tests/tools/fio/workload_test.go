package fio

import (
	"fmt"
	"io/ioutil"
	"path/filepath"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestWriteFiles(t *testing.T) {
	r, err := NewRunner()
	testenv.AssertNoError(t, err)

	defer r.Cleanup()

	relativeWritePath := "some/path/to/check"
	writeSizeB := int64(3 * 1024 * 1024 * 1024) // 3 GiB
	numFiles := 13

	// Test a call to WriteFiles
	err = r.WriteFiles(relativeWritePath, writeSizeB, numFiles, Options{})
	testenv.AssertNoError(t, err)

	fullPath := filepath.Join(r.DataDir, relativeWritePath)
	dir, err := ioutil.ReadDir(fullPath)
	testenv.AssertNoError(t, err)

	if got, want := len(dir), numFiles; got != want {
		t.Errorf("Did not get expected number of files %v (actual) != %v (expected", got, want)
	}

	sizeTot := int64(0)
	sizeExp := int64(0)

	for _, fi := range dir {
		fmt.Println(fi.Name(), fi.Size())
		sizeTot += fi.Size()

		// Calculate the expected size taking into account the rounding error
		// introduced by dividing the requested write size across the number of files
		sizeExp += writeSizeB / int64(numFiles)
	}

	if got, want := sizeTot, sizeExp; got != want {
		t.Errorf("Did not get the expected amount of data written %v (actual) != %v (expected)", got, want)
	}
}
