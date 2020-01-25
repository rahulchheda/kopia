package walker

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"strings"
	"testing"

	fspb "github.com/google/fswalker/proto/fswalker"

	"github.com/kopia/kopia/tests/testenv"
)

func TestWalk(t *testing.T) {
	dataDir, err := ioutil.TempDir("", "walk-data-")
	testenv.AssertNoError(t, err)

	defer os.RemoveAll(dataDir)

	counters := new(testenv.DirectoryTreeCounters)
	err = testenv.CreateDirectoryTree(
		dataDir,
		testenv.DirectoryTreeOptions{
			Depth:                  2,
			MaxSubdirsPerDirectory: 2,
			MaxFilesPerDirectory:   2,
		},
		counters,
	)
	testenv.AssertNoError(t, err)

	outW := &bytes.Buffer{}

	err = Walk(context.TODO(),
		&fspb.Policy{
			Include: []string{
				dataDir,
			},
		}, outW)
	testenv.AssertNoError(t, err)

	var foundFSEntryCount int

	// Output expected to contain the paths to all dirs/files
	lines := strings.Split(outW.String(), "\n")
	for _, line := range lines {
		if strings.Contains(line, dataDir) {
			foundFSEntryCount++
		}
	}

	if got, want := foundFSEntryCount, counters.Files+counters.Directories; got != want {
		t.Errorf("Expected number of walk entries (%v) to equal sum of file and dir counts (%v)", got, want)
	}
}

func TestWalkFail(t *testing.T) {
	outW := &bytes.Buffer{}

	err := Walk(
		context.TODO(),
		&fspb.Policy{
			Include: []string{
				"some/nonexistent/directory",
			},
		},
		outW,
	)
	if err == nil {
		t.Fatalf("Expected non-nil error when walk directory is not present")
	}

	if !strings.Contains(err.Error(), "no such file or directory") {
		t.Errorf("Expected walk call to return an error for finding no directory but got %q", err.Error())
	}
}
