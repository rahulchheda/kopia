package walker

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestWalkerRun(t *testing.T) {
	wr, err := NewRunner()
	testenv.AssertNoError(t, err)

	stdout, stderr, err := wr.Run()
	if err == nil {
		t.Fatal("Expected error to be set as no params were passed")
	}
	fmt.Println("Stdout", stdout)
	fmt.Println("Stderr", stderr)
}

func TestWalkerRunPolicy(t *testing.T) {
	walkTarget := "some/path"
	testenv.MustCreateDirectoryTree(t, walkTarget, testenv.DirectoryTreeOptions{
		Depth:                  2,
		MaxSubdirsPerDirectory: 2,
		MaxFilesPerDirectory:   2,
	})

	wr, err := NewRunner()
	testenv.AssertNoError(t, err)

	outputDir, err := ioutil.TempDir("", "test-output-")
	testenv.AssertNoError(t, err)
	defer os.RemoveAll(outputDir)

	_, err = wr.RunPolicy(Policy{
		Name:    "test-policy-",
		Include: []string{walkTarget},
	},
		outputDir,
		false,
	)

	testenv.AssertNoError(t, err)

	fl, err := ioutil.ReadDir(outputDir)
	testenv.AssertNoError(t, err)

	if want, got := 1, len(fl); want != got {
		t.Errorf("Output directory expected to have %d output files but got %d", want, got)
	}
}
