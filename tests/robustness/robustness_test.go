package robustness_test

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"testing"

	"github.com/google/fswalker"
	fspb "github.com/google/fswalker/proto/fswalker"

	"github.com/kopia/kopia/tests/robustness/snapif"
	"github.com/kopia/kopia/tests/robustness/snapstore"
	"github.com/kopia/kopia/tests/testenv"
	"github.com/kopia/kopia/tests/tools/fio"
	fswwrap "github.com/kopia/kopia/tests/tools/fswalker"
	"github.com/kopia/kopia/tests/tools/fswalker/reporter"
	"github.com/kopia/kopia/tests/tools/fswalker/walker"
)

func TestBasicRestore(t *testing.T) {
	e := testenv.NewCLITest(t)
	defer e.Cleanup(t)

	e.RunAndExpectSuccess(t, "repo", "create", "filesystem", "--path", e.RepoDir)

	fioRunner, err := fio.NewRunner()
	testenv.AssertNoError(t, err)

	fileSize := int64(256 * 1024 * 1024)
	numFiles := 10
	err = fioRunner.WriteFiles("", fileSize, numFiles, fio.Options{})
	testenv.AssertNoError(t, err)

	// ==========================
	// Snapshot
	walk, err := walker.WalkPathHash(context.Background(), fioRunner.DataDir)
	testenv.AssertNoError(t, err)

	for _, f := range walk.File {
		f.Path, err = filepath.Rel(fioRunner.DataDir, f.Path)
		testenv.AssertNoError(t, err)
	}

	_, errOut := e.RunAndExpectSuccessWithErrOut(t, "snapshot", "create", fioRunner.DataDir)
	snapID := parseSnapID(t, errOut)

	// ==========================
	// Restore

	restoreDir, err := ioutil.TempDir("", "restore-data-")
	testenv.AssertNoError(t, err)

	defer os.RemoveAll(restoreDir) //nolint:errcheck

	e.RunAndExpectSuccess(t, "snapshot", "restore", snapID, restoreDir)

	walk2, err := walker.WalkPathHash(context.Background(), restoreDir)
	testenv.AssertNoError(t, err)

	for _, f := range walk2.File {
		f.Path, err = filepath.Rel(restoreDir, f.Path)
		testenv.AssertNoError(t, err)
	}

	report, err := reporter.Report(context.Background(), &fspb.ReportConfig{}, walk, walk2)
	testenv.AssertNoError(t, err)

	rptr := fswalker.Reporter{}
	rptr.PrintDiffSummary(os.Stdout, report)

	for _, mod := range report.Modified {
		fmt.Println(mod.Diff)
	}
}

func parseSnapID(t *testing.T, lines []string) string {
	pattern := regexp.MustCompile(`uploaded snapshot ([\S]+)`)

	for _, l := range lines {
		match := pattern.FindAllStringSubmatch(l, 1)
		if len(match) > 0 && len(match[0]) > 1 {
			return match[0][1]
		}
	}

	t.Fatal("Snap ID could not be parsed")

	return ""
}

var (
	metadataRepoPath = filepath.Join("/tmp", "metadata-repo")
	dataRepoPath     = filepath.Join("/tmp", "data-repo")
)

func TestEngine(t *testing.T) {
	fioRunner, err := fio.NewRunner()
	defer fioRunner.Cleanup()
	testenv.AssertNoError(t, err)

	fileSize := int64(256 * 1024 * 1024)
	numFiles := 10
	err = fioRunner.WriteFiles("", fileSize, numFiles, fio.Options{})
	testenv.AssertNoError(t, err)

	kopiaSnapper, err := snapif.NewKopiaSnapshotter()
	defer kopiaSnapper.Cleanup()
	testenv.AssertNoError(t, err)

	// repoDir, err := ioutil.TempDir("", "kopia-repo-")
	// defer os.RemoveAll(repoDir) //nolint:errcheck
	// testenv.AssertNoError(t, err)
	// kopiaSnapper.CreateRepo("filesystem", "--path", repoDir)

	// kopiaSnapper.CreateRepo("s3", "--bucket", "nick-kasten-io-test-1", "--prefix", "some/prefix/")

	err = kopiaSnapper.ConnectOrCreateRepo("filesystem", "--path", dataRepoPath)
	testenv.AssertNoError(t, err)

	// snapStore := snapstore.NewSimple()
	snapStore, err := snapstore.NewKopiaMetadata()
	defer snapStore.Cleanup() //nolint:errcheck
	testenv.AssertNoError(t, err)

	err = snapStore.ConnectOrCreateRepoFilesystem(metadataRepoPath)
	testenv.AssertNoError(t, err)

	defer func() {
		err := snapStore.FlushMetadata()
		testenv.AssertNoError(t, err)
	}()

	err = snapStore.LoadMetadata()
	if err != nil {
		fmt.Println("LOAD METADATA ERROR:", err.Error())
	}

	chkr, err := fswwrap.NewChecker(kopiaSnapper, snapStore)
	defer chkr.Cleanup()
	testenv.AssertNoError(t, err)

	keys := snapStore.GetKeys()

	ctx := context.Background()
	snapID, err := chkr.TakeSnapshot(ctx, fioRunner.DataDir)
	testenv.AssertNoError(t, err)

	err = chkr.RestoreSnapshot(ctx, snapID, os.Stdout)
	testenv.AssertNoError(t, err)

	for _, key := range keys {
		err = chkr.RestoreSnapshot(ctx, key, os.Stdout)
		testenv.AssertNoError(t, err)
	}
}
