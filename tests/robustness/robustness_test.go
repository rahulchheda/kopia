package robustness

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	engine "github.com/kopia/kopia/tests/robustness/test_engine"
	"github.com/kopia/kopia/tests/testenv"
)

// func TestManySmallFiles(t *testing.T) {
// 	fileSize := int64(4096)
// 	numFiles := 10000

// 	fioOpt := fio.Options{}.
// 		WithFileSize(fileSize).
// 		WithNumFiles(numFiles).
// 		WithBlockSize(fileSize)

// 	err := eng.FileWriter.WriteFiles("", fioOpt)
// 	testenv.AssertNoError(t, err)

// 	ctx := context.TODO()
// 	snapID, err := eng.Checker.TakeSnapshot(ctx, eng.FileWriter.LocalDataDir)
// 	testenv.AssertNoError(t, err)

// 	output, err := ioutil.TempFile("", t.Name())
// 	testenv.AssertNoError(t, err)

// 	defer output.Close() //nolint:errcheck

// 	err = eng.Checker.RestoreSnapshot(ctx, snapID, output)
// 	testenv.AssertNoError(t, err)
// }

// func TestModifyWorkload(t *testing.T) {
// 	const (
// 		numSnapshots = 10
// 		numDirs      = 10
// 		maxOpsPerMod = 5
// 	)

// 	numFiles := 10
// 	fileSize := int64(65536)
// 	fioOpt := fio.Options{}.
// 		WithBlockSize(4096).
// 		WithDedupePercentage(35).
// 		WithRandRepeat(false).
// 		WithNumFiles(numFiles).
// 		WithFileSize(fileSize)

// 	var resultIDs []string

// 	ctx := context.Background()

// 	for snapNum := 0; snapNum < numSnapshots; snapNum++ {
// 		opsThisLoop := rand.Intn(maxOpsPerMod) + 1
// 		for mod := 0; mod < opsThisLoop; mod++ {
// 			dirIdxToMod := rand.Intn(numDirs)
// 			writeToDir := filepath.Join(t.Name(), fmt.Sprintf("dir%d", dirIdxToMod))

// 			err := eng.FileWriter.WriteFiles(writeToDir, fioOpt)
// 			testenv.AssertNoError(t, err)
// 		}

// 		snapID, err := eng.Checker.TakeSnapshot(ctx, eng.FileWriter.LocalDataDir)
// 		testenv.AssertNoError(t, err)

// 		resultIDs = append(resultIDs, snapID)
// 	}

// 	for _, snapID := range resultIDs {
// 		err := eng.Checker.RestoreSnapshot(ctx, snapID, nil)
// 		testenv.AssertNoError(t, err)
// 	}
// }

// func TestRandomized(t *testing.T) {
// 	st := time.Now()

// 	opts := engine.ActionOpts{
// 		engine.ActionControlActionKey: map[string]string{
// 			string(engine.SnapshotRootDirActionKey):          strconv.Itoa(2),
// 			string(engine.RestoreRandomSnapshotActionKey):    strconv.Itoa(2),
// 			string(engine.DeleteRandomSnapshotActionKey):     strconv.Itoa(1),
// 			string(engine.WriteRandomFilesActionKey):         strconv.Itoa(8),
// 			string(engine.DeleteRandomSubdirectoryActionKey): strconv.Itoa(1),
// 		},
// 		engine.WriteRandomFilesActionKey: map[string]string{
// 			engine.IOLimitPerWriteAction: fmt.Sprintf("%d", 1*1024*1024*1024),
// 		},
// 	}

// 	// Perform actions until the timer expires, at least until one action
// 	// has been performed
// 	for time.Since(st) <= *randomizedTestDur || eng.RunStats.ActionCounter == 0 {
// 		err := eng.RandomAction(opts)
// 		testenv.AssertNoError(t, err)
// 	}
// }

func TestRandomizedSmall(t *testing.T) {
	err := eng.ExecAction(engine.RestoreIntoDataDirectoryActionKey, nil)
	if err != nil && err == engine.ErrNoOp {
		err = nil
	}

	testenv.AssertNoError(t, err)

	st := time.Now()

	opts := engine.ActionOpts{
		engine.ActionControlActionKey: map[string]string{
			string(engine.SnapshotRootDirActionKey):          strconv.Itoa(2),
			string(engine.RestoreRandomSnapshotActionKey):    strconv.Itoa(2),
			string(engine.DeleteRandomSnapshotActionKey):     strconv.Itoa(1),
			string(engine.WriteRandomFilesActionKey):         strconv.Itoa(8),
			string(engine.DeleteRandomSubdirectoryActionKey): strconv.Itoa(1),
		},
		engine.WriteRandomFilesActionKey: map[string]string{
			engine.IOLimitPerWriteAction:    fmt.Sprintf("%d", 512*1024*1024),
			engine.MaxNumFilesPerWriteField: strconv.Itoa(100),
			engine.MaxFileSizeField:         strconv.Itoa(64 * 1024 * 1024),
			engine.MaxDirDepthField:         strconv.Itoa(3),
		},
	}

	for time.Since(st) <= *randomizedTestDur {
		err := eng.RandomAction(opts)
		testenv.AssertNoError(t, err)
	}
}
