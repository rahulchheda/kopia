package robustness

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	engine "github.com/kopia/kopia/tests/robustness/test_engine"
	"github.com/kopia/kopia/tests/testenv"
)

func TestManySmallFiles(t *testing.T) {
	fileSize := 4096
	numFiles := 10000

	fileWriteOpts := map[string]string{
		engine.MaxDirDepthField:         strconv.Itoa(1),
		engine.MaxFileSizeField:         strconv.Itoa(fileSize),
		engine.MinFileSizeField:         strconv.Itoa(fileSize),
		engine.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		engine.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	_, err := eng.ExecAction(engine.WriteRandomFilesActionKey, fileWriteOpts)
	testenv.AssertNoError(t, err)

	snapOut, err := eng.ExecAction(engine.SnapshotRootDirActionKey, nil)
	testenv.AssertNoError(t, err)

	_, err = eng.ExecAction(engine.RestoreSnapshotActionKey, snapOut)
	testenv.AssertNoError(t, err)
}

func TestOneLargeFile(t *testing.T) {
	fileSize := 40 * 1024 * 1024
	numFiles := 1

	fileWriteOpts := map[string]string{
		engine.MaxDirDepthField:         strconv.Itoa(1),
		engine.MaxFileSizeField:         strconv.Itoa(fileSize),
		engine.MinFileSizeField:         strconv.Itoa(fileSize),
		engine.MaxNumFilesPerWriteField: strconv.Itoa(numFiles),
		engine.MinNumFilesPerWriteField: strconv.Itoa(numFiles),
	}

	_, err := eng.ExecAction(engine.WriteRandomFilesActionKey, fileWriteOpts)
	testenv.AssertNoError(t, err)

	snapOut, err := eng.ExecAction(engine.SnapshotRootDirActionKey, nil)
	testenv.AssertNoError(t, err)

	_, err = eng.ExecAction(engine.RestoreSnapshotActionKey, snapOut)
	testenv.AssertNoError(t, err)
}

func TestManySmallFilesAcrossDirecoryTree(t *testing.T) {
	t.Skip("Test takes too long - need to address performance issues with fio writes")
	fileSize := 4096
	numFiles := 10000
	filesPerWrite := 10
	actionRepeats := numFiles / filesPerWrite

	fileWriteOpts := map[string]string{
		engine.MaxDirDepthField:         strconv.Itoa(15),
		engine.MaxFileSizeField:         strconv.Itoa(fileSize),
		engine.MinFileSizeField:         strconv.Itoa(fileSize),
		engine.MaxNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		engine.MinNumFilesPerWriteField: strconv.Itoa(filesPerWrite),
		engine.ActionRepeaterField:      strconv.Itoa(actionRepeats),
	}

	_, err := eng.ExecAction(engine.WriteRandomFilesActionKey, fileWriteOpts)
	testenv.AssertNoError(t, err)

	snapOut, err := eng.ExecAction(engine.SnapshotRootDirActionKey, nil)
	testenv.AssertNoError(t, err)

	_, err = eng.ExecAction(engine.RestoreSnapshotActionKey, snapOut)
	testenv.AssertNoError(t, err)
}

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
	_, err := eng.ExecAction(engine.RestoreIntoDataDirectoryActionKey, nil)
	if err != nil && err == engine.ErrNoOp {
		err = nil
	}

	testenv.AssertNoError(t, err)

	st := time.Now()

	opts := engine.ActionOpts{
		engine.ActionControlActionKey: map[string]string{
			string(engine.SnapshotRootDirActionKey):          strconv.Itoa(2),
			string(engine.RestoreSnapshotActionKey):          strconv.Itoa(2),
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