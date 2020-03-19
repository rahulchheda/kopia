package engine

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/kopia/kopia/tests/tools/fio"
)

// ExecAction executes the action denoted by the provided ActionKey
func (e *Engine) ExecAction(actionKey ActionKey, opts map[string]string) (map[string]string, error) {
	if opts == nil {
		opts = make(map[string]string)
	}

	action := actions[actionKey]

	if e.ReadOnly && !action.ReadOnlyOkay {
		return nil, ErrReadOnlyMode
	}

	st := time.Now()

	e.RunStats.ActionCounter++
	e.CumulativeStats.ActionCounter++
	log.Printf("Engine executing ACTION: name=%q actionCount=%v totActCount=%v t=%vs (%vs)", actionKey, e.RunStats.ActionCounter, e.CumulativeStats.ActionCounter, e.RunStats.getLifetimeSeconds(), e.getRuntimeSeconds())

	logEntry := &LogEntry{
		StartTime:       st,
		EngineTimestamp: e.getTimestampS(),
		Action:          actionKey,
		ActionOpts:      opts,
	}

	// Execute the action n times
	err := ErrNoOp // Default to no-op error

	// TODO: return more than the last output
	var out map[string]string

	n := getOptAsIntOrDefault(ActionRepeaterField, opts, defaultActionRepeats)
	for i := 0; i < n; i++ {
		out, err = action.f(e, opts, logEntry)
		if err != nil {
			break
		}
	}

	// If error was just a no-op, don't bother logging the action
	switch {
	case errorIs(err, ErrNoOp):
		e.RunStats.NoOpCount++
		e.CumulativeStats.NoOpCount++

		return out, err

	case err != nil:
		log.Printf("error=%q", err.Error())
	}

	if e.RunStats.PerActionStats != nil && e.RunStats.PerActionStats[actionKey] == nil {
		e.RunStats.PerActionStats[actionKey] = new(ActionStats)
	}

	if e.CumulativeStats.PerActionStats != nil && e.CumulativeStats.PerActionStats[actionKey] == nil {
		e.CumulativeStats.PerActionStats[actionKey] = new(ActionStats)
	}

	e.RunStats.PerActionStats[actionKey].Record(st, err)
	e.CumulativeStats.PerActionStats[actionKey].Record(st, err)

	e.EngineLog.AddCompleted(logEntry, err)

	return out, err
}

// RandomAction executes a random action picked by the relative weights given
// in actionOpts[ActionControlActionKey], or uniform probability if that
// key is not present in the input options
func (e *Engine) RandomAction(actionOpts ActionOpts) error {
	actionControlOpts := actionOpts.getActionControlOpts()

	actionName := pickActionWeighted(actionControlOpts, actions)
	if string(actionName) == "" {
		return fmt.Errorf("unable to pick an action with the action control options provided")
	}

	_, err := e.ExecAction(actionName, actionOpts[actionName])
	err = e.checkErrRecovery(err, actionOpts)

	return err
}

func (e *Engine) checkErrRecovery(incomingErr error, actionOpts ActionOpts) (outgoingErr error) {
	outgoingErr = incomingErr
	if incomingErr == nil {
		return nil
	}

	ctrl := actionOpts.getActionControlOpts()

	if errIsNotEnoughSpace(incomingErr) && ctrl[ThrowNoSpaceOnDeviceErrField] == "" {
		// no space left on device
		// Delete everything in the data directory
		const hundredPcnt = 100

		deleteDirActionKey := DeleteDirectoryContentsActionKey
		deleteRootOpts := map[string]string{
			MaxDirDepthField:             strconv.Itoa(0),
			DeletePercentOfContentsField: strconv.Itoa(hundredPcnt),
		}

		_, outgoingErr = e.ExecAction(deleteDirActionKey, deleteRootOpts)
		if outgoingErr != nil {
			return outgoingErr
		}

		e.RunStats.DataPurgeCount++
		e.CumulativeStats.DataPurgeCount++

		// Restore a previoius snapshot to the data directory
		restoreActionKey := RestoreIntoDataDirectoryActionKey
		_, outgoingErr = e.ExecAction(restoreActionKey, actionOpts[restoreActionKey])

		if errorIs(outgoingErr, ErrNoOp) {
			outgoingErr = nil
		} else {
			e.RunStats.DataRestoreCount++
			e.CumulativeStats.DataRestoreCount++
		}
	}

	if outgoingErr == nil {
		e.RunStats.ErrorRecoveryCount++
		e.CumulativeStats.ErrorRecoveryCount++
	}

	return outgoingErr
}

// List of action keys
const (
	ActionControlActionKey            ActionKey = "action-control"
	SnapshotRootDirActionKey          ActionKey = "snapshot-root"
	RestoreSnapshotActionKey          ActionKey = "restore-random-snapID"
	DeleteRandomSnapshotActionKey     ActionKey = "delete-random-snapID"
	WriteRandomFilesActionKey         ActionKey = "write-random-files"
	DeleteRandomSubdirectoryActionKey ActionKey = "delete-random-subdirectory"
	DeleteDirectoryContentsActionKey  ActionKey = "delete-files"
	RestoreIntoDataDirectoryActionKey ActionKey = "restore-into-data-dir"
)

// ActionOpts is a structure that designates the options for
// picking and running an action
type ActionOpts map[ActionKey]map[string]string

func (actionOpts ActionOpts) getActionControlOpts() map[string]string {
	actionControlOpts := defaultActionControls()
	if actionOpts != nil && actionOpts[ActionControlActionKey] != nil {
		actionControlOpts = actionOpts[ActionControlActionKey]
	}

	return actionControlOpts
}

// Action is a unit of functionality that can be executed by
// the engine
type Action struct {
	ReadOnlyOkay bool
	f            func(eng *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error)
}

// ActionKey refers to an action that can be executed by the engine
type ActionKey string

var actions = map[ActionKey]Action{
	SnapshotRootDirActionKey: {
		ReadOnlyOkay: false,
		f: func(e *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error) {

			log.Printf("Creating snapshot of root directory %s", e.FileWriter.LocalDataDir)

			ctx := context.TODO()
			snapID, err := e.Checker.TakeSnapshot(ctx, e.FileWriter.LocalDataDir)

			setLogEntryCmdOpts(l, map[string]string{
				"snap-dir": e.FileWriter.LocalDataDir,
				"snapID":   snapID,
			})

			return map[string]string{
				SnapshotIDField: snapID,
			}, err
		},
	},
	RestoreSnapshotActionKey: {
		ReadOnlyOkay: true,
		f: func(e *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error) {
			snapID, err := e.getSnapIDOptOrRandLive(opts)
			if err != nil {
				return nil, err
			}

			setLogEntryCmdOpts(l, map[string]string{"snapID": snapID})

			log.Printf("Restoring snapshot %s", snapID)

			ctx := context.Background()
			b := &bytes.Buffer{}

			err = e.Checker.RestoreSnapshot(ctx, snapID, b)
			if err != nil {
				log.Print(b.String())
			}

			return nil, err
		},
	},
	DeleteRandomSnapshotActionKey: {
		ReadOnlyOkay: false,
		f: func(e *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error) {
			snapID, err := e.getSnapIDOptOrRandLive(opts)
			if err != nil {
				return nil, err
			}

			log.Printf("Deleting snapshot %s", snapID)

			setLogEntryCmdOpts(l, map[string]string{"snapID": snapID})

			ctx := context.Background()
			err = e.Checker.DeleteSnapshot(ctx, snapID)
			return nil, err
		},
	},
	WriteRandomFilesActionKey: {
		ReadOnlyOkay: true,
		f: func(e *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error) {

			// Directory depth
			maxDirDepth := getOptAsIntOrDefault(MaxDirDepthField, opts, defaultMaxDirDepth)
			dirDepth := rand.Intn(maxDirDepth + 1)

			// File size range
			maxFileSizeB := getOptAsIntOrDefault(MaxFileSizeField, opts, defaultMaxFileSize)
			minFileSizeB := getOptAsIntOrDefault(MinFileSizeField, opts, defaultMinFileSize)

			// Number of files to write
			maxNumFiles := getOptAsIntOrDefault(MaxNumFilesPerWriteField, opts, defaultMaxNumFilesPerWrite)
			minNumFiles := getOptAsIntOrDefault(MinNumFilesPerWriteField, opts, defaultMinNumFilesPerWrite)

			numFiles := rand.Intn(maxNumFiles-minNumFiles+1) + minNumFiles

			// Dedup Percentage
			maxDedupPcnt := getOptAsIntOrDefault(MaxDedupePercentField, opts, defaultMaxDedupePercent)
			minDedupPcnt := getOptAsIntOrDefault(MinDedupePercentField, opts, defaultMinDedupePercent)

			dedupStep := getOptAsIntOrDefault(DedupePercentStepField, opts, defaultDedupePercentStep)

			dedupPcnt := dedupStep * (rand.Intn(maxDedupPcnt/dedupStep-minDedupPcnt/dedupStep+1) + minDedupPcnt/dedupStep)

			blockSize := int64(defaultMinFileSize)

			fioOpts := fio.Options{}.
				WithFileSizeRange(int64(minFileSizeB), int64(maxFileSizeB)).
				WithNumFiles(numFiles).
				WithBlockSize(blockSize).
				WithDedupePercentage(dedupPcnt).
				WithNoFallocate()

			ioLimit := getOptAsIntOrDefault(IOLimitPerWriteAction, opts, defaultIOLimitPerWriteAction)

			if ioLimit > 0 {
				freeSpaceLimitB := getOptAsIntOrDefault(FreeSpaceLimitField, opts, defaultFreeSpaceLimit)

				freeSpaceB, err := getFreeSpaceB(e.FileWriter.LocalDataDir)
				if err != nil {
					return nil, err
				}
				log.Printf("Free Space %v B, limit %v B, ioLimit %v B\n", freeSpaceB, freeSpaceLimitB, ioLimit)

				if int(freeSpaceB)-ioLimit < freeSpaceLimitB {
					ioLimit = int(freeSpaceB) - freeSpaceLimitB
					log.Printf("Cutting down I/O limit for space %v", ioLimit)
					if ioLimit <= 0 {
						return nil, ErrCannotPerformIO
					}
				}

				fioOpts = fioOpts.WithIOLimit(int64(ioLimit))
			}

			relBasePath := "."

			log.Printf("Writing files at depth %v (fileSize: %v-%v, numFiles: %v, blockSize: %v, dedupPcnt: %v, ioLimit: %v)\n", dirDepth, minFileSizeB, maxFileSizeB, numFiles, blockSize, dedupPcnt, ioLimit)

			setLogEntryCmdOpts(l, map[string]string{
				"dirDepth":    strconv.Itoa(dirDepth),
				"relBasePath": relBasePath,
			})

			for k, v := range fioOpts {
				l.CmdOpts[k] = v
			}

			return nil, e.FileWriter.WriteFilesAtDepthRandomBranch(relBasePath, dirDepth, fioOpts)
		},
	},
	DeleteRandomSubdirectoryActionKey: {
		ReadOnlyOkay: true,
		f: func(e *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error) {
			maxDirDepth := getOptAsIntOrDefault(MaxDirDepthField, opts, defaultMaxDirDepth)
			if maxDirDepth <= 0 {
				return nil, fmt.Errorf("invalid option setting: %s=%v", MaxDirDepthField, maxDirDepth)
			}
			dirDepth := rand.Intn(maxDirDepth) + 1

			log.Printf("Deleting directory at depth %v\n", dirDepth)

			setLogEntryCmdOpts(l, map[string]string{"dirDepth": strconv.Itoa(dirDepth)})

			err = e.FileWriter.DeleteDirAtDepth("", dirDepth)
			if err != nil && err == fio.ErrNoDirFound {
				log.Print(err)
				return nil, nil
			}

			return nil, err
		},
	},
	DeleteDirectoryContentsActionKey: {
		ReadOnlyOkay: true,
		f: func(e *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error) {
			maxDirDepth := getOptAsIntOrDefault(MaxDirDepthField, opts, defaultMaxDirDepth)
			dirDepth := rand.Intn(maxDirDepth + 1)

			pcnt := getOptAsIntOrDefault(DeletePercentOfContentsField, opts, defaultDeletePercentOfContents)

			log.Printf("Deleting %d%% of directory contents at depth %v\n", pcnt, dirDepth)

			setLogEntryCmdOpts(l, map[string]string{
				"dirDepth": strconv.Itoa(dirDepth),
				"percent":  strconv.Itoa(pcnt),
			})

			err = e.FileWriter.DeleteContentsAtDepth("", dirDepth, pcnt)

			return nil, err
		},
	},
	RestoreIntoDataDirectoryActionKey: {
		ReadOnlyOkay: true,
		f: func(e *Engine, opts map[string]string, l *LogEntry) (out map[string]string, err error) {
			snapID, err := e.getSnapIDOptOrRandLive(opts)
			if err != nil {
				return nil, err
			}

			log.Printf("Restoring snap ID %v into data directory\n", snapID)

			setLogEntryCmdOpts(l, map[string]string{"snapID": snapID})

			b := &bytes.Buffer{}
			err = e.Checker.RestoreSnapshotToPath(context.Background(), snapID, e.FileWriter.LocalDataDir, b)
			if err != nil {
				log.Print(b.String())
				return nil, err
			}

			return nil, nil
		},
	},
}

// Action constants
const (
	defaultMaxDirDepth             = 20
	defaultMaxFileSize             = 1 * 1024 * 1024 * 1024 // 1GB
	defaultMinFileSize             = 4096
	defaultMaxNumFilesPerWrite     = 10000
	defaultMinNumFilesPerWrite     = 1
	defaultIOLimitPerWriteAction   = 0                 // A zero value does not impose any limit on IO
	defaultFreeSpaceLimit          = 100 * 1024 * 1024 // 100 MB
	defaultMaxDedupePercent        = 100
	defaultMinDedupePercent        = 0
	defaultDedupePercentStep       = 25
	defaultDeletePercentOfContents = 20
	defaultActionRepeats           = 1
)

// Option field names
const (
	MaxDirDepthField             = "max-dir-depth"
	MaxFileSizeField             = "max-file-size"
	MinFileSizeField             = "min-file-size"
	MaxNumFilesPerWriteField     = "max-num-files-per-write"
	MinNumFilesPerWriteField     = "min-num-files-per-write"
	IOLimitPerWriteAction        = "io-limit-per-write"
	FreeSpaceLimitField          = "free-space-limit"
	MaxDedupePercentField        = "max-dedupe-percent"
	MinDedupePercentField        = "min-dedupe-percent"
	DedupePercentStepField       = "dedupe-percent"
	ActionRepeaterField          = "repeat-action"
	ThrowNoSpaceOnDeviceErrField = "throw-no-space-error"
	DeletePercentOfContentsField = "delete-contents-percent"
	SnapshotIDField              = "snapshot-ID"
)

func getOptAsIntOrDefault(key string, opts map[string]string, def int) int {
	if opts == nil {
		return def
	}

	if opts[key] == "" {
		return def
	}

	retInt, err := strconv.Atoi(opts[key])
	if err != nil {
		return def
	}

	return retInt
}

func defaultActionControls() map[string]string {
	ret := make(map[string]string, len(actions))

	for actionKey := range actions {
		switch actionKey {
		case RestoreIntoDataDirectoryActionKey:
			// Don't restore into data directory by default
			ret[string(actionKey)] = strconv.Itoa(0)
		default:
			ret[string(actionKey)] = strconv.Itoa(1)
		}
	}

	return ret
}

func pickActionWeighted(actionControlOpts map[string]string, actionList map[ActionKey]Action) ActionKey {
	var keepKey ActionKey

	sum := 0

	for actionName := range actionList {
		weight := getOptAsIntOrDefault(string(actionName), actionControlOpts, 0)
		if weight == 0 {
			continue
		}

		sum += weight
		if rand.Intn(sum) < weight {
			keepKey = actionName
		}
	}

	return keepKey
}

func errorIs(err, target error) bool {
	return err == target
}

func errIsNotEnoughSpace(err error) bool {
	return err == ErrCannotPerformIO || strings.Contains(err.Error(), noSpaceOnDeviceMatchStr)
}

// TODO: Remove this debug code
// when a better blacklist solution
// is designed and implemented

// ============================
// Previous method:

// func (e *Engine) getSnapIDOptOrRandLive(opts map[string]string) (snapID string, err error) {
// 	snapID = opts[SnapshotIDField]
// 	if snapID != "" {
// 		return snapID, nil
// 	}

// 	snapIDList := e.Checker.GetLiveSnapIDs()
// 	if len(snapIDList) == 0 {
// 		return "", ErrNoOp
// 	}

// 	return snapIDList[rand.Intn(len(snapIDList))], nil
// }

// ============================
// New contrived method:

var tempSnapIDBlacklist = map[string]struct{}{
	"7c4b89f4bc95c6e35ad40cff2847cb7d": struct{}{},
	"835da4a8ec20d91a3f9c3a5e0bccd140": struct{}{},
	"c1ec030e169569ba288333d8a4086622": struct{}{},
	"19629ec3aec2c60d0aa94e45fc9e7401": struct{}{},
	"ff8446c46877b5c34f26607ebddaf460": struct{}{},
	"e88dd444d978533b3ad125b31eb13d20": struct{}{},
	"47b4c5ded4163f5bbd5adf7a80503751": struct{}{},
	"3c7412546cf93ec6c4c180c4976b1ed7": struct{}{},
	"f5daa403ac5f4a20b39e773372cd5a3d": struct{}{},
	"0e78e4546383f98f8a432625695f8f66": struct{}{},
	"f2f076997068ed3cd87fda1ec188416e": struct{}{},
	"b376859ab10d8d76a392c945c6131b06": struct{}{},
	"2dd89bf90a8a70bfd0f6ec021326127c": struct{}{},
	"4a7b8e8d72edd4176cf28362f1fd8a82": struct{}{},
	"f58e054da630d2afbce79de6b2970f42": struct{}{},
	"07fc1fc12bc9592b558eb9681dc7327e": struct{}{},
	"37a0fe2a4b15059f1fa4689666ce45cb": struct{}{},
	"22a367e1722873cb4f2e59ee20b9d20a": struct{}{},
}

func isInBlacklist(snapID string) bool {
	_, ok := tempSnapIDBlacklist[snapID]
	return ok
}

func (e *Engine) getSnapIDOptOrRandLive(opts map[string]string) (snapID string, err error) {
	snapID = opts[SnapshotIDField]
	if snapID != "" {
		if isInBlacklist(snapID) {
			return "", ErrNoOp
		}

		return snapID, nil
	}

	snapIDList := e.Checker.GetLiveSnapIDs()
	if len(snapIDList) == 0 {
		return "", ErrNoOp
	}

	randSnapID := snapIDList[rand.Intn(len(snapIDList))]
	if isInBlacklist(randSnapID) {
		return "", ErrNoOp
	}

	return randSnapID, nil
}
