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
func (e *Engine) ExecAction(actionKey ActionKey, opts map[string]string) error {
	if opts == nil {
		opts = make(map[string]string)
	}

	e.RunStats.ActionCounter++
	e.CumulativeStats.ActionCounter++
	log.Printf("Engine executing ACTION: name=%q actionCount=%v totActCount=%v t=%vs (%vs)", actionKey, e.RunStats.ActionCounter, e.CumulativeStats.ActionCounter, e.RunStats.getLifetimeSeconds(), e.getRuntimeSeconds())

	action := actions[actionKey]
	st := time.Now()

	logEntry := &LogEntry{
		StartTime:       st,
		EngineTimestamp: e.getTimestampS(st),
		Action:          actionKey,
		ActionOpts:      opts,
	}

	// Execute the action
	err := action.f(e, opts, logEntry)

	// If error was just a no-op, don't bother logging the action
	switch {
	case errorIs(err, ErrNoOp):
		e.RunStats.NoOpCount++
		e.CumulativeStats.NoOpCount++

		return err

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

	return err
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

	err := e.ExecAction(actionName, actionOpts[actionName])
	err = e.checkErrRecovery(err, actionOpts)
	return err
}

func (e *Engine) checkErrRecovery(incomingErr error, actionOpts ActionOpts) (outgoingErr error) {
	if incomingErr == nil {
		return nil
	}

	ctrl := actionOpts.getActionControlOpts()

	switch {
	case strings.Contains(incomingErr.Error(), noSpaceOnDeviceMatchStr) && ctrl[ThrowNoSpaceOnDeviceErrField] == "":
		// no space left on device

		restoreActionKey := RestoreIntoDataDirectoryActionKey
		outgoingErr = e.ExecAction(restoreActionKey, actionOpts[restoreActionKey])

		switch {
		case errorIs(outgoingErr, ErrNoOp):
			deleteDirActionKey := DeleteDirectoryContentsActionKey
			deleteRootOpts := map[string]string{
				MaxDirDepthField:             strconv.Itoa(0),
				DeletePercentOfContentsField: strconv.Itoa(100),
			}

			outgoingErr = e.ExecAction(deleteDirActionKey, deleteRootOpts)

			e.RunStats.DataPurgeCount++
			e.CumulativeStats.DataPurgeCount++

		case outgoingErr == nil:
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
	RestoreRandomSnapshotActionKey    ActionKey = "restore-random-snapID"
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
	f func(eng *Engine, opts map[string]string, l *LogEntry) error
}

// ActionKey refers to an action that can be executed by the engine
type ActionKey string

var actions = map[ActionKey]Action{
	SnapshotRootDirActionKey: {
		f: func(e *Engine, opts map[string]string, l *LogEntry) error {

			log.Printf("Creating snapshot of root directory %s", e.FileWriter.LocalDataDir)

			ctx := context.TODO()
			_, err := e.Checker.TakeSnapshot(ctx, e.FileWriter.LocalDataDir)

			setLogEntryCmdOpts(l, map[string]string{"snap-dir": e.FileWriter.LocalDataDir})

			return err
		},
	},
	RestoreRandomSnapshotActionKey: {
		f: func(e *Engine, opts map[string]string, l *LogEntry) error {

			snapIDList := e.Checker.GetLiveSnapIDs()
			if len(snapIDList) == 0 {
				return ErrNoOp
			}

			ctx := context.TODO()
			snapID := snapIDList[rand.Intn(len(snapIDList))]

			setLogEntryCmdOpts(l, map[string]string{"snapID": snapID})

			log.Printf("Restoring snapshot %s", snapID)

			err := e.Checker.RestoreSnapshot(ctx, snapID, nil)
			return err
		},
	},
	DeleteRandomSnapshotActionKey: {
		f: func(e *Engine, opts map[string]string, l *LogEntry) error {

			snapIDList := e.Checker.GetLiveSnapIDs()
			if len(snapIDList) == 0 {
				return ErrNoOp
			}

			ctx := context.TODO()
			snapID := snapIDList[rand.Intn(len(snapIDList))]

			log.Printf("Deleting snapshot %s", snapID)

			setLogEntryCmdOpts(l, map[string]string{"snapID": snapID})

			err := e.Checker.DeleteSnapshot(ctx, snapID)
			return err
		},
	},
	WriteRandomFilesActionKey: {
		f: func(e *Engine, opts map[string]string, l *LogEntry) error {

			// Directory depth
			maxDirDepth := getOptAsIntOrDefault(MaxDirDepthField, opts, defaultMaxDirDepth)
			dirDepth := rand.Intn(maxDirDepth)

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
					return err
				}
				log.Printf("Free Space %v B, limit %v B, ioLimit %v B\n", freeSpaceB, freeSpaceLimitB, ioLimit)

				if int(freeSpaceB)-ioLimit < freeSpaceLimitB {
					ioLimit = int(freeSpaceB) - freeSpaceLimitB
					log.Printf("Cutting down I/O limit for space %v", ioLimit)
					if ioLimit <= 0 {
						return ErrCannotPerformIO
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

			return e.FileWriter.WriteFilesAtDepthRandomBranch(relBasePath, dirDepth, fioOpts)
		},
	},
	DeleteRandomSubdirectoryActionKey: {
		f: func(e *Engine, opts map[string]string, l *LogEntry) error {
			maxDirDepth := getOptAsIntOrDefault(MaxDirDepthField, opts, defaultMaxDirDepth)
			if maxDirDepth <= 0 {
				return fmt.Errorf("invalid option setting: %s=%v", MaxDirDepthField, maxDirDepth)
			}
			dirDepth := rand.Intn(maxDirDepth) + 1

			log.Printf("Deleting directory at depth %v\n", dirDepth)

			setLogEntryCmdOpts(l, map[string]string{"dirDepth": strconv.Itoa(dirDepth)})

			err := e.FileWriter.DeleteDirAtDepth("", dirDepth)
			if err != nil && err == fio.ErrNoDirFound {
				log.Print(err)
				return nil
			}

			return err
		},
	},
	DeleteDirectoryContentsActionKey: {
		f: func(e *Engine, opts map[string]string, l *LogEntry) error {
			maxDirDepth := getOptAsIntOrDefault(MaxDirDepthField, opts, defaultMaxDirDepth)
			dirDepth := rand.Intn(maxDirDepth + 1)

			pcnt := getOptAsIntOrDefault(DeletePercentOfContentsField, opts, defaultDeletePercentOfContents)

			log.Printf("Deleting %d%% of directory contents at depth %v\n", pcnt, dirDepth)

			setLogEntryCmdOpts(l, map[string]string{
				"dirDepth": strconv.Itoa(dirDepth),
				"percent":  strconv.Itoa(pcnt),
			})

			err := e.FileWriter.DeleteContentsAtDepth("", dirDepth, pcnt)

			return err
		},
	},
	RestoreIntoDataDirectoryActionKey: {
		f: func(e *Engine, opts map[string]string, l *LogEntry) error {
			snapIDs := e.Checker.GetLiveSnapIDs()
			if len(snapIDs) == 0 {
				return ErrNoOp
			}

			randSnapID := snapIDs[rand.Intn(len(snapIDs))]

			log.Printf("Restoring snap ID %v into data directory\n", randSnapID)

			setLogEntryCmdOpts(l, map[string]string{"snapID": randSnapID})

			b := &bytes.Buffer{}
			err := e.Checker.RestoreSnapshotToPath(context.Background(), randSnapID, e.FileWriter.LocalDataDir, b)
			if err != nil {
				log.Print(b.String())
				return err
			}

			return nil
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
	ThrowNoSpaceOnDeviceErrField = "throw-no-space-error"
	DeletePercentOfContentsField = "delete-contents-percent"
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
	if err == target {
		return true
	}
	return false
}
