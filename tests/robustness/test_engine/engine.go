// Package engine provides the framework for a snapshot repository testing engine
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/kopia/kopia/tests/robustness/checker"
	"github.com/kopia/kopia/tests/robustness/snap"
	"github.com/kopia/kopia/tests/robustness/snapmeta"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/fswalker"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

const (
	// S3BucketNameEnvKey is the environment variable required to connect to a repo on S3
	S3BucketNameEnvKey = "S3_BUCKET_NAME"
)

// ErrS3BucketNameEnvUnset is the error returned when the S3BucketNameEnvKey environment variable is not set
var ErrS3BucketNameEnvUnset = fmt.Errorf("environment variable required: %v", S3BucketNameEnvKey)

// Engine is the outer level testing framework for robustness testing
type Engine struct {
	FileWriter      *fio.Runner
	TestRepo        snap.Snapshotter
	MetaStore       snapmeta.Persister
	Checker         *checker.Checker
	cleanupRoutines []func()
	baseDirPath     string

	// ActionCounter      int64
	// engineCreationTime time.Time
	// perActionStats     map[ActionKey]*ActionStats
	// dataRestoreCount   int64

	RunStats        EngineStats
	CumulativeStats EngineStats
	EngineLog       EngineLog
}

// NewEngine instantiates a new Engine and returns its pointer. It is
// currently created with:
// - FIO file writer
// - Kopia test repo snapshotter
// - Kopia metadata storage repo
// - FSWalker data integrity checker
func NewEngine(workingDir string) (*Engine, error) {
	baseDirPath, err := ioutil.TempDir(workingDir, "engine-data-")
	if err != nil {
		return nil, err
	}

	e := &Engine{
		baseDirPath: baseDirPath,
	}

	// Fill the file writer
	e.FileWriter, err = fio.NewRunner()
	if err != nil {
		e.cleanup() //nolint:errcheck
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, e.FileWriter.Cleanup)

	// Fill Snapshotter interface
	kopiaSnapper, err := kopiarunner.NewKopiaSnapshotter(baseDirPath)
	if err != nil {
		e.cleanup() //nolint:errcheck
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, kopiaSnapper.Cleanup)
	e.TestRepo = kopiaSnapper

	// Fill the snapshot store interface
	snapStore, err := snapmeta.New(baseDirPath)
	if err != nil {
		e.cleanup() //nolint:errcheck
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, snapStore.Cleanup)

	e.MetaStore = snapStore

	// Create the data integrity checker
	chk, err := checker.NewChecker(kopiaSnapper, snapStore, fswalker.NewWalkCompare(), baseDirPath)
	e.cleanupRoutines = append(e.cleanupRoutines, chk.Cleanup)

	if err != nil {
		e.cleanup() //nolint:errcheck
		return nil, err
	}

	e.Checker = chk

	e.RunStats = EngineStats{
		RunCounter:     1,
		CreationTime:   time.Now(),
		PerActionStats: make(map[ActionKey]*ActionStats),
	}

	return e, nil
}

// Cleanup cleans up after each component of the test engine
func (e *Engine) Cleanup() error {
	// Perform a snapshot action to capture the state of the data directory
	// at the end of the run
	e.ExecAction(SnapshotRootDirActionKey, make(map[string]string))

	log.Printf("Cleanup summary:\n%v", e.Stats())

	defer e.cleanup()

	if e.MetaStore != nil {
		err := e.SaveLog()
		if err != nil {
			return err
		}

		err = e.SaveStats()
		if err != nil {
			return err
		}

		return e.MetaStore.FlushMetadata()
	}

	return nil
}

func (e *Engine) cleanup() {
	for _, f := range e.cleanupRoutines {
		f()
	}

	os.RemoveAll(e.baseDirPath) //nolint:errcheck
}

// InitS3 attempts to connect to a test repo and metadata repo on S3. If connection
// is successful, the engine is populated with the metadata associated with the
// snapshot in that repo. A new repo will be created if one does not already
// exist.
func (e *Engine) InitS3(ctx context.Context, testRepoPath, metaRepoPath string) error {
	bucketName := os.Getenv(S3BucketNameEnvKey)
	if bucketName == "" {
		return ErrS3BucketNameEnvUnset
	}

	err := e.MetaStore.ConnectOrCreateS3(bucketName, metaRepoPath)
	if err != nil {
		return err
	}

	err = e.TestRepo.ConnectOrCreateS3(bucketName, testRepoPath)
	if err != nil {
		return err
	}

	return e.init(ctx)
}

func (e *Engine) init(ctx context.Context) error {
	err := e.MetaStore.LoadMetadata()
	if err != nil {
		return err
	}

	err = e.LoadStats()
	if err != nil {
		return err
	}

	e.CumulativeStats.RunCounter++

	err = e.LoadLog()
	if err != nil {
		return err
	}

	_, _, err = e.TestRepo.Run("policy", "set", "--global", "--keep-latest", strconv.Itoa(1<<31-1), "--compression", "s2-default")
	if err != nil {
		return err
	}

	err = e.Checker.VerifySnapshotMetadata()
	if err != nil {
		return err
	}

	return e.RestoreLiveSnapshotToDataDir(ctx)
}

// InitFilesystem attempts to connect to a test repo and metadata repo on the local
// filesystem. If connection is successful, the engine is populated with the
// metadata associated with the snapshot in that repo. A new repo will be created if
// one does not already exist.
func (e *Engine) InitFilesystem(ctx context.Context, testRepoPath, metaRepoPath string) error {
	err := e.MetaStore.ConnectOrCreateFilesystem(metaRepoPath)
	if err != nil {
		return err
	}

	err = e.TestRepo.ConnectOrCreateFilesystem(testRepoPath)
	if err != nil {
		return err
	}

	return e.init(ctx)
}

// RestoreLiveSnapshotToDataDir restores an existing snapshot to the data directory
// to be used as a basis for kopia commands
func (e *Engine) RestoreLiveSnapshotToDataDir(ctx context.Context) error {
	snapIDs := e.Checker.GetLiveSnapIDs()
	if len(snapIDs) > 0 {
		randSnapID := snapIDs[rand.Intn(len(snapIDs))]

		err := e.Checker.RestoreSnapshotToPath(ctx, randSnapID, e.FileWriter.LocalDataDir, os.Stdout)
		if err != nil {
			return err
		}
	}

	return nil
}

// ActionOpts is a structure that designates the options for
// picking and running an action
type ActionOpts map[ActionKey]map[string]string

// Action is a unit of functionality that can be executed by
// the engine
type Action struct {
	f func(eng *Engine, opts map[string]string, l *EngineLogEntry) error
}

// ActionKey refers to an action that can be executed by the engine
type ActionKey string

// List of action keys
const (
	ActionControlActionKey            ActionKey = "action-control"
	SnapshotRootDirActionKey          ActionKey = "snapshot-root"
	RestoreRandomSnapshotActionKey    ActionKey = "restore-random-snapID"
	DeleteRandomSnapshotActionKey     ActionKey = "delete-random-snapID"
	WriteRandomFilesActionKey         ActionKey = "write-random-files"
	DeleteRandomSubdirectoryActionKey ActionKey = "delete-random-subdirectory"
)

var actions = map[ActionKey]Action{
	SnapshotRootDirActionKey: {
		f: func(e *Engine, opts map[string]string, l *EngineLogEntry) error {

			log.Printf("Creating snapshot of root directory %s", e.FileWriter.LocalDataDir)

			ctx := context.TODO()
			_, err := e.Checker.TakeSnapshot(ctx, e.FileWriter.LocalDataDir)

			setLogEntryCmdOpts(l, map[string]string{"snap-dir": e.FileWriter.LocalDataDir})

			return err
		},
	},
	RestoreRandomSnapshotActionKey: {
		f: func(e *Engine, opts map[string]string, l *EngineLogEntry) error {

			snapIDList := e.Checker.GetLiveSnapIDs()
			if len(snapIDList) == 0 {
				return nil
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
		f: func(e *Engine, opts map[string]string, l *EngineLogEntry) error {

			snapIDList := e.Checker.GetLiveSnapIDs()
			if len(snapIDList) == 0 {
				return nil
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
		f: func(e *Engine, opts map[string]string, l *EngineLogEntry) error {

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

			err := e.FileWriter.WriteFilesAtDepthRandomBranch(relBasePath, dirDepth, fioOpts)
			if err != nil {
				if strings.Contains(err.Error(), "no space left on device") {
					log.Printf("Hit device full error - Resoring an old snapshot (%v)", err.Error())
					e.RunStats.DataRestoreCount++
					e.CumulativeStats.DataRestoreCount++
					e.RestoreLiveSnapshotToDataDir(context.Background())
					return nil
				}
				return err
			}

			return nil
		},
	},
	DeleteRandomSubdirectoryActionKey: {
		f: func(e *Engine, opts map[string]string, l *EngineLogEntry) error {
			maxDirDepth := getOptAsIntOrDefault(MaxDirDepthField, opts, defaultMaxDirDepth)
			dirDepth := rand.Intn(maxDirDepth)

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
}

func setLogEntryCmdOpts(l *EngineLogEntry, opts map[string]string) {
	if l == nil {
		return
	}

	l.CmdOpts = opts
}

// Action constants
const (
	defaultMaxDirDepth           = 20
	defaultMaxFileSize           = 1 * 1024 * 1024 * 1024 // 1GB
	defaultMinFileSize           = 4096
	defaultMaxNumFilesPerWrite   = 10000
	defaultMinNumFilesPerWrite   = 1
	defaultIOLimitPerWriteAction = 0 // A zero value does not impose any limit on IO
	defaultMaxDedupePercent      = 100
	defaultMinDedupePercent      = 0
	defaultDedupePercentStep     = 25
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

// Option field names
const (
	MaxDirDepthField         = "max-dir-depth"
	MaxFileSizeField         = "max-file-size"
	MinFileSizeField         = "min-file-size"
	MaxNumFilesPerWriteField = "max-num-files-per-write"
	MinNumFilesPerWriteField = "min-num-files-per-write"
	IOLimitPerWriteAction    = "io-limit-per-write"
	MaxDedupePercentField    = "max-dedupe-percent"
	MinDedupePercentField    = "min-dedupe-percent"
	DedupePercentStepField   = "dedupe-percent"
)

func defaultActionControls() map[string]string {
	ret := make(map[string]string, len(actions))
	for actionKey := range actions {
		ret[string(actionKey)] = strconv.Itoa(1)
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

// RandomAction executes a random action picked by the relative weights given
// in actionOpts[ActionControlActionKey], or uniform probability if that
// key is not present in the input options
func (e *Engine) RandomAction(actionOpts ActionOpts) error {
	actionControlOpts := defaultActionControls()
	if actionOpts != nil && actionOpts[ActionControlActionKey] != nil {
		actionControlOpts = actionOpts[ActionControlActionKey]
	}

	actionName := pickActionWeighted(actionControlOpts, actions)
	if string(actionName) == "" {
		return fmt.Errorf("unable to pick an action with the action control options provided")
	}

	return e.ExecAction(actionName, actionOpts[actionName])
}

// ExecAction executes the action denoted by the provided ActionKey
func (e *Engine) ExecAction(actionKey ActionKey, opts map[string]string) error {
	e.RunStats.ActionCounter++
	e.CumulativeStats.ActionCounter++
	log.Printf("Engine executing ACTION: name=%q actionCount=%v totActCount=%v t=%vs", actionKey, e.RunStats.ActionCounter, e.CumulativeStats.ActionCounter, e.RunStats.getRuntimeSeconds())

	action := actions[actionKey]
	st := time.Now()

	if e.RunStats.PerActionStats != nil && e.RunStats.PerActionStats[actionKey] == nil {
		e.RunStats.PerActionStats[actionKey] = new(ActionStats)
	}
	if e.CumulativeStats.PerActionStats != nil && e.CumulativeStats.PerActionStats[actionKey] == nil {
		e.CumulativeStats.PerActionStats[actionKey] = new(ActionStats)
	}

	defer e.RunStats.PerActionStats[actionKey].Record(st)
	defer e.CumulativeStats.PerActionStats[actionKey].Record(st)

	logEntry := &EngineLogEntry{
		StartTime:  st,
		Action:     actionKey,
		ActionOpts: opts,
	}

	err := action.f(e, opts, logEntry)

	e.EngineLog.AddCompleted(logEntry, err)

	return err
}

func (e *Engine) SaveLog() error {
	b, err := e.EngineLog.MarshalJSON()
	if err != nil {
		return err
	}

	return e.MetaStore.Store(engineLogsStoreKey, b)
}

func (e *Engine) LoadLog() error {
	b, err := e.MetaStore.Load(engineLogsStoreKey)

	err = e.EngineLog.UnmarshalJSON(b)
	if err != nil {
		if errors.Is(err, snapmeta.ErrKeyNotFound) {
			// Swallow key-not-found error. May not have historical logs
			return nil
		}
		return err
	}

	return err
}

type EngineLog struct {
	Log []EngineLogEntry
}

type EngineLogEntry struct {
	StartTime  time.Time
	EndTime    time.Time
	Action     ActionKey
	Error      error
	Idx        int64
	ActionOpts map[string]string
	CmdOpts    map[string]string
}

func (l EngineLogEntry) String() string {
	b := &strings.Builder{}

	fmt.Fprintf(b, "%4v. %s - %s (%s) %v\n",
		l.Idx,
		l.StartTime,
		l.EndTime,
		l.EndTime.Sub(l.StartTime),
		l.Action,
	)

	return b.String()
}

func (elog EngineLog) String() string {
	b := &strings.Builder{}

	for _, l := range elog.Log {
		fmt.Fprintf(b, l.String())
	}

	return b.String()
}

func (elog EngineLog) MarshalJSON() ([]byte, error) {
	return json.Marshal(&elog)
}

func (elog EngineLog) UnmarshalJSON(b []byte) error {
	return json.Unmarshal(b, &elog)
}

func (elog EngineLog) AddEntry(l EngineLogEntry) {
	elog.Log = append(elog.Log, l)
	l.Idx = int64(len(elog.Log))
}

func (elog EngineLog) AddCompleted(logEntry *EngineLogEntry, err error) {
	logEntry.EndTime = time.Now()
	logEntry.Error = err
	elog.AddEntry(*logEntry)
}

// func (elog EngineLog) RecordCompletedAction(entry *EngineLogEntry, st time.Time, act ActionKey, actOpt, cmdOpt map[string]string) {
// 	entry.StartTime =  st
// 	entry.EndTime = time.Now()
// 	entry.Action =
// 		EndTime:    time.Now(),
// 		Action:     act,
// 		ActionOpts: actOpt,
// 		CmdOpts:    cmdOpt,
// 	}

// 	elog.AddEntry(*entry)
// }

type EngineStats struct {
	RunCounter     int64
	ActionCounter  int64
	CreationTime   time.Time
	PerActionStats map[ActionKey]*ActionStats

	DataRestoreCount int64
}

const (
	engineStatsStoreKey = "cumulative-engine-stats"
	engineLogsStoreKey  = "engine-logs"
)

func (e *Engine) SaveStats() error {
	cumulStatRaw, err := json.Marshal(e.CumulativeStats)
	if err != nil {
		return err
	}

	return e.MetaStore.Store(engineStatsStoreKey, cumulStatRaw)
}

func (e *Engine) LoadStats() error {
	b, err := e.MetaStore.Load(engineStatsStoreKey)
	if err != nil {
		if errors.Is(err, snapmeta.ErrKeyNotFound) {
			// Swallow key-not-found error. We may not have historical
			// stats data. Initialize the action map for the cumulative stats
			e.CumulativeStats.PerActionStats = make(map[ActionKey]*ActionStats)
			return nil
		}
		return err
	}

	return json.Unmarshal(b, &e.CumulativeStats)
}

func (e *Engine) Stats() string {
	b := &strings.Builder{}

	fmt.Fprintln(b, "==================================")
	fmt.Fprintln(b, "Engine Action Summary (Cumulative)")
	fmt.Fprintln(b, "==================================")
	fmt.Fprint(b, e.CumulativeStats.Stats())
	fmt.Fprintln(b, "")

	fmt.Fprintln(b, "==================================")
	fmt.Fprintln(b, "Engine Action Summary (This Run)")
	fmt.Fprintln(b, "==================================")
	fmt.Fprint(b, e.RunStats.Stats())
	fmt.Fprintln(b, "")

	return b.String()
}

// Stats returns a string report of the engine's stats
func (stats *EngineStats) Stats() string {
	b := &strings.Builder{}

	fmt.Fprintln(b, "=============")
	fmt.Fprintln(b, "Stat summary")
	fmt.Fprintln(b, "=============")
	fmt.Fprintf(b, "  Number of runs:    %10vs\n", stats.RunCounter)
	fmt.Fprintf(b, "  Engine runtime:    %10vs\n", stats.getRuntimeSeconds())
	fmt.Fprintf(b, "  Actions run:        %10v\n", stats.ActionCounter)
	fmt.Fprintf(b, "  Data Dir Restores:  %10v\n", stats.ActionCounter)
	fmt.Fprintln(b, "")
	fmt.Fprintln(b, "=============")
	fmt.Fprintln(b, "Action stats")
	fmt.Fprintln(b, "=============")

	for actionKey, actionStat := range stats.PerActionStats {
		fmt.Fprintf(b, "%s:\n", actionKey)
		fmt.Fprintf(b, "  Count:            %10d\n", actionStat.Count)
		fmt.Fprintf(b, "  Avg Runtime:      %10v\n", actionStat.avgRuntimeString())
		fmt.Fprintf(b, "  Max Runtime:     %10vs\n", durationToSec(actionStat.MaxRuntime))
		fmt.Fprintf(b, "  Min Runtime:     %10vs\n", durationToSec(actionStat.MinRuntime))
		fmt.Fprintln(b, "")
	}

	return b.String()
}

func (e *EngineStats) getRuntimeSeconds() float64 {
	return durationToSec(time.Since(e.CreationTime))
}

func durationToSec(dur time.Duration) float64 {
	return dur.Round(time.Second).Seconds()
}

// ActionStats tracks runtime statistics for an action
type ActionStats struct {
	Count        int64
	TotalRuntime time.Duration
	MinRuntime   time.Duration
	MaxRuntime   time.Duration
}

// AverageRuntime returns the average run time for the action
func (s *ActionStats) AverageRuntime() time.Duration {
	return time.Duration(int64(s.TotalRuntime) / s.Count)
}

func (s *ActionStats) avgRuntimeString() string {
	if s.Count == 0 {
		return "--"
	}

	return fmt.Sprintf("%vs", durationToSec(s.AverageRuntime()))
}

// Record records the current time against the provided start time
// and updates the stats accordingly
func (s *ActionStats) Record(st time.Time) {
	thisRuntime := time.Since(st)
	s.TotalRuntime += thisRuntime

	if thisRuntime > s.MaxRuntime {
		s.MaxRuntime = thisRuntime
	}

	if s.Count == 0 || thisRuntime < s.MinRuntime {
		s.MinRuntime = thisRuntime
	}

	s.Count++
}
