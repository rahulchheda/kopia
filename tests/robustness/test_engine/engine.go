// Package engine provides the framework for a snapshot repository testing engine
package engine

import (
	"context"
	"fmt"
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

	actionCounter      int64
	engineCreationTime time.Time
	perActionStats     map[ActionKey]*ActionStats
}

// NewEngine instantiates a new Engine and returns its pointer. It is
// currently created with:
// - FIO file writer
// - Kopia test repo snapshotter
// - Kopia metadata storage repo
// - FSWalker data integrity checker
func NewEngine() (*Engine, error) {
	e := new(Engine)

	var err error

	// Fill the file writer
	e.FileWriter, err = fio.NewRunner()
	if err != nil {
		e.Cleanup() //nolint:errcheck
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, e.FileWriter.Cleanup)

	// Fill Snapshotter interface
	kopiaSnapper, err := kopiarunner.NewKopiaSnapshotter()
	if err != nil {
		e.Cleanup() //nolint:errcheck
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, kopiaSnapper.Cleanup)
	e.TestRepo = kopiaSnapper

	// Fill the snapshot store interface
	snapStore, err := snapmeta.New()
	if err != nil {
		e.Cleanup() //nolint:errcheck
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, snapStore.Cleanup)

	e.MetaStore = snapStore

	// Create the data integrity checker
	chk, err := checker.NewChecker(kopiaSnapper, snapStore, fswalker.NewWalkCompare())
	e.cleanupRoutines = append(e.cleanupRoutines, chk.Cleanup)

	if err != nil {
		e.Cleanup() //nolint:errcheck
		return nil, err
	}

	e.Checker = chk

	e.engineCreationTime = time.Now()
	e.perActionStats = make(map[ActionKey]*ActionStats)

	return e, nil
}

// Cleanup cleans up after each component of the test engine
func (e *Engine) Cleanup() error {
	log.Printf("Cleanup summary:\n%v", e.Stats())

	defer e.cleanup()

	if e.MetaStore != nil {
		return e.MetaStore.FlushMetadata()
	}

	return nil
}

func (e *Engine) cleanup() {
	for _, f := range e.cleanupRoutines {
		f()
	}
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

	err = e.MetaStore.LoadMetadata()
	if err != nil {
		return err
	}

	err = e.TestRepo.ConnectOrCreateS3(bucketName, testRepoPath)
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

	snapIDs := e.Checker.GetLiveSnapIDs()
	if len(snapIDs) > 0 {
		randSnapID := snapIDs[rand.Intn(len(snapIDs))]

		err = e.Checker.RestoreSnapshotToPath(ctx, randSnapID, e.FileWriter.DataDir, os.Stdout)
		if err != nil {
			return err
		}
	}

	return nil
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

	err = e.MetaStore.LoadMetadata()
	if err != nil {
		return err
	}

	err = e.TestRepo.ConnectOrCreateFilesystem(testRepoPath)
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

	snapIDs := e.Checker.GetLiveSnapIDs()
	if len(snapIDs) > 0 {
		randSnapID := snapIDs[rand.Intn(len(snapIDs))]

		err = e.Checker.RestoreSnapshotToPath(ctx, randSnapID, e.FileWriter.DataDir, os.Stdout)
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
	f func(eng *Engine, opts map[string]string) error
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
	SnapshotRootDirActionKey: Action{
		f: func(e *Engine, opts map[string]string) error {

			log.Printf("Creating snapshot of root directory %s", e.FileWriter.DataDir)

			ctx := context.TODO()
			_, err := e.Checker.TakeSnapshot(ctx, e.FileWriter.DataDir)
			return err
		},
	},
	RestoreRandomSnapshotActionKey: Action{
		f: func(e *Engine, opts map[string]string) error {

			snapIDList := e.Checker.GetLiveSnapIDs()
			if len(snapIDList) <= 0 {
				return nil
			}

			ctx := context.TODO()
			snapID := snapIDList[rand.Intn(len(snapIDList))]

			log.Printf("Restoring snapshot %s", snapID)

			err := e.Checker.RestoreSnapshot(ctx, snapID, nil)
			return err
		},
	},
	DeleteRandomSnapshotActionKey: Action{
		f: func(e *Engine, opts map[string]string) error {

			snapIDList := e.Checker.GetLiveSnapIDs()
			if len(snapIDList) <= 0 {
				return nil
			}

			ctx := context.TODO()
			snapID := snapIDList[rand.Intn(len(snapIDList))]

			log.Printf("Deleting snapshot %s", snapID)

			err := e.Checker.DeleteSnapshot(ctx, snapID)
			return err
		},
	},
	WriteRandomFilesActionKey: Action{
		f: func(e *Engine, opts map[string]string) error {

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

			log.Printf("Writing files at depth %v (fileSize: %v-%v, numFiles: %v, blockSize: %v, dedupPcnt: %v, ioLimit: %v)\n", dirDepth, minFileSizeB, maxFileSizeB, numFiles, blockSize, dedupPcnt, ioLimit)

			err := e.FileWriter.WriteFilesAtDepthRandomBranch(".", dirDepth, fioOpts)
			return err
		},
	},
	DeleteRandomSubdirectoryActionKey: Action{
		f: func(e *Engine, opts map[string]string) error {
			maxDirDepth := getOptAsIntOrDefault(MaxDirDepthField, opts, defaultMaxDirDepth)
			dirDepth := rand.Intn(maxDirDepth)

			log.Printf("Deleting directory at depth %v\n", dirDepth)

			err := e.FileWriter.DeleteDirAtDepth("", dirDepth)
			if err != nil && err == fio.ErrNoDirFound {
				log.Print(err)
				return nil
			}

			return err
		},
	},
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

func pickActionWeightedOld(actionControlOpts map[string]string) ActionKey {
	sum := 0
	intervals := []struct {
		boundEnd   int
		actionName ActionKey
	}{}

	for actionName, weightStr := range actionControlOpts {
		actionKey := ActionKey(actionName)
		if _, ok := actions[actionKey]; !ok {
			// Skip if this doesn't correspond to an action
			continue
		}

		weight, err := strconv.Atoi(weightStr)
		if err != nil {
			weight = 1
		}

		sum += weight

		intervals = append(intervals,
			struct {
				boundEnd   int
				actionName ActionKey
			}{
				boundEnd:   sum,
				actionName: actionKey,
			})
	}

	randVal := rand.Intn(sum)

	for _, actionInfo := range intervals {
		if randVal < actionInfo.boundEnd {
			return actionInfo.actionName
		}
	}

	return ActionKey("")
}

func pickActionWeighted(actionControlOpts map[string]string, actionList map[ActionKey]Action) ActionKey {
	sum := 0
	var keepKey ActionKey
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
	e.actionCounter++
	log.Printf("Engine executing ACTION: name=%q actionCount=%v t=%vs", actionKey, e.actionCounter, e.getRuntimeSeconds())

	action := actions[actionKey]
	st := time.Now()
	if e.perActionStats[actionKey] == nil {
		e.perActionStats[actionKey] = new(ActionStats)
	}

	defer e.perActionStats[actionKey].Record(st)

	return action.f(e, opts)
}

// Stats returns a string report of the engine's stats
func (e *Engine) Stats() string {
	b := &strings.Builder{}

	fmt.Fprintln(b, "===============================")
	fmt.Fprintln(b, "Engine Action Summary")
	fmt.Fprintln(b, "===============================")
	fmt.Fprintf(b, "Engine runtime:  %10vs\n", e.getRuntimeSeconds())
	fmt.Fprintf(b, "Actions run:      %10v\n", e.actionCounter)
	fmt.Fprintln(b, "")
	fmt.Fprintln(b, "=============")
	fmt.Fprintln(b, "Action stats")
	fmt.Fprintln(b, "=============")

	for actionKey, actionStat := range e.perActionStats {
		fmt.Fprintf(b, "%s:\n", actionKey)
		fmt.Fprintf(b, "  Count:          %10d\n", actionStat.Count())
		fmt.Fprintf(b, "  Avg Runtime:    %10v\n", actionStat.avgRuntimeString())
		fmt.Fprintf(b, "  Max Runtime:   %10vs\n", durationToSec(actionStat.MaxRuntime()))
		fmt.Fprintf(b, "  Min Runtime:   %10vs\n", durationToSec(actionStat.MinRuntime()))
		fmt.Fprintln(b, "")
	}
	return b.String()
}

func (e *Engine) getRuntimeSeconds() float64 {
	return durationToSec(time.Since(e.engineCreationTime))
}

func durationToSec(dur time.Duration) float64 {
	return dur.Round(time.Second).Seconds()
}

// ActionStats tracks runtime statistics for an action
type ActionStats struct {
	count        int64
	totalRuntime time.Duration
	minRuntime   time.Duration
	maxRuntime   time.Duration
}

// Count returns the number of time this action was executed
func (s *ActionStats) Count() int64 {
	return s.count
}

// AverageRuntime returns the average run time for the action
func (s *ActionStats) AverageRuntime() time.Duration {
	return time.Duration(int64(s.totalRuntime) / s.count)
}

func (s *ActionStats) avgRuntimeString() string {
	if s.count == 0 {
		return "--"
	}
	return fmt.Sprintf("%vs", durationToSec(s.AverageRuntime()))
}

// MaxRuntime returns the maximum run time for the action
func (s *ActionStats) MaxRuntime() time.Duration {
	return s.maxRuntime
}

// MinRuntime returns the minimum run time for the action
func (s *ActionStats) MinRuntime() time.Duration {
	return s.minRuntime
}

// Record records the current time against the provided start time
// and updates the stats accordingly
func (s *ActionStats) Record(st time.Time) {
	thisRuntime := time.Since(st)
	s.totalRuntime += thisRuntime

	if thisRuntime > s.maxRuntime {
		s.maxRuntime = thisRuntime
	}

	if s.count == 0 || thisRuntime < s.minRuntime {
		s.minRuntime = thisRuntime
	}

	s.count++
}
