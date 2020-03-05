// Package engine provides the framework for a snapshot repository testing engine
package engine

import (
	"context"
	"fmt"
	"io/ioutil"
	"log"
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

var (
	// ErrNoOp is thrown when an action could not do anything useful
	ErrNoOp = fmt.Errorf("no-op")
	// ErrCannotPerformIO is returned if the engine determines there is not enough space
	// to write files
	ErrCannotPerformIO = fmt.Errorf("cannot perform i/o")
	// ErrS3BucketNameEnvUnset is the error returned when the S3BucketNameEnvKey environment variable is not set
	ErrS3BucketNameEnvUnset = fmt.Errorf("environment variable required: %v", S3BucketNameEnvKey)
	noSpaceOnDeviceMatchStr = "no space left on device"
)

// Engine is the outer level testing framework for robustness testing
type Engine struct {
	FileWriter      *fio.Runner
	TestRepo        snap.Snapshotter
	MetaStore       snapmeta.Persister
	Checker         *checker.Checker
	cleanupRoutines []func()
	baseDirPath     string

	RunStats        Stats
	CumulativeStats Stats
	EngineLog       Log
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

	e.RunStats = Stats{
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

	e.RunStats.RunTime = time.Since(e.RunStats.CreationTime)
	e.CumulativeStats.RunTime += e.RunStats.RunTime

	log.Println("================")
	log.Println("Cleanup summary:")
	log.Println("")
	log.Println(e.Stats())
	log.Println("")
	log.Println(e.EngineLog.StringThisRun())

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

	return e.Checker.VerifySnapshotMetadata()
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

// Stats prints the engine stats, cumulative and from the current run
func (e *Engine) Stats() string {
	b := &strings.Builder{}

	fmt.Fprintln(b, "==================================")
	fmt.Fprintln(b, "Engine Action Summary (Cumulative)")
	fmt.Fprintln(b, "==================================")
	fmt.Fprintf(b, "  Engine runtime:   %10vs\n", e.getRuntimeSeconds())
	fmt.Fprintln(b, "")
	fmt.Fprint(b, e.CumulativeStats.Stats())
	fmt.Fprintln(b, "")

	fmt.Fprintln(b, "==================================")
	fmt.Fprintln(b, "Engine Action Summary (This Run)")
	fmt.Fprintln(b, "==================================")
	fmt.Fprint(b, e.RunStats.Stats())
	fmt.Fprintln(b, "")

	return b.String()
}

func (e *Engine) getTimestampS(t time.Time) int64 {
	return e.getRuntimeSeconds()
}

func (e *Engine) getRuntimeSeconds() int64 {
	return durationToSec(e.CumulativeStats.RunTime + time.Since(e.RunStats.CreationTime))
}

func errorIs(err, target error) bool {
	if err == target {
		return true
	}
	return false
}
