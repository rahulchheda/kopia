// +build darwin,amd64 linux,amd64

// Package engine provides the framework for a snapshot repository testing engine
package engine

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/kopia/kopia/tests/robustness"
	"github.com/kopia/kopia/tests/robustness/checker"
	"github.com/kopia/kopia/tests/robustness/snapmeta"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/fswalker"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

const (
	// S3BucketNameEnvKey is the environment variable required to connect to a repo on S3.
	S3BucketNameEnvKey = "S3_BUCKET_NAME"
	// EngineModeEnvKey is the environment variable required to switch between basic and server/client model.
	EngineModeEnvKey = "ENGINE_MODE"
	// EngineModeBasic is a constant used to check the engineMode.
	EngineModeBasic = "BASIC"
	// EngineModeServer is a constant used to check the engineMode.
	EngineModeServer = "SERVER"
	// defaultAddr is used for setting the address of Kopia Server.
	defaultAddr = "localhost:51515"
	// DefaultRunnerCount is used for setting the number of concurrent clients to be spawned.
	DefaultRunnerCount = 3
)

var (
	// ErrNoOp is thrown when an action could not do anything useful.
	ErrNoOp = fmt.Errorf("no-op")
	// ErrCannotPerformIO is returned if the engine determines there is not enough space
	// to write files.
	ErrCannotPerformIO = fmt.Errorf("cannot perform i/o")
	// ErrS3BucketNameEnvUnset is the error returned when the S3BucketNameEnvKey environment variable is not set.
	ErrS3BucketNameEnvUnset = fmt.Errorf("environment variable required: %v", S3BucketNameEnvKey)
	noSpaceOnDeviceMatchStr = "no space left on device"
)

// Engine is the outer level testing framework for robustness testing.
type Engine struct {
	FileWriter      []*fio.Runner
	TestRepo        []robustness.Snapshotter
	MetaStore       robustness.Persister
	Checker         []*checker.Checker
	cleanupRoutines []func()
	baseDirPath     string
	serverCmd       *exec.Cmd

	RunStats        []Stats
	CumulativeStats []Stats
	EngineLog       []Log
	RunnerCount     int
}

// NewEngine instantiates a new Engine and returns its pointer. It is
// currently created with:
// - FIO file writer
// - Kopia test repo snapshotter
// - Kopia metadata storage repo
// - FSWalker data integrity checker.
func NewEngine(workingDir string) (*Engine, error) {
	baseDirPath, err := ioutil.TempDir(workingDir, "engine-data-")
	if err != nil {
		return nil, err
	}

	e := &Engine{
		baseDirPath:     baseDirPath,
		RunStats:        multipleRunStats(DefaultRunnerCount),
		RunnerCount:     DefaultRunnerCount,
		cleanupRoutines: make([]func(), 0),
	}

	if os.Getenv(EngineModeEnvKey) == EngineModeBasic || os.Getenv(EngineModeEnvKey) == "" {
		e.RunnerCount = 1
	}

	multipleFileWriter := make([]*fio.Runner, e.RunnerCount)
	multipleSnapshotter := make([]robustness.Snapshotter, e.RunnerCount)
	multipleChecker := make([]*checker.Checker, e.RunnerCount)

	// Fill Snapshotter interface
	kopiaSnapper, err := kopiarunner.NewMultipleKopiaSnapshotter(baseDirPath, e.RunnerCount)
	if err != nil {
		e.CleanAllComponents()
		return nil, err
	}

	// Fill the snapshot store interface
	snapStore, err := snapmeta.New(baseDirPath)
	if err != nil {
		e.CleanAllComponents()
		return nil, err
	}

	e.cleanupRoutines = append(e.cleanupRoutines, snapStore.Cleanup)

	for i := 0; i < e.RunnerCount; i++ {
		singleChecker, singleCheckerErr := checker.NewChecker(kopiaSnapper[i], snapStore, fswalker.NewWalkCompare(), baseDirPath)
		if singleCheckerErr != nil {
			e.CleanAllComponents()
			return nil, singleCheckerErr
		}

		e.cleanupRoutines = append(e.cleanupRoutines, singleChecker.Cleanup)

		fileWriter, fileWriterErr := fio.NewRunner()
		if fileWriterErr != nil {
			e.CleanAllComponents()
			return nil, fileWriterErr
		}

		e.cleanupRoutines = append(e.cleanupRoutines, fileWriter.Cleanup, kopiaSnapper[i].Cleanup)

		e.MetaStore = snapStore

		err := e.setupLogging(i)
		if err != nil {
			e.CleanAllComponents()
			return nil, err
		}

		multipleSnapshotter[i] = kopiaSnapper[i]
		multipleFileWriter[i] = fileWriter
		multipleChecker[i] = singleChecker
	}

	e.cleanupRoutines = append(e.cleanupRoutines, e.cleanUpServer)

	e.FileWriter = multipleFileWriter
	e.TestRepo = multipleSnapshotter
	e.Checker = multipleChecker

	return e, nil
}

// Cleanup cleans up after each component of the test engine.
func (e *Engine) Cleanup() error {
	// Perform a snapshot action to capture the state of the data directory
	// at the end of the run
	defer e.CleanAllComponents()
	for i := 0; i < e.RunnerCount; i++ {
		lastWriteEntry := e.EngineLog[i].FindLastThisRun(WriteRandomFilesActionKey)
		lastSnapEntry := e.EngineLog[i].FindLastThisRun(SnapshotRootDirActionKey)
		log.Printf("%s %s engineIndex=%d", lastWriteEntry, lastSnapEntry, i)
		if lastWriteEntry != nil {
			if lastSnapEntry == nil || lastSnapEntry.Idx < lastWriteEntry.Idx {
				// Only force a final snapshot if the data tree has been modified since the last snapshot
				e.ExecAction(SnapshotRootDirActionKey, make(map[string]string), i) //nolint:errcheck
			}
		}
		cleanupSummaryBuilder := new(strings.Builder)
		cleanupSummaryBuilder.WriteString("\n================\n")
		cleanupSummaryBuilder.WriteString("Cleanup Summary:\n\n")
		cleanupSummaryBuilder.WriteString(e.Stats(i))
		cleanupSummaryBuilder.WriteString("\n\n")
		cleanupSummaryBuilder.WriteString(e.EngineLog[i].StringThisRun())
		cleanupSummaryBuilder.WriteString("\n")

		log.Printf("%s engineIndex=%d", cleanupSummaryBuilder.String(), i)

		e.RunStats[i].RunTime = time.Since(e.RunStats[i].CreationTime)
		e.CumulativeStats[i].RunTime += e.RunStats[i].RunTime

		if e.MetaStore != nil {
			err := e.SaveLog(i)
			if err != nil {
				return err
			}

			err = e.SaveStats(i)
			if err != nil {
				return err
			}

			err = e.MetaStore.FlushMetadata()
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func (e *Engine) setupLogging(index int) error {
	dirPath := e.MetaStore.GetPersistDir()

	newLogPath := filepath.Join(dirPath, e.formatLogName(index))

	f, err := os.Create(newLogPath)
	if err != nil {
		return err
	}

	// Write to both stderr and persistent log file
	wrt := io.MultiWriter(os.Stderr, f)
	log.SetOutput(wrt)

	return nil
}

func (e *Engine) formatLogName(index int) string {
	st := e.RunStats[index].CreationTime
	return fmt.Sprintf("Log_%s", st.Format("2006_01_02_15_04_05"))
}

func (e *Engine) CleanAllComponents() {
	for _, f := range e.cleanupRoutines {
		if f != nil {
			f()
		}
	}

	os.RemoveAll(e.baseDirPath) //nolint:errcheck
}

// Init initializes the Engine to a repository location according to the environment setup.
// - If S3_BUCKET_NAME is set, initialize S3
// - Else initialize filesystem.
func (e *Engine) Init(ctx context.Context, testRepoPath, metaRepoPath string) error {
	bucketName := os.Getenv(S3BucketNameEnvKey)
	engineMode := os.Getenv(EngineModeEnvKey)

	switch {
	case bucketName != "" && engineMode == EngineModeBasic:
		return e.InitS3(ctx, bucketName, testRepoPath, metaRepoPath)

	case bucketName != "" && engineMode == EngineModeServer:
		return e.InitS3WithServer(ctx, bucketName, testRepoPath, metaRepoPath, defaultAddr)

	case bucketName == "" && engineMode == EngineModeServer:
		return e.InitFilesystemWithServer(ctx, testRepoPath, metaRepoPath, defaultAddr)

	default:
		return e.InitFilesystem(ctx, testRepoPath, metaRepoPath)
	}
}

// InitS3 attempts to connect to a test repo and metadata repo on S3. If connection
// is successful, the engine is populated with the metadata associated with the
// snapshot in that repo. A new repo will be created if one does not already
// exist.
func (e *Engine) InitS3(ctx context.Context, bucketName, testRepoPath, metaRepoPath string) error {
	err := e.MetaStore.ConnectOrCreateS3(bucketName, metaRepoPath)
	if err != nil {
		return err
	}

	err = e.TestRepo[0].ConnectOrCreateS3(bucketName, testRepoPath)
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

	err = e.TestRepo[0].ConnectOrCreateFilesystem(testRepoPath)
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

	if e.EngineLog == nil {
		e.EngineLog = make([]Log, e.RunnerCount)
	}

	e.CumulativeStats = make([]Stats, e.RunnerCount)

	for i := 0; i < e.RunnerCount; i++ {
		err = e.LoadStats(i)
		if err != nil {
			return err
		}

		e.CumulativeStats[i].RunCounter++

		err = e.LoadLog(i)
		if err != nil {
			return err
		}
	}

	for i := 0; i < e.RunnerCount; i++ {
		_, _, err = e.TestRepo[i].Run("policy", "set", "--global", "--keep-latest", strconv.Itoa(1<<31-1), "--compression", "s2-default")
		if err != nil {
			return err
		}
	}

	for i := 0; i < e.RunnerCount; i++ {
		if err := e.Checker[i].VerifySnapshotMetadata(); err != nil {
			return err
		}
	}

	for i := 0; i < e.RunnerCount; i++ {
		if err := e.Checker[i].VerifySnapshotMetadata(); err != nil {
			return err
		}
	}

	return nil
}

// InitS3WithServer initializes the Engine with InitS3 for use with the server/client model.
func (e *Engine) InitS3WithServer(ctx context.Context, bucketName, testRepoPath, metaRepoPath, addr string) error {
	if err := e.MetaStore.ConnectOrCreateS3(bucketName, metaRepoPath); err != nil {
		return err
	}

	cmd, fingerprint, err := e.TestRepo[0].ConnectOrCreateS3WithServer(addr, bucketName, testRepoPath)
	if err != nil {
		return err
	}

	e.serverCmd = cmd

	for i := 0; i < e.RunnerCount; i++ {
		clientArgs := []string{"--server-cert-fingerprint", fingerprint, fmt.Sprintf("--override-username=engine-%v", i), fmt.Sprintf("--override-hostname=engine-%v", i)}
		if err := e.TestRepo[i].ConnectServer(fmt.Sprintf("https://%v", defaultAddr), clientArgs...); err != nil {
			return err
		}
	}

	return e.init(ctx)
}

// InitFilesystemWithServer initializes the Engine for testing the server/client model with a local filesystem repository.
func (e *Engine) InitFilesystemWithServer(ctx context.Context, testRepoPath, metaRepoPath, addr string) error {
	if err := e.MetaStore.ConnectOrCreateFilesystem(metaRepoPath); err != nil {
		return err
	}

	cmd, fingerprint, err := e.TestRepo[0].ConnectOrCreateFilesystemWithServer(addr, testRepoPath)
	if err != nil {
		return err
	}

	e.serverCmd = cmd

	for i := 1; i < e.RunnerCount; i++ {
		clientArgs := []string{"--server-cert-fingerprint", fingerprint, fmt.Sprintf("--override-username=engine-%v", i), fmt.Sprintf("--override-hostname=engine-%v", i)}
		if err := e.TestRepo[i].ConnectServer(fmt.Sprintf("https://%v", defaultAddr), clientArgs...); err != nil {
			return err
		}
	}

	return e.init(ctx)
}

// cleanUpServer cleans up the server process.
func (e *Engine) cleanUpServer() {
	if e.serverCmd == nil {
		return
	}

	if err := e.serverCmd.Process.Signal(syscall.SIGTERM); err != nil {
		log.Println("Failed to send termination signal to kopia server process:", err)
	}
}

func multipleRunStats(count int) []Stats {
	var multipleStats []Stats

	for i := 0; i < count; i++ {
		multipleStats = append(multipleStats, Stats{
			RunCounter:     1,
			CreationTime:   time.Now(),
			PerActionStats: make(map[ActionKey]*ActionStats),
		})
	}
	return multipleStats
}
