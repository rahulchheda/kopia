// +build darwin,amd64 linux,amd64

package robustness

import (
	"context"
	"errors"
	"flag"
	"log"
	"os"
	"path"
	"testing"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

var eng *engine.Engine

const (
	dataSubPath     = "robustness-data"
	metadataSubPath = "robustness-metadata"
	defaultTestDur  = 5 * time.Minute
)

var (
	randomizedTestDur = flag.Duration("rand-test-duration", defaultTestDur, "Set the duration for the randomized test")
	repoPathPrefix    = flag.String("repo-path-prefix", "", "Point the robustness tests at this path prefix")
)

func TestMain(m *testing.M) {
	flag.Parse()

	var err error

	eng, err = engine.NewEngine("")

	switch {
	case errors.Is(err, kopiarunner.ErrExeVariableNotSet):
		log.Println("Skipping robustness tests because KOPIA_EXE is not set")
		os.Exit(0)
	case errors.Is(err, fio.ErrEnvNotSet):
		log.Println("Skipping robustness tests because FIO environment is not set")
		os.Exit(0)
	case err != nil:
		log.Fatalln("error on engine creation:", err)
	}

	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)
	metadataRepoPath := path.Join(*repoPathPrefix, metadataSubPath)

	// Try to reconcile metadata if it is out of sync with the repo state
	for i := 0; i < eng.RunnerCount; i++ {
		eng.Checker[i].RecoveryMode = true
	}

	// Initialize the engine, connecting it to the repositories
	err = eng.Init(context.Background(), dataRepoPath, metadataRepoPath)
	if err != nil {
		// Clean the temporary dirs from the file system, don't write out the
		// metadata, in case there was an issue loading it
		eng.CleanAllComponents()
		log.Fatalln("error initializing engine for S3:", err)
	}
	// Restore a random snapshot into the data directory
	var errs errgroup.Group

	for i := range eng.Checker {
		func(index int) {
			errs.Go(func() error {
				_, err = eng.ExecAction(engine.RestoreIntoDataDirectoryActionKey, nil, index)
				if err != nil && !errors.Is(err, engine.ErrNoOp) {
					eng.Cleanup(index)
					log.Fatalln("error restoring into the data directory:", err)
					panic(err)
				}
				return nil
			})
		}(i)
	}

	err = errs.Wait()
	if err != nil {
		panic(err)
	}

	result := m.Run()

	var errsCleaner errgroup.Group

	for i := range eng.Checker {
		func(index int) {
			errs.Go(func() error {
				err = eng.Cleanup(index)
				if err != nil {
					panic(err)
				}
				return nil
			})
		}(i)
	}

	err = errsCleaner.Wait()
	if err != nil {
		log.Printf("error cleaning up the engine: %s\n", err.Error())
		os.Exit(2)
	}

	os.Exit(result)
}
