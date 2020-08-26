// +build darwin,amd64 linux,amd64

package robustness

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	"github.com/kopia/kopia/tests/robustness/engine"
	"github.com/kopia/kopia/tests/tools/fio"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

var eng *engine.Engine

// engServerClient is engine intialized with
// Server and Client Model
var engServerClient *engine.Engine

var engList []*engine.Engine

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
	case err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet):
		fmt.Println("Skipping robustness tests if KOPIA_EXE is not set")
		os.Exit(0)
	case err != nil:
		fmt.Printf("error on engine creation: %s\n", err.Error())
		os.Exit(1)
	}

	engServerClient, err = engine.NewEngine("")
	switch {
	case err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet):
		fmt.Println("Skipping robustness tests if KOPIA_EXE is not set")
		os.Exit(0)
	case err != nil:
		fmt.Printf("error on engine with server/client model creation: %s\n", err.Error())
		os.Exit(1)
	}

	engList, err = engine.NewEngineList("", 3)
	switch {
	case err == kopiarunner.ErrExeVariableNotSet || errors.Is(err, fio.ErrEnvNotSet):
		fmt.Println("Skipping robustness tests if KOPIA_EXE is not set")
		os.Exit(0)
	case err != nil:
		fmt.Printf("error on engine with server/client model creation: %s\n", err.Error())
		os.Exit(1)
	}

	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)
	metadataRepoPath := path.Join(*repoPathPrefix, metadataSubPath)

	// Try to reconcile metadata if it is out of sync with the repo state
	eng.Checker.RecoveryMode = true
	engServerClient.Checker.RecoveryMode = true
	for i := range engList {
		engList[i].Checker.RecoveryMode = true
	}

	// Initialize the engine, connecting it to the repositories
	err = eng.Init(context.Background(), dataRepoPath, metadataRepoPath)
	if err != nil {
		// Clean the temporary dirs from the file system, don't write out the
		// metadata, in case there was an issue loading it
		eng.CleanComponents()
		fmt.Printf("error initializing engine for S3: %s\n", err.Error())
		os.Exit(1)
	}

	// Initialize the engineWithServerClient, connecting it to the repositories
	err = engServerClient.InitWithServerClientModel(context.Background(), dataRepoPath, metadataRepoPath)
	if err != nil {
		// Clean the temporary dirs from the file system, don't write out the
		// metadata, in case there was an issue loading it
		engServerClient.CleanComponents()
		fmt.Printf("error initializing engine with server/client model for S3: %s\n", err.Error())
		os.Exit(1)
	}
	fmt.Printf("Creating the Engines List")
	err = engine.InitEngineList(engList, context.Background(), dataRepoPath, metadataRepoPath)
	if err != nil {
		for i := range engList {
			engList[i].CleanComponents()
			fmt.Printf("error initializing engine for S3: %s\n", err.Error())
			os.Exit(1)
		}
	}
	fmt.Printf("Done with creation")

	for i := range engList {
		_, err = engList[i].ExecAction(engine.RestoreIntoDataDirectoryActionKey, nil)
		if err != nil && err != engine.ErrNoOp {
			eng.Cleanup() //nolint:errcheck
			fmt.Printf("error restoring into the data directory: %s\n", err.Error())
			os.Exit(1)
		}
	}
	// Restore a random snapshot into the data directory
	_, err = eng.ExecAction(engine.RestoreIntoDataDirectoryActionKey, nil)
	if err != nil && err != engine.ErrNoOp {
		eng.Cleanup() //nolint:errcheck
		fmt.Printf("error restoring into the data directory: %s\n", err.Error())
		os.Exit(1)
	}

	// Restore a random snapshot into the data directory
	_, err = engServerClient.ExecAction(engine.RestoreIntoDataDirectoryActionKey, nil)
	if err != nil && err != engine.ErrNoOp {
		engServerClient.Cleanup() //nolint:errcheck
		fmt.Printf("error restoring into the data directory: %s\n", err.Error())
		os.Exit(1)
	}

	result := m.Run()

	err = eng.Cleanup()
	if err != nil {
		panic(err)
	}

	err = engServerClient.Cleanup()
	if err != nil {
		panic(err)
	}

	for i := range engList {
		err = engList[i].Cleanup()
		if err != nil {
			panic(err)
		}
	}

	os.Exit(result)
}
