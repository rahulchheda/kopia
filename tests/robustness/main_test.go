package robustness

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"testing"
	"time"

	engine "github.com/kopia/kopia/tests/robustness/test_engine"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

var eng *engine.Engine

const (
	dataSubPath     = "robustness-data"
	metadataSubPath = "robustness-metadata"
	defaultTestDur  = 15 * time.Minute
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
	case err == kopiarunner.ErrExeVariableNotSet:
		fmt.Println("Skipping robustness tests if KOPIA_EXE is not set")
		os.Exit(0)
	case err != nil:
		fmt.Printf("error on engine creation: %s\n", err.Error())
		os.Exit(1)
	}

	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)
	metadataRepoPath := path.Join(*repoPathPrefix, metadataSubPath)

	err = eng.Init(context.Background(), dataRepoPath, metadataRepoPath)
	if err != nil {
		fmt.Printf("error initializing engine for S3: %s\n", err.Error())
		os.Exit(1)
	}

	result := m.Run()

	err = eng.Cleanup()
	if err != nil {
		panic(err)
	}

	os.Exit(result)
}
