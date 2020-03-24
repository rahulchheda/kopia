//+build utils

package utils

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path"
	"testing"

	engine "github.com/kopia/kopia/tests/robustness/test_engine"
)

var eng *engine.Engine

const (
	dataSubPath     = "robustness-data"
	metadataSubPath = "robustness-metadata"
)

var (
	repoPathPrefix = flag.String("repo-path-prefix", "", "Point the robustness tests at this path prefix")
)

func TestMain(m *testing.M) {
	flag.Parse()

	var err error

	eng, err = engine.NewEngine("")
	if err != nil {
		eng.Cleanup() //nolint:errcheck
		fmt.Printf("error on engine creation: %s\n", err.Error())
		os.Exit(1)
	}

	dataRepoPath := path.Join(*repoPathPrefix, dataSubPath)
	metadataRepoPath := path.Join(*repoPathPrefix, metadataSubPath)

	// Initialize the engine, connecting it to the repositories
	err = eng.Init(context.Background(), dataRepoPath, metadataRepoPath)
	if err != nil {
		eng.Cleanup() //nolint:errcheck
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
