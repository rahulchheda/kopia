package robustness

import (
	"context"
	"flag"
	"fmt"
	"os"
	"testing"
	"time"

	engine "github.com/kopia/kopia/tests/robustness/test_engine"
	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

var eng *engine.Engine

const (
	fsDataPath     = "/tmp/robustness-data"
	fsMetadataPath = "/tmp/robustness-metadata"
	s3DataPath     = "robustness-data/"
	s3MetadataPath = "robustness-metadata/"
	defaultTestDur = 3 * time.Minute
)

var (
	randomizedTestDur = flag.Duration("rand-test-duration", defaultTestDur, "Set the duration for the randomized test")
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
		fmt.Printf("error on engine creation: %s", err.Error())
		os.Exit(1)
	}

	switch {
	case os.Getenv(engine.S3BucketNameEnvKey) != "":
		err = eng.InitS3(context.Background(), s3DataPath, s3MetadataPath)
		if err != nil {
			fmt.Printf("error initializing engine for S3: %s", err.Error())
			os.Exit(1)
		}
	default:
		err = eng.InitFilesystem(context.Background(), fsDataPath, fsMetadataPath)
		if err != nil {
			fmt.Printf("error initializing engine for filesystem: %s", err.Error())
			os.Exit(1)
		}
	}

	result := m.Run()

	err = eng.Cleanup()
	if err != nil {
		panic(err)
	}

	os.Exit(result)
}
