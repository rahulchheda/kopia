package robustness

import (
	"context"
	"fmt"
	"os"
	"testing"

	engine "github.com/kopia/kopia/tests/robustness/test_engine"
	kopiarun "github.com/kopia/kopia/tests/tools/kopia_runner"
)

var eng *engine.Engine

const (
	fsDataPath     = "/tmp/robustness-data"
	fsMetadataPath = "/tmp/robustness-metadata"
	s3DataPath     = "robustness-data"
	s3MetadataPath = "robustness-metadata"
)

func TestMain(m *testing.M) {
	var err error

	eng, err = engine.NewEngine()
	if err == kopiarun.ErrExeVariableNotSet {
		fmt.Println("Skipping robustness tests if KOPIA_EXE is not set")
		os.Exit(0)
	}

	switch {
	case os.Getenv(engine.S3BucketNameEnvKey) != "":
		eng.InitS3(context.Background(), s3DataPath, s3MetadataPath)
	default:
		eng.InitFilesystem(context.Background(), fsDataPath, fsMetadataPath)
	}

	result := m.Run()

	err = eng.Cleanup()
	if err != nil {
		panic(err)
	}

	os.Exit(result)
}
