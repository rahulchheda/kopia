package robustness

import (
	"fmt"
	"os"
	"testing"

	engine "github.com/kopia/kopia/tests/robustness/test_engine"
	kopiarun "github.com/kopia/kopia/tests/tools/kopia_runner"
)

var eng *engine.Engine

func TestMain(m *testing.M) {
	var err error
	eng, err = engine.NewEngine()
	if err == kopiarun.ErrExeVariableNotSet {
		fmt.Println("Skipping robustness tests if KOPIA_EXE is not set")
	}

	defer func() {
		err := eng.Cleanup()
		if err != nil {
			panic(err)
		}
	}

	result := m.Run()

	os.Exit(result)
}
