//+build utils

package robustness

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/kopia/kopia/tests/robustness/checker"
	engine "github.com/kopia/kopia/tests/robustness/test_engine"
	"github.com/kopia/kopia/tests/testenv"
)

func TestRobustnessStatusLogString(t *testing.T) {
	fmt.Println(eng.EngineLog.String())
}

func TestRobustnessStatusLogJSON(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping full json log output")
	}
	b, err := json.MarshalIndent(eng.EngineLog, "", "   ")
	testenv.AssertNoError(t, err)

	fmt.Println(b)
}

func TestRobustnessStatusStats(t *testing.T) {
	fmt.Println(eng.Stats())
}

func TestRobustnessStatusSpecificSnapshotRestores(t *testing.T) {
	eng.ReadOnly = true
	eng.Checker.RecoveryMode = checker.IgnoreErrorRecoveryMode

	snaps := []string{
		"835da4a8ec20d91a3f9c3a5e0bccd140",
		"7c4b89f4bc95c6e35ad40cff2847cb7d",
		"c1ec030e169569ba288333d8a4086622",
	}

	for _, snap := range snaps {
		opts := map[string]string{
			engine.SnapshotIDField: snap,
		}

		_, err := eng.ExecAction(engine.RestoreSnapshotActionKey, opts)
		if err != nil {
			fmt.Println("ERROR RETURNED IS:", err)
		}
	}
}
