//+build utils

package robustness

import (
	"encoding/json"
	"fmt"
	"testing"

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
