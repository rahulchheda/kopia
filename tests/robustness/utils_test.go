//+build utils

package robustness

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestLogString(t *testing.T) {
	fmt.Println(eng.EngineLog.String())
}

func TestLogJSON(t *testing.T) {
	b, err := json.MarshalIndent(eng.EngineLog, "", "   ")
	testenv.AssertNoError(t, err)

	fmt.Println(b)
}

func TestStats(t *testing.T) {
	fmt.Println(eng.Stats())
}
