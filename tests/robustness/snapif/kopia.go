package snapif

import (
	"os"
	"regexp"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

type KopiaSnapshotter struct {
	e testenv.CLITest
	t *testing.T
}

func NewKopiaSnapshotter(t *testing.T) *KopiaSnapshotter {
	exe := os.Getenv("KOPIA_EXE")
	if exe == "" {
		// Exe = "kopia"
		t.Skip()
	}

	return &KopiaSnapshotter{
		e: testenv.CLITest{
			Exe: exe,
		},
		t: t,
	}
}

func (ks *KopiaSnapshotter) TakeSnapshot(sourceDir string) (snapID string, err error) {
	_, errOut, err := ks.e.Run(ks.t, "snapshot", "create", sourceDir)
	if err != nil {
		return "", err
	}
	snapID = parseSnapID(ks.t, errOut)
	return snapID, nil
}

func (ks *KopiaSnapshotter) RestoreSnapshot(snapID string, restoreDir string) (err error) {
	_, _, err = ks.e.Run(ks.t, "snapshot", "restore", snapID, restoreDir)
	return err
}

func parseSnapID(t *testing.T, lines []string) string {
	pattern := regexp.MustCompile(`uploaded snapshot ([\S]+)`)

	for _, l := range lines {
		match := pattern.FindAllStringSubmatch(l, 1)
		if len(match) > 0 && len(match[0]) > 1 {
			return match[0][1]
		}
	}

	t.Fatal("Snap ID could not be parsed")

	return ""
}
