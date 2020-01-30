package snapif

import (
	"errors"
	"regexp"
	"strings"

	kopiarun "github.com/kopia/kopia/tests/tools/kopia_runner"
)

type KopiaSnapshotter struct {
	RepoDir string
	Runner  *kopiarun.Runner
}

func NewKopiaSnapshotter() (*KopiaSnapshotter, error) {
	runner, err := kopiarun.NewRunner()
	if err != nil {
		return nil, err
	}

	return &KopiaSnapshotter{
		Runner: runner,
	}, nil
}

func (ks *KopiaSnapshotter) Cleanup() {
	ks.Runner.Cleanup()
}

func (ks *KopiaSnapshotter) CreateRepo(args ...string) (err error) {
	args = append([]string{"repo", "create"}, args...)
	_, _, err = ks.Runner.Run(args...)
	return err
}

func (ks *KopiaSnapshotter) TakeSnapshot(sourceDir string) (snapID string, err error) {
	_, errOut, err := ks.Runner.Run("snapshot", "create", sourceDir)
	if err != nil {
		return "", err
	}
	return parseSnapID(strings.Split(errOut, "\n"))
}

func (ks *KopiaSnapshotter) RestoreSnapshot(snapID string, restoreDir string) (err error) {
	_, _, err = ks.Runner.Run("snapshot", "restore", snapID, restoreDir)
	return err
}

func parseSnapID(lines []string) (string, error) {
	pattern := regexp.MustCompile(`uploaded snapshot ([\S]+)`)

	for _, l := range lines {
		match := pattern.FindAllStringSubmatch(l, 1)
		if len(match) > 0 && len(match[0]) > 1 {
			return match[0][1], nil
		}
	}

	return "", errors.New("Snap ID could not be parsed")
}
