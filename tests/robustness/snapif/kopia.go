package snapif

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"os"
	"path"
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
	if ks.Runner != nil {
		ks.Runner.Cleanup()
	}
}

func (ks *KopiaSnapshotter) CreateRepo(args ...string) (err error) {
	args = append([]string{"repo", "create"}, args...)
	_, _, err = ks.Runner.Run(args...)
	return err
}

func (ks *KopiaSnapshotter) ConnectRepo(args ...string) (err error) {
	args = append([]string{"repo", "connect"}, args...)
	_, _, err = ks.Runner.Run(args...)
	return err
}

func (ks *KopiaSnapshotter) ConnectOrCreateRepo(args ...string) error {
	err := ks.ConnectRepo(args...)
	if err == nil {
		return nil
	}

	return ks.CreateRepo(args...)
}

func (ks *KopiaSnapshotter) ConnectOrCreateS3(bucketName, pathPrefix string) error {
	path.Join(getHostPath(), pathPrefix)
	args := []string{"s3", "--bucket", bucketName, "--prefix", pathPrefix}

	return ks.ConnectOrCreateRepo(args...)
}

func getHostPath() string {
	hn, err := os.Hostname()
	if err != nil {
		return "kopia-test-1"
	}

	h := sha1.New()
	fmt.Fprintf(h, "%v", hn)

	return fmt.Sprintf("kopia-test-%x", h.Sum(nil)[0:8])
}

func (ks *KopiaSnapshotter) ConnectOrCreateFilesystem(path string) error {
	args := []string{"filesystem", "--path", path}

	return ks.ConnectOrCreateRepo(args...)
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

func (ks *KopiaSnapshotter) DeleteSnapshot(snapID string) (err error) {
	_, _, err = ks.Runner.Run("snapshot", "delete", snapID)
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
