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

var _ Snapshotter = &KopiaSnapshotter{}

// KopiaSnapshotter implements the Snapshotter interface using Kopia commands
type KopiaSnapshotter struct {
	Runner *kopiarun.Runner
}

// NewKopiaSnapshotter instantiates a new KopiaSnapshotter and returns its pointer
func NewKopiaSnapshotter() (*KopiaSnapshotter, error) {
	runner, err := kopiarun.NewRunner()
	if err != nil {
		return nil, err
	}

	return &KopiaSnapshotter{
		Runner: runner,
	}, nil
}

// Cleanup cleans up the kopia Runner
func (ks *KopiaSnapshotter) Cleanup() {
	if ks.Runner != nil {
		ks.Runner.Cleanup()
	}
}

// CreateRepo creates a kopia repository with the provided arguments
func (ks *KopiaSnapshotter) CreateRepo(args ...string) (err error) {
	args = append([]string{"repo", "create"}, args...)
	_, _, err = ks.Runner.Run(args...)

	return err
}

// ConnectRepo connects to the repository described by the provided arguments
func (ks *KopiaSnapshotter) ConnectRepo(args ...string) (err error) {
	args = append([]string{"repo", "connect"}, args...)
	_, _, err = ks.Runner.Run(args...)

	return err
}

// ConnectOrCreateRepo attempts to connect to a repo described by the provided
// arguments, and attempts to create it if connection was unsuccessful.
func (ks *KopiaSnapshotter) ConnectOrCreateRepo(args ...string) error {
	err := ks.ConnectRepo(args...)
	if err == nil {
		return nil
	}

	return ks.CreateRepo(args...)
}

// ConnectOrCreateS3 attempts to connect to a kopia repo in the s3 bucket identified
// by the provided bucketName, at the provided path prefix. It will attempt to
// create one there if connection was unsuccessful.
func (ks *KopiaSnapshotter) ConnectOrCreateS3(bucketName, pathPrefix string) error {
	path.Join(getHostPath(), pathPrefix)
	args := []string{"s3", "--bucket", bucketName, "--prefix", pathPrefix}

	return ks.ConnectOrCreateRepo(args...)
}

// ConnectOrCreateFilesystem attempts to connect to a kopia repo in the local
// filesystem at the path provided. It will attempt to create one there if
// connection was unsuccessful.
func (ks *KopiaSnapshotter) ConnectOrCreateFilesystem(repoPath string) error {
	args := []string{"filesystem", "--path", repoPath}

	return ks.ConnectOrCreateRepo(args...)
}

// TakeSnapshot implements the Snapshotter interface, issues a kopia snapshot
// create command on the provided source path.
func (ks *KopiaSnapshotter) TakeSnapshot(source string) (snapID string, err error) {
	_, errOut, err := ks.Runner.Run("snapshot", "create", source)
	if err != nil {
		return "", err
	}

	return parseSnapID(strings.Split(errOut, "\n"))
}

// RestoreSnapshot implements the Snapshotter interface, issues a kopia snapshot
// restore command of the provided snapshot ID to the provided restore destination
func (ks *KopiaSnapshotter) RestoreSnapshot(snapID, restoreDir string) (err error) {
	_, _, err = ks.Runner.Run("snapshot", "restore", snapID, restoreDir)
	return err
}

// DeleteSnapshot implements the Snapshotter interface, issues a kopia snapshot
// delete of the provided snapshot ID
func (ks *KopiaSnapshotter) DeleteSnapshot(snapID string) (err error) {
	_, _, err = ks.Runner.Run("snapshot", "delete", snapID, "--unsafe-ignore-source")
	return err
}

// ListSnapshots implements the Snapshotter interface, issues a kopia snapshot
// list and parses the snapshot IDs
func (ks *KopiaSnapshotter) ListSnapshots() ([]string, error) {
	stdout, _, err := ks.Runner.Run("manifest", "list")
	if err != nil {
		return nil, err
	}
	return parseListForSnapshotIDs(stdout), nil
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

func parseSnapID(lines []string) (string, error) {
	pattern := regexp.MustCompile(`uploaded snapshot ([\S]+)`)

	for _, l := range lines {
		match := pattern.FindAllStringSubmatch(l, 1)
		if len(match) > 0 && len(match[0]) > 1 {
			return match[0][1], nil
		}
	}

	return "", errors.New("snap ID could not be parsed")
}

func parseListForSnapshotIDs(output string) []string {
	lines := strings.Split(output, "\n")

	var ret []string
	for _, l := range lines {
		fields := strings.Fields(l)
		if len(fields) > 5 {
			if fields[5] == "type:snapshot" {
				ret = append(ret, fields[0])
			}
		}
	}

	return ret
}
