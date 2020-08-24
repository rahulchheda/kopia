package kopiarunner

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/internal/retry"
	"github.com/kopia/kopia/tests/robustness/snap"
)

var _ snap.Snapshotter = &KopiaSnapshotter{}

const (
	contentCacheSizeMBFlag  = "--content-cache-size-mb"
	metadataCacheSizeMBFlag = "--metadata-cache-size-mb"
	noCheckForUpdatesFlag   = "--no-check-for-updates"
	noProgressFlag          = "--no-progress"
	parallelFlag            = "--parallel"
)

// Flag value settings
var (
	contentCacheSizeSettingMB  = strconv.Itoa(500)
	metadataCacheSizeSettingMB = strconv.Itoa(500)
	parallelSetting            = strconv.Itoa(8)
)

// KopiaSnapshotter implements the Snapshotter interface using Kopia commands
type KopiaSnapshotter struct {
	Runner *Runner
}

// NewKopiaSnapshotter instantiates a new KopiaSnapshotter and returns its pointer
func NewKopiaSnapshotter(baseDir string) (*KopiaSnapshotter, error) {
	runner, err := NewRunner(baseDir)
	if err != nil {
		return nil, err
	}

	return &KopiaSnapshotter{
		Runner: runner,
	}, nil
}

// Cleanup cleans up the kopia Runner.
func (ks *KopiaSnapshotter) Cleanup() {
	if ks.Runner != nil {
		ks.Runner.Cleanup()
	}
}

func (ks *KopiaSnapshotter) repoConnectCreate(op string, args ...string) error {
	args = append([]string{"repo", op}, args...)

	args = append(args,
		contentCacheSizeMBFlag, contentCacheSizeSettingMB,
		metadataCacheSizeMBFlag, metadataCacheSizeSettingMB,
		noCheckForUpdatesFlag,
	)

	_, _, err := ks.Runner.Run(args...)

	return err
}

// CreateRepo creates a kopia repository with the provided arguments
func (ks *KopiaSnapshotter) CreateRepo(args ...string) (err error) {
	return ks.repoConnectCreate("create", args...)
}

// ConnectRepo connects to the repository described by the provided arguments
func (ks *KopiaSnapshotter) ConnectRepo(args ...string) (err error) {
	return ks.repoConnectCreate("connect", args...)
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
	args := []string{"s3", "--bucket", bucketName, "--prefix", pathPrefix}

	return ks.ConnectOrCreateRepo(args...)
}

// ConnectOrCreateS3WithServer attempts to connect or create S3 bucket, but with Client/Server Model
func (ks *KopiaSnapshotter) ConnectOrCreateS3WithServer(serverAddr, bucketName, pathPrefix string) error {
	args := []string{"s3", "--bucket", bucketName, "--prefix", pathPrefix}

	if err := ks.ConnectOrCreateRepo(args...); err != nil {
		return err
	}

	if err := ks.CreateServer(serverAddr); err != nil {
		fmt.Printf("Error in Creating Server: %v\n", err)
		return err
	}

	if err := ks.ConnectServer(serverAddr); err != nil {
		fmt.Printf("Error in Connecting Server: %v\n", err)

		return err
	}

	return nil

}

func (ks *KopiaSnapshotter) ConnectS3WithDeployedServer(serverAddr, bucketName, pathPrefix string) error {

	if err := ks.ConnectServer(serverAddr); err != nil {
		fmt.Printf("Error in Connecting Server: %v\n", err)

		return err
	}

	return nil

}

// ConnectOrCreateFilesystem attempts to connect to a kopia repo in the local
// filesystem at the path provided. It will attempt to create one there if
// connection was unsuccessful.
func (ks *KopiaSnapshotter) ConnectOrCreateFilesystem(repoPath string) error {
	args := []string{"filesystem", "--path", repoPath}

	return ks.ConnectOrCreateRepo(args...)
}

// ConnectOrCreateFilesystemWithServer attempts to connect or create repo in locak filesystem,
// but with Client/Server Model
func (ks *KopiaSnapshotter) ConnectOrCreateFilesystemWithServer(repoPath, serverAddr string) error {
	args := []string{"filesystem", "--path", repoPath}

	if err := ks.ConnectOrCreateRepo(args...); err != nil {
		return err
	}

	if err := ks.CreateServer(serverAddr); err != nil {
		fmt.Printf("Error in Creating Server: %v\n", err)
		return err
	}

	if err := ks.ConnectServer(serverAddr); err != nil {
		fmt.Printf("Error in Connecting Server: %v\n", err)

		return err
	}

	return nil
}

// CreateSnapshot implements the Snapshotter interface, issues a kopia snapshot
// create command on the provided source path.
func (ks *KopiaSnapshotter) CreateSnapshot(source string) (snapID string, err error) {
	_, errOut, err := ks.Runner.Run("snapshot", "create", parallelFlag, parallelSetting, noProgressFlag, source)
	if err != nil {
		return "", err
	}

	return parseSnapID(strings.Split(errOut, "\n"))
}

// RestoreSnapshot implements the Snapshotter interface, issues a kopia snapshot
// restore command of the provided snapshot ID to the provided restore destination.
func (ks *KopiaSnapshotter) RestoreSnapshot(snapID, restoreDir string) (err error) {
	_, _, err = ks.Runner.Run("snapshot", "restore", snapID, restoreDir)
	return err
}

// DeleteSnapshot implements the Snapshotter interface, issues a kopia snapshot
// delete of the provided snapshot ID.
func (ks *KopiaSnapshotter) DeleteSnapshot(snapID string) (err error) {
	_, _, err = ks.Runner.Run("snapshot", "delete", snapID, "--delete")
	return err
}

// RunGC implements the Snapshotter interface, issues a gc command to the kopia repo
func (ks *KopiaSnapshotter) RunGC() (err error) {
	_, _, err = ks.Runner.Run("snapshot", "gc")
	return err
}

// ListSnapshots implements the Snapshotter interface, issues a kopia snapshot
// list and parses the snapshot IDs.
func (ks *KopiaSnapshotter) ListSnapshots() ([]string, error) {
	snapIDListMan, err := ks.snapIDsFromManifestList()
	if err != nil {
		return nil, err
	}

	// Validate the list against kopia snapshot list --all
	snapIDListSnap, err := ks.snapIDsFromSnapListAll()
	if err != nil {
		return nil, err
	}

	if got, want := len(snapIDListSnap), len(snapIDListMan); got != want {
		return nil, errors.Errorf("Snapshot list len (%d) does not match manifest list len (%d)", got, want)
	}

	return snapIDListMan, nil
}

func (ks *KopiaSnapshotter) snapIDsFromManifestList() ([]string, error) {
	stdout, _, err := ks.Runner.Run("manifest", "list")
	if err != nil {
		return nil, errors.Wrap(err, "failure during kopia manifest list")
	}

	return parseManifestListForSnapshotIDs(stdout), nil
}

func (ks *KopiaSnapshotter) snapIDsFromSnapListAll() ([]string, error) {
	// Validate the list against kopia snapshot list --all
	stdout, _, err := ks.Runner.Run("snapshot", "list", "--all", "--manifest-id", "--show-identical")
	if err != nil {
		return nil, errors.Wrap(err, "failure during kopia snapshot list")
	}

	return parseSnapshotListForSnapshotIDs(stdout), nil
}

// Run implements the Snapshotter interface, issues an arbitrary kopia command and returns
// the output.
func (ks *KopiaSnapshotter) Run(args ...string) (stdout, stderr string, err error) {
	return ks.Runner.Run(args...)
}

func (ks *KopiaSnapshotter) CreateServer(addr string, args ...string) error {
	args = append([]string{"server", "start", fmt.Sprintf("--address=%s", addr)}, args...)
	fmt.Printf("Command for Creating to server: %v\n", args)

	_, _, err := ks.Runner.RunServer(args...)
	if err != nil {
		return err
	}
	statusArgs := append([]string{"server", "status", fmt.Sprintf("--address=http://%s", addr)})
	err = waitUntilServerStarted(ks, context.TODO(), statusArgs...)
	if err != nil {
		return err
	}
	return nil
}

func (ks *KopiaSnapshotter) ConnectServer(addr string, args ...string) error {
	args = append([]string{"repo", "connect", "server", fmt.Sprintf("--url=http://%s", addr)}, args...)
	fmt.Printf("Command for Connecting to server: %v\n", args)
	_, _, err := ks.Runner.Run(args...)

	return err
}

func parseSnapID(lines []string) (string, error) {
	pattern := regexp.MustCompile(`Created snapshot with root [\S]+ and ID ([\S]+)`)

	for _, l := range lines {
		match := pattern.FindAllStringSubmatch(l, 1)
		if len(match) > 0 && len(match[0]) > 1 {
			return match[0][1], nil
		}
	}

	return "", errors.New("snap ID could not be parsed")
}

func parseSnapshotListForSnapshotIDs(output string) []string {
	var ret []string

	lines := strings.Split(output, "\n")
	for _, l := range lines {
		fields := strings.Fields(l)

		for _, f := range fields {
			spl := strings.Split(f, "manifest:")
			if len(spl) == 2 {
				ret = append(ret, spl[1])
			}
		}
	}

	return ret
}

func parseManifestListForSnapshotIDs(output string) []string {
	var ret []string

	lines := strings.Split(output, "\n")
	for _, l := range lines {
		fields := strings.Fields(l)

		typeFieldIdx := 5
		if len(fields) > typeFieldIdx {
			if fields[typeFieldIdx] == "type:snapshot" {
				ret = append(ret, fields[0])
			}
		}
	}

	return ret
}

// waitForServerReady returns error if the Kopia API server fails to start before timeout
func waitUntilServerStarted(ks *KopiaSnapshotter, ctx context.Context, args ...string) error {
	if err := retry.PeriodicallyNoValue(ctx, 1*time.Second, 180, "wait for server start", func() error {
		stdout, stderr, err := ks.Runner.Run(args...)
		if err != nil && stderr == "" {
			return errors.New(fmt.Sprintf("Server status: %s-%s", stdout, stderr))
		}
		return err
	}, retry.Always); err != nil {
		return errors.New("server failed to start")
	}
	return nil
}
