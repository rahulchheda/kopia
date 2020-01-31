package snapstore

import (
	"crypto/sha1"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/kopia/kopia/tests/robustness/snapif"
)

var _ Storer = &KopiaMetadata{}

type KopiaMetadata struct {
	LocalMetadataDir string
	s                *Simple
	snap             *snapif.KopiaSnapshotter
}

func NewKopiaMetadata() (*KopiaMetadata, error) {
	snap, err := snapif.NewKopiaSnapshotter()
	if err != nil {
		return nil, err
	}

	localDir, err := ioutil.TempDir("", "kopia-local-metadata-")
	if err != nil {
		return nil, err
	}

	return &KopiaMetadata{
		LocalMetadataDir: localDir,
		s:                NewSimple(),
		snap:             snap,
	}, nil
}

func (store *KopiaMetadata) Cleanup() {
	if store.LocalMetadataDir != "" {
		os.RemoveAll(store.LocalMetadataDir) //nolint:errcheck
	}
}

func (store *KopiaMetadata) Store(key string, val []byte) error {
	store.s.Store(key, val)
	return nil
}

func (store *KopiaMetadata) Load(key string) ([]byte, error) {
	return store.s.Load(key)
}

func (store *KopiaMetadata) GetKeys() []string {
	return store.s.GetKeys()
}

func (store *KopiaMetadata) ConnectOrCreateRepoS3(bucketName, pathPrefix string) error {
	path.Join(getHostPath(), pathPrefix)
	args := []string{"s3", "--bucket", bucketName, "--prefix", pathPrefix}

	return store.snap.ConnectOrCreateRepo(args...)
}

func (store *KopiaMetadata) ConnectOrCreateRepoFilesystem(path string) error {
	args := []string{"filesystem", "--path", path}

	return store.snap.ConnectOrCreateRepo(args...)
}

func (store *KopiaMetadata) LoadMetadata() error {
	stdout, _, err := store.snap.Runner.Run("manifest", "ls")
	if err != nil {
		return err
	}

	lastSnapID := parseForLatestSnapshotID(stdout)
	if lastSnapID == "" {
		return errors.New("Could not parse snapshot ID")
	}

	restorePath := filepath.Join(store.LocalMetadataDir, "kopia-metadata-latest")
	err = store.snap.RestoreSnapshot(lastSnapID, restorePath)
	if err != nil {
		return err
	}

	defer os.Remove(restorePath)

	f, err := os.Open(restorePath)
	if err != nil {
		return err
	}

	err = json.NewDecoder(f).Decode(&(store.s.s))
	if err != nil {
		return err
	}

	return nil
}

func (store *KopiaMetadata) FlushMetadata() error {

	f, err := ioutil.TempFile(store.LocalMetadataDir, "kopia-metadata-")
	if err != nil {
		return err
	}

	defer func() {
		f.Close()           //nolint:errcheck
		os.Remove(f.Name()) //nolint:errcheck
	}()

	err = json.NewEncoder(f).Encode(store.s.s)
	if err != nil {
		return err
	}

	_, err = store.snap.TakeSnapshot(f.Name())
	if err != nil {
		return err
	}

	return nil
}

func parseForLatestSnapshotID(output string) string {
	lines := strings.Split(output, "\n")

	var lastSnapID string
	for _, l := range lines {
		fields := strings.Fields(l)
		if len(fields) > 0 {
			if fields[5] == "type:snapshot" {
				lastSnapID = fields[0]
			}
		}
	}

	return lastSnapID
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
