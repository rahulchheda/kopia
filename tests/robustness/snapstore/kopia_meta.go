package snapstore

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/kopia/kopia/tests/robustness/snapif"
)

var _ Storer = &KopiaMetadata{}

type KopiaMetadata struct {
	*Simple
	LocalMetadataDir string
	snap             *snapif.KopiaSnapshotter
}

func NewKopiaMetadata() (*KopiaMetadata, error) {
	localDir, err := ioutil.TempDir("", "kopia-local-metadata-")
	if err != nil {
		return nil, err
	}

	snap, err := snapif.NewKopiaSnapshotter()
	if err != nil {
		return nil, err
	}

	return &KopiaMetadata{
		LocalMetadataDir: localDir,
		Simple:           NewSimple(),
		snap:             snap,
	}, nil
}

func (store *KopiaMetadata) Cleanup() {
	if store.LocalMetadataDir != "" {
		os.RemoveAll(store.LocalMetadataDir) //nolint:errcheck
	}

	if store.snap != nil {
		store.snap.Cleanup()
	}
}

func (store *KopiaMetadata) ConnectOrCreateS3(bucketName, pathPrefix string) error {
	return store.snap.ConnectOrCreateS3(bucketName, pathPrefix)
}

func (store *KopiaMetadata) ConnectOrCreateFilesystem(path string) error {
	return store.snap.ConnectOrCreateFilesystem(path)
}

func (store *KopiaMetadata) LoadMetadata() error {
	stdout, _, err := store.snap.Runner.Run("manifest", "ls")
	if err != nil {
		return err
	}

	lastSnapID := parseForLatestSnapshotID(stdout)
	if lastSnapID == "" {
		return nil //errors.New("Could not parse snapshot ID")
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

	err = json.NewDecoder(f).Decode(&(store.Simple.s))
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

	err = json.NewEncoder(f).Encode(store.Simple.s)
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
		if len(fields) > 5 {
			if fields[5] == "type:snapshot" {
				lastSnapID = fields[0]
			}
		}
	}

	return lastSnapID
}
