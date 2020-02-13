package snapmeta

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/kopia/kopia/tests/tools/kopiarunner"
)

var _ Store = &KopiaMetadata{}
var _ DataPersister = &KopiaMetadata{}

// KopiaMetadata handles metadata persistency of a snapshot store, using a Kopia
// repository as the persistency mechanism
type KopiaMetadata struct {
	*Simple
	LocalMetadataDir string
	snap             *kopiarunner.KopiaSnapshotter
}

// NewKopiaMetadata instantiates a new KopiaMetadata and returns its pointer.
func NewKopiaMetadata() (*KopiaMetadata, error) {
	localDir, err := ioutil.TempDir("", "kopia-local-metadata-")
	if err != nil {
		return nil, err
	}

	snap, err := kopiarunner.NewKopiaSnapshotter()
	if err != nil {
		return nil, err
	}

	return &KopiaMetadata{
		LocalMetadataDir: localDir,
		Simple:           NewSimple(),
		snap:             snap,
	}, nil
}

// Cleanup cleans up the local temporary files used by a KopiaMetadata
func (store *KopiaMetadata) Cleanup() {
	if store.LocalMetadataDir != "" {
		os.RemoveAll(store.LocalMetadataDir) //nolint:errcheck
	}

	if store.snap != nil {
		store.snap.Cleanup()
	}
}

// ConnectOrCreateS3 implements the RepoManager interface, connects to a repo in an S3
// bucket or attempts to create one if connection is unsuccessful
func (store *KopiaMetadata) ConnectOrCreateS3(bucketName, pathPrefix string) error {
	return store.snap.ConnectOrCreateS3(bucketName, pathPrefix)
}

// ConnectOrCreateFilesystem implements the RepoManager interface, connects to a repo in the filesystem
// or attempts to create one if connection is unsuccessful
func (store *KopiaMetadata) ConnectOrCreateFilesystem(path string) error {
	return store.snap.ConnectOrCreateFilesystem(path)
}

// LoadMetadata implements the DataPersister interface, restores the latest
// snapshot from the kopia repository and decodes its contents, populating
// its metadata on the snapshots residing in the target test repository.
func (store *KopiaMetadata) LoadMetadata() error {
	snapIDs, err := store.snap.ListSnapshots()
	if err != nil {
		return err
	}

	if len(snapIDs) == 0 {
		return nil // No snapshot IDs fouund in repository
	}

	lastSnapID := snapIDs[len(snapIDs)-1]

	restorePath := filepath.Join(store.LocalMetadataDir, "kopia-metadata-latest")

	err = store.snap.RestoreSnapshot(lastSnapID, restorePath)
	if err != nil {
		return err
	}

	defer os.Remove(restorePath) //nolint:errcheck

	f, err := os.Open(restorePath) //nolint:gosec
	if err != nil {
		return err
	}

	err = json.NewDecoder(f).Decode(&(store.Simple.m))
	if err != nil {
		return err
	}

	return nil
}

// FlushMetadata implements the DataPersister interface, flushing the local
// metadata on the target test repo's snapshots to the metadata Kopia repository
// as a snapshot create.
func (store *KopiaMetadata) FlushMetadata() error {
	f, err := ioutil.TempFile(store.LocalMetadataDir, "kopia-metadata-")
	if err != nil {
		return err
	}

	defer func() {
		f.Close()           //nolint:errcheck
		os.Remove(f.Name()) //nolint:errcheck
	}()

	err = json.NewEncoder(f).Encode(store.Simple.m)
	if err != nil {
		return err
	}

	_, err = store.snap.CreateSnapshot(f.Name())
	if err != nil {
		return err
	}

	return nil
}
