package volumefs

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/volume"
)

// InitializeForBackup initializes the filesystem for a snapshot creation operation and returns
// root fs.Directory interface.
// The "Previous*" parameters are optional, but both must be set together if at all.
// If set then the previous filesystem state is recovered from the repository and only
// changed blocks fetched from the volume.
// If not set then then a complete backup is performed.
func (f *Filesystem) InitializeForBackup(ctx context.Context, previousSnapshotID, previousVolumeSnapshotID string) (fs.Directory, error) {
	var err error

	f.epoch = time.Now()
	f.logger = log(ctx)

	// load the previous snapshot if any
	if previousSnapshotID != "" {
		if f.rootDir, err = f.scanPreviousSnapshot(ctx, previousSnapshotID); err != nil {
			return nil, err
		}
	} else {
		f.rootDir = &dirMeta{
			name:  "/",
			mTime: f.epoch,
		}

		f.setMetadata(ctx)
	}

	// now get the changed blocks
	gbrArgs := volume.GetBlockReaderArgs{
		VolumeID:           f.VolumeID,
		SnapshotID:         f.VolumeSnapshotID,
		PreviousSnapshotID: previousVolumeSnapshotID,
		BlockSizeBytes:     f.GetBlockSize(),
		Profile:            f.VolumeAccessProfile,
	}

	f.blockReader, err = f.VolumeManager.GetBlockReader(gbrArgs)
	if err != nil {
		return nil, err
	}

	// update/synthesize the filesystem
	err = f.generateOrUpdateMetadata(ctx)
	if err != nil {
		return nil, err
	}

	return f.rootDir.fsEntry(f), nil
}

// scanPreviousSnapshot imports a previous snapshot and returns the in-memory metadata hierarchy.
func (f *Filesystem) scanPreviousSnapshot(ctx context.Context, prevSnapID string) (*dirMeta, error) {
	var err error // nolint:wsl

	f.previousRootEntry, err = f.findPreviousSnapshot(ctx, prevSnapID)
	if err != nil {
		return nil, err
	}

	if err = f.recoverMetadataFromRootEntry(ctx, f.previousRootEntry); err != nil { // nolint:gocritic
		return nil, err
	}

	return f.scanSnapshotDir(ctx, nil, f.previousRootEntry)
}

// scanSnapshotDir recursively descends a snapshot directory hierarchy and builds the corresponding in-memory tree.
func (f *Filesystem) scanSnapshotDir(ctx context.Context, ppp parsedPath, dir fs.Directory) (*dirMeta, error) {
	dm := &dirMeta{
		name:  dir.Name(),
		mTime: dir.ModTime(),
	}
	pp := parsedPath{}

	if ppp != nil {
		pp = append(ppp, dm.name) // nolint:gocritic
	} else {
		dm.mTime = f.previousRootEntry.ModTime() // default is to put previous StartTime
	}

	entries, err := dir.Readdir(ctx)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("error reading %s", pp))
	}

	f.logger.Debugf("scanDir [%s] => %s #%d", pp, dm.mTime.UTC().Round(0), len(entries))

	for _, entry := range entries {
		de, ok := entry.(fs.Directory)
		if !ok {
			continue
		}

		if dm.subdirs == nil {
			dm.subdirs = make([]*dirMeta, 0, len(entries))
		}

		sdm, err := f.scanSnapshotDir(ctx, pp, de)
		if err != nil {
			return nil, err
		}

		dm.insertSubdir(sdm)
	}

	for _, entry := range entries {
		fe, ok := entry.(fs.File)
		if !ok {
			continue
		}

		if dm.files == nil {
			dm.files = make([]*fileMeta, 0, len(entries))
		}

		fm := &fileMeta{
			name:  fe.Name(),
			mTime: fe.ModTime(),
		}

		dm.insertFile(fm)
		fpp := append(pp, fm.name)
		log(ctx).Debugf("scanDir [%s] => %s", fpp, fm.mTime.UTC().Round(0))
	}

	return dm, nil
}

// generateOrUpdateMetadata processes the changed blocks from the volume
func (f *Filesystem) generateOrUpdateMetadata(ctx context.Context) error {
	bal, err := f.blockReader.GetBlockAddresses(ctx)
	if err != nil {
		return err
	}

	log(ctx).Debugf("got %d changed blocks", len(bal))

	for _, ba := range bal {
		pp, err := f.addrToPath(ba)
		if err != nil {
			log(ctx).Errorf("address(%08x): %s", ba, err.Error())
			return err
		}

		f.ensureFile(ctx, pp)
	}

	return nil
}
