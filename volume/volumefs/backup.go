package volumefs

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
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

	// load the previous snapshot if any
	if previousSnapshotID != "" {
		if f.rootDir, err = f.scanPreviousSnapshot(ctx, previousSnapshotID); err != nil {
			return nil, err
		}
		f.recoverMetadata(ctx)
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
	if err = f.generateOrUpdateMetadata(ctx); err != nil {
		return nil, err
	}
	return f.rootDir.fsEntry(f), nil
}

// scanPreviousSnapshot imports a previous snapshot and returns the in-memory metadata hierarchy.
func (f *Filesystem) scanPreviousSnapshot(ctx context.Context, prevSnapID string) (*dirMeta, error) {
	var err error
	var man *snapshot.Manifest
	var rootEntry fs.Entry
	var rootOID object.ID
	if rootOID, err = object.ParseID(prevSnapID); err == nil {
		if man, err = f.scanGetSnapshotManifest(ctx, rootOID); err == nil {
			if rootEntry, err = snapshotfs.SnapshotRoot(f.Repo, man); err == nil {
				return f.scanSnapshotDir(ctx, nil, rootEntry)
			}
		}
	}
	return nil, err
}

// scanGetSnapshotManifest returns the latest complete manifest containing the specified snapshot ID.
func (f *Filesystem) scanGetSnapshotManifest(ctx context.Context, oid object.ID) (*snapshot.Manifest, error) {
	man, err := snapshot.ListSnapshots(ctx, f.Repo, f.SourceInfo())
	if err != nil {
		return nil, err
	}
	var latest *snapshot.Manifest
	for _, m := range man {
		if m.RootObjectID() == oid && m.IncompleteReason == "" && (latest == nil || m.StartTime.After(latest.StartTime)) {
			latest = m
		}
	}
	if latest != nil {
		return latest, nil
	}
	return nil, fmt.Errorf("manifest not found")
}

// scanSnapshotDir recursively descends a snapshot directory hierarchy and builds the corresponding in-memory tree.
func (f *Filesystem) scanSnapshotDir(ctx context.Context, ppp parsedPath, fse fs.Entry) (*dirMeta, error) {
	if !fse.IsDir() {
		return nil, fmt.Errorf("expected IsDir %s", ppp)
	}
	dir := fse.(fs.Directory)
	dm := &dirMeta{
		name:  dir.Name(),
		mTime: dir.ModTime(),
	}
	pp := parsedPath{}
	if ppp != nil {
		pp = append(ppp, dm.name)
	}
	entries, err := dir.Readdir(ctx)
	if err != nil {
		return nil, errors.Wrap(err, fmt.Sprintf("error reading %s", pp))
	}
	log(ctx).Debugf("scanDir [%s] => %s #%d", pp, dm.mTime.UTC().Round(0), len(entries))
	for _, entry := range entries {
		if de, ok := entry.(fs.Directory); ok {
			if dm.subdirs == nil {
				dm.subdirs = make([]*dirMeta, 0, len(entries))
			}
			sdm, err := f.scanSnapshotDir(ctx, pp, de)
			if err != nil {
				return nil, err
			}
			dm.insertSubdir(sdm)
			continue
		}
	}
	for _, entry := range entries {
		if fe, ok := entry.(fs.File); ok {
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

// metadata file names match the following patterns
var reMetaFile = regexp.MustCompile(`meta:(\w+):(.*)`)
var metaFmtX = "meta:%s:%x"

const (
	metaBlockSzB = "blockSzB"
)

func (f *Filesystem) isMetaFile(name string) (bool, string, string) {
	matches := reMetaFile.FindStringSubmatch(name)
	if len(matches) != 3 {
		return false, "", ""
	}
	return true, matches[1], matches[2]
}

// recoverMetadata extracts previously saved metadata from top level files
func (f *Filesystem) recoverMetadata(ctx context.Context) {
	for _, fm := range f.rootDir.files {
		isMeta, name, value := f.isMetaFile(fm.name)
		if !isMeta {
			continue
		}
		switch name {
		case metaBlockSzB:
			sz := int64(0)
			fmt.Sscanf(value, "%x", &sz)
			if sz > 0 {
				log(ctx).Debugf("recovered blockSzB=%d", sz)
				f.blockSzB = sz
			}
		}
	}
}

// setMetadata sets one-time filesystem metadata
func (f *Filesystem) setMetadata(ctx context.Context) {
	fn := fmt.Sprintf(metaFmtX, metaBlockSzB, f.blockSzB)
	pp := parsedPath([]string{fn})
	f.ensureFile(ctx, pp)
}
