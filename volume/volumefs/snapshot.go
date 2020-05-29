package volumefs

import (
	"context"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	currentSnapshotDirName  = "t"
	previousSnapshotDirName = "p"
	directoryStreamType     = "kopia:directory" // copied from snapshot
)

func (f *Filesystem) initFromSnapshot(ctx context.Context, snapshotID string) (*snapshot.Manifest, fs.Directory, metadata, error) {
	man, rootEntry, md, err := f.findPreviousSnapshot(ctx, snapshotID)
	if err != nil {
		return nil, nil, md, err
	}

	f.layoutProperties.initLayoutProperties(md.BlockSzB, md.DirSz, md.Depth)

	return man, rootEntry, md, err
}

// findPreviousSnapshot searches for the previous snapshot and parses it.
func (f *Filesystem) findPreviousSnapshot(ctx context.Context, prevSnapID string) (*snapshot.Manifest, fs.Directory, metadata, error) {
	var (
		err       error
		man       *snapshot.Manifest
		md        metadata
		rootEntry fs.Entry
		rootOID   object.ID
	)

	if rootOID, err = object.ParseID(prevSnapID); err == nil {
		if man, err = f.findSnapshotManifest(ctx, rootOID); err == nil {
			if rootEntry, err = f.sp.SnapshotRoot(f.Repo, man); err == nil {
				if !rootEntry.IsDir() {
					f.logger.Debugf("expected rootEntry to be a directory")
					return nil, nil, md, ErrInvalidSnapshot
				}

				dfe := rootEntry.(fs.Directory)
				if md, err = f.recoverMetadataFromDirEntry(ctx, dfe); err == nil {
					return man, dfe, md, err
				}
			}
		}
	}

	return nil, nil, md, err
}

// findSnapshotManifest returns the latest complete manifest containing the specified snapshot ID.
func (f *Filesystem) findSnapshotManifest(ctx context.Context, oid object.ID) (*snapshot.Manifest, error) {
	man, err := f.sp.ListSnapshots(ctx, f.Repo, f.SourceInfo())
	if err != nil {
		return nil, err
	}

	for _, m := range man {
		if m.RootObjectID() == oid {
			log(ctx).Debugf("found manifest %s startTime:%s", m.ID, m.StartTime)
			return m, nil
		}
	}

	return nil, ErrSnapshotNotFound
}

type snapshotProcessor interface {
	ListSnapshots(ctx context.Context, repo repo.Repository, si snapshot.SourceInfo) ([]*snapshot.Manifest, error)
	SnapshotRoot(rep repo.Repository, man *snapshot.Manifest) (fs.Entry, error)
}

type snapshotHelper struct{}

// nolint:gocritic
func (sh *snapshotHelper) ListSnapshots(ctx context.Context, repo repo.Repository, si snapshot.SourceInfo) ([]*snapshot.Manifest, error) {
	return snapshot.ListSnapshots(ctx, repo, si)
}

// nolint:gocritic
func (sh *snapshotHelper) SnapshotRoot(rep repo.Repository, man *snapshot.Manifest) (fs.Entry, error) {
	return snapshotfs.SnapshotRoot(rep, man)
}
