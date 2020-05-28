package volumefs

import (
	"context"
	"fmt"

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
			if rootEntry, err = snapshotfs.SnapshotRoot(f.Repo, man); err == nil {
				if !rootEntry.IsDir() {
					return nil, nil, md, fmt.Errorf("expected rootEntry to be a directory") // nolint
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
	man, err := snapshotLister(ctx, f.Repo, f.SourceInfo())
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

type snapshotListerFn func(context.Context, repo.Repository, snapshot.SourceInfo) ([]*snapshot.Manifest, error)

var snapshotLister snapshotListerFn = snapshot.ListSnapshots // for UT interception
