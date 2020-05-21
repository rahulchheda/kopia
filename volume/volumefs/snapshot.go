package volumefs

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	currentSnapshotDirName  = "t"
	previousSnapshotDirName = "p"
	directoryStreamType     = "kopia:directory" // copied from snapshot
)

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
		log(ctx).Debugf("found manifest %s startTime:%s", latest.ID, latest.StartTime)
		return latest, nil
	}

	return nil, fmt.Errorf("manifest not found")
}
