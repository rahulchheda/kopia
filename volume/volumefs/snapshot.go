package volumefs

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

// findPreviousSnapshot returns the root directory entry for a snapshot.
func (f *Filesystem) findPreviousSnapshot(ctx context.Context, prevSnapID string) (fs.Directory, error) {
	var err error              // nolint:wsl
	var man *snapshot.Manifest // nolint:wsl
	var rootEntry fs.Entry     // nolint:wsl
	var rootOID object.ID      // nolint:wsl

	if rootOID, err = object.ParseID(prevSnapID); err == nil {
		if man, err = f.findSnapshotManifest(ctx, rootOID); err == nil {
			if rootEntry, err = snapshotfs.SnapshotRoot(f.Repo, man); err == nil {
				if !rootEntry.IsDir() {
					return nil, fmt.Errorf("expected rootEntry to be a directory") // nolint
				}

				dfe := rootEntry.(fs.Directory)

				return dfe, nil
			}
		}
	}

	return nil, err
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
		log(ctx).Debugf("found manifest %s modTime:%s", latest.ID, latest.RootEntry.ModTime)
		return latest, nil
	}

	return nil, fmt.Errorf("manifest not found")
}

func (f *Filesystem) initFromSnapshot(ctx context.Context, previousSnapshotID string) (err error) {
	f.previousRootEntry, err = f.findPreviousSnapshot(ctx, previousSnapshotID)
	if err != nil {
		return
	}

	if err = f.recoverMetadataFromRootEntry(ctx, f.previousRootEntry); err != nil {
		return
	}

	return
}
