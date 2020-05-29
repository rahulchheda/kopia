package volumefs

import (
	"context"
	"fmt"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	currentSnapshotDirName  = "t"
	previousSnapshotDirName = "p"
	directoryStreamType     = "kopia:directory" // copied from snapshot
)

// snapshotProcessor aids in unit testing
type snapshotProcessor interface {
	ListSnapshots(ctx context.Context, repo repo.Repository, si snapshot.SourceInfo) ([]*snapshot.Manifest, error)
	SnapshotRoot(rep repo.Repository, man *snapshot.Manifest) (fs.Entry, error)
	SaveSnapshot(ctx context.Context, rep repo.Repository, man *snapshot.Manifest) (manifest.ID, error)
}

type snapshotHelper struct{}

var _ snapshotProcessor = (*snapshotHelper)(nil)

// nolint:gocritic
func (sh *snapshotHelper) ListSnapshots(ctx context.Context, repo repo.Repository, si snapshot.SourceInfo) ([]*snapshot.Manifest, error) {
	return snapshot.ListSnapshots(ctx, repo, si)
}

// nolint:gocritic
func (sh *snapshotHelper) SnapshotRoot(rep repo.Repository, man *snapshot.Manifest) (fs.Entry, error) {
	return snapshotfs.SnapshotRoot(rep, man)
}

// nolint:gocritic
func (sh *snapshotHelper) SaveSnapshot(ctx context.Context, rep repo.Repository, man *snapshot.Manifest) (manifest.ID, error) {
	return snapshot.SaveSnapshot(ctx, rep, man)
}

// initFromSnapshot retrieves a snapshot and initializes the filesystem with the previous metadata.
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

// commitSnapshot writes a snapshot manifest to the repository
func (f *Filesystem) commitSnapshot(ctx context.Context, rootDir *dirMeta, psm *snapshot.Manifest) (*snapshot.Manifest, error) {
	// write the root directory manifest
	err := f.up.writeDirToRepo(ctx, parsedPath{}, rootDir, false)
	if err != nil {
		return nil, err
	}

	curManifest := f.createSnapshotManifest(rootDir, psm)

	_, err = f.sp.SaveSnapshot(ctx, f.Repo, curManifest)
	if err != nil {
		f.logger.Debugf("snapshot.SaveSnapshot: %v", err)
		return nil, err
	}

	if err = f.repo.Flush(ctx); err != nil {
		f.logger.Debugf("repo.Flush: %v", err)
		return nil, err
	}

	return curManifest, nil
}

func (f *Filesystem) createSnapshotManifest(rootDir *dirMeta, psm *snapshot.Manifest) *snapshot.Manifest {
	summary := rootDir.summary
	sm := &snapshot.Manifest{
		Source:      f.SourceInfo(),
		Description: f.snapshotDescription(),
		StartTime:   f.epoch,
		EndTime:     time.Now(),
		Stats: snapshot.Stats{
			TotalDirectoryCount: int(summary.TotalDirCount),
			TotalFileCount:      int(summary.TotalFileCount),
			TotalFileSize:       summary.TotalFileSize,
			CachedFiles:         int32(f.blockSzB),
		},
		RootEntry: &snapshot.DirEntry{
			Name:        rootDir.name,
			Type:        snapshot.EntryTypeDirectory,
			ModTime:     f.epoch,
			Permissions: 0700, // nolint:gomnd
			ObjectID:    rootDir.oid,
			DirSummary:  summary,
		},
	}

	if psm != nil {
		sm.Stats.ExcludedDirCount = psm.Stats.TotalDirectoryCount
		sm.Stats.ExcludedFileCount = psm.Stats.TotalFileCount
		sm.Stats.ExcludedTotalFileSize = psm.Stats.TotalFileSize
		sm.Stats.NonCachedFiles = psm.Stats.NonCachedFiles + 1 // chain length
	}

	return sm
}

func (f *Filesystem) snapshotDescription() string {
	return fmt.Sprintf("volume:%s:%s", f.VolumeID, f.VolumeSnapshotID)
}
