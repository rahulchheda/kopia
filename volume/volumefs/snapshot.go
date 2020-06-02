package volumefs

import (
	"context"
	"fmt"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
)

const (
	currentSnapshotDirName  = "t"
	previousSnapshotDirName = "p"
	directoryStreamType     = "kopia:directory" // copied from snapshot
)

// Snapshot returns a snapshot manifest.
type Snapshot struct {
	Current  *snapshot.Manifest
	Previous *snapshot.Manifest
}

// SnapshotAnalysis analyzes the data in a snapshot manifest.
type SnapshotAnalysis struct {
	BlockSizeBytes   int
	Bytes            int64
	NumBlocks        int
	NumDirs          int
	ChainLength      int
	ChainedBytes     int64
	ChainedNumBlocks int
	ChainedNumDirs   int
}

// Analyze sets values from a snapshot manifest
func (sa *SnapshotAnalysis) Analyze(man *snapshot.Manifest) {
	cs := man.Stats

	sa.BlockSizeBytes = int(cs.CachedFiles)
	sa.Bytes = cs.TotalFileSize - cs.ExcludedTotalFileSize
	sa.NumBlocks = cs.TotalFileCount - cs.ExcludedFileCount
	sa.NumDirs = cs.TotalDirectoryCount - cs.ExcludedDirCount

	sa.ChainLength = int(cs.NonCachedFiles)
	sa.ChainedBytes = cs.ExcludedTotalFileSize
	sa.ChainedNumBlocks = cs.ExcludedFileCount
	sa.ChainedNumDirs = cs.ExcludedDirCount
}

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
func (f *Filesystem) initFromSnapshot(ctx context.Context, prevVolumeSnapshotID string) (*snapshot.Manifest, fs.Directory, metadata, error) {
	man, rootEntry, md, err := f.findPreviousSnapshot(ctx, prevVolumeSnapshotID)
	if err != nil {
		return nil, nil, md, err
	}

	f.layoutProperties.initLayoutProperties(md.BlockSzB, md.DirSz, md.Depth)

	return man, rootEntry, md, err
}

// findPreviousSnapshot searches for the previous snapshot and parses it.
func (f *Filesystem) findPreviousSnapshot(ctx context.Context, prevVolumeSnapshotID string) (*snapshot.Manifest, fs.Directory, metadata, error) {
	var (
		err       error
		man       *snapshot.Manifest
		md        metadata
		rootEntry fs.Entry
	)

	if man, err = f.findSnapshotManifest(ctx, prevVolumeSnapshotID); err == nil {
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

	return nil, nil, md, err
}

// findSnapshotManifest returns the latest manifest for the specified volume snapshot.
// There may be multiple such manifests due to compact operations.
func (f *Filesystem) findSnapshotManifest(ctx context.Context, prevVolumeSnapshotID string) (*snapshot.Manifest, error) {
	man, err := f.sp.ListSnapshots(ctx, f.Repo, f.SourceInfo())
	if err != nil {
		return nil, err
	}

	var (
		descKey = f.snapshotDescription(prevVolumeSnapshotID)
		latest  *snapshot.Manifest
	)

	for _, m := range man {
		if m.Description == descKey {
			log(ctx).Debugf("found manifest %s startTime:%s endTime:%s", m.ID, m.StartTime, m.EndTime)

			if latest == nil || latest.EndTime.Before(m.EndTime) {
				latest = m
			}
		}
	}

	if latest == nil {
		return nil, ErrSnapshotNotFound
	}

	log(ctx).Debugf("latest: %s startTime:%s endTime:%s OID:%s", latest.ID, latest.StartTime, latest.EndTime, latest.RootObjectID())

	return latest, nil
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
		Description: f.snapshotDescription(""),
		StartTime:   f.epoch,
		EndTime:     time.Now(),
		Stats: snapshot.Stats{
			TotalDirectoryCount: int(summary.TotalDirCount),
			TotalFileCount:      int(summary.TotalFileCount),
			TotalFileSize:       summary.TotalFileSize,
			CachedFiles:         int32(f.blockSzB),
		},
		RootEntry: &snapshot.DirEntry{
			Name:       rootDir.name,
			Type:       snapshot.EntryTypeDirectory,
			ObjectID:   rootDir.oid,
			DirSummary: summary,
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

func (f *Filesystem) snapshotDescription(volSnapID string) string {
	if volSnapID == "" {
		volSnapID = f.VolumeSnapshotID
	}

	return fmt.Sprintf("volume:%s:%s", f.VolumeID, volSnapID)
}

// linkToPreviousSnapshot finds the previous snapshot and returns its dirMeta entry.
func (f *Filesystem) linkPreviousSnapshot(ctx context.Context, prevVolumeSnapshotID string) (*dirMeta, *snapshot.Manifest, error) {
	prevMan, _, prevMd, err := f.findPreviousSnapshot(ctx, prevVolumeSnapshotID)
	if err != nil {
		return nil, nil, err
	}

	if prevMd.VolSnapID != prevVolumeSnapshotID {
		f.logger.Debugf("previous volume snapshot exp[%s] md[%s]", prevVolumeSnapshotID, prevMd.VolSnapID)
		return nil, nil, ErrInvalidSnapshot
	}

	// import previous data
	f.logger.Debugf("found snapshot [%s] %#v %#v", prevMan.RootObjectID(), prevMd, prevMan)
	f.layoutProperties.initLayoutProperties(prevMd.BlockSzB, prevMd.DirSz, prevMd.Depth)
	f.prevVolumeSnapshotID = prevMd.VolSnapID

	// add the previous directory object to the root directory
	prevRootDm := &dirMeta{
		name:    previousSnapshotDirName,
		oid:     prevMan.RootObjectID(),
		summary: prevMan.RootEntry.DirSummary,
	}

	return prevRootDm, prevMan, nil
}
