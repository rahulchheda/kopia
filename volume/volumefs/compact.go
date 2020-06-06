package volumefs

import (
	"context"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
)

// Compact constants.
const (
	DefaultCompactConcurrency = 4
)

// CompactArgs contain arguments to the Compact method.
type CompactArgs struct {
	// The amount of concurrency during restore. 0 assigns a default value.
	Concurrency int
}

// Validate checks the arguments for correctness.
func (a *CompactArgs) Validate() error {
	if a.Concurrency < 0 {
		return ErrInvalidArgs
	}

	return nil
}

// CompactResult returns the result of a Compact operation
type CompactResult struct {
	Snapshot         *Snapshot
	PreviousSnapshot *Snapshot
	BlockIterStats
}

// Compact a volume snapshot creating a new snapshot with the same VolSnapID metadata file
// but no VolPrevSnapID metadata.  After compaction earlier snapshots can be deleted to free
// up references to shadowed blocks.
//
// Note that this should not be used concurrently with an incremental snapshot that references the
// snapshot being compacted (i.e. the "head" snapshot).
func (f *Filesystem) Compact(ctx context.Context, args CompactArgs) (*CompactResult, error) {
	if err := args.Validate(); err != nil {
		return nil, err
	}

	f.logger = log(ctx)
	f.epoch = time.Now()
	f.logger.Debugf("compact volumeID[%s] VolumeSnapshotID[%s]", f.VolumeID, f.VolumeSnapshotID)

	man, rootEntry, _, err := f.cp.initFromSnapshot(ctx, f.VolumeSnapshotID)
	if err != nil {
		return nil, err
	}

	chainLen := int(man.Stats.NonCachedFiles)

	if args.Concurrency <= 0 {
		args.Concurrency = DefaultCompactConcurrency
	}

	var (
		bis     BlockIterStats
		curDm   *dirMeta
		curMan  *snapshot.Manifest
		rootDir *dirMeta
	)

	if curDm, bis, err = f.compactBackup(ctx, nil, rootEntry, chainLen, args.Concurrency); err == nil {
		if err = f.cp.writeDirToRepo(ctx, parsedPath{curDm.name}, curDm, true); err == nil {
			if rootDir, err = f.cp.createRoot(ctx, curDm, nil); err == nil {
				curMan, err = f.cp.commitSnapshot(ctx, rootDir, nil)
			}
		}
	}

	if err != nil {
		return nil, err
	}

	res := &CompactResult{
		Snapshot:         newSnapshot(f.VolumeID, f.VolumeSnapshotID, curMan),
		PreviousSnapshot: newSnapshot(f.VolumeID, f.VolumeSnapshotID, man),
		BlockIterStats:   bis,
	}

	return res, nil
}

// compactBackup compacts a snapshot during Backup by overlaying the curDm in-memory
// directory hierarchy onto the effective block map and returns a new in-memory hierarchy.
// When used in the Compact operation curDm should be nil.
func (f *Filesystem) compactBackup(ctx context.Context, curDm *dirMeta, prevRoot fs.Directory, chainLen, concurrency int) (*dirMeta, BlockIterStats, error) {
	bm, err := f.cp.effectiveBlockMap(ctx, chainLen, prevRoot, curDm, concurrency)
	if err != nil {
		return nil, BlockIterStats{}, err
	}

	return f.cp.createTreeFromBlockMap(bm)
}

type compactProcessor interface {
	commitSnapshot(ctx context.Context, rootDir *dirMeta, psm *snapshot.Manifest) (*snapshot.Manifest, error)
	createRoot(ctx context.Context, curDm, prevRootDm *dirMeta) (*dirMeta, error)
	createTreeFromBlockMap(bm BlockMap) (*dirMeta, BlockIterStats, error)
	effectiveBlockMap(ctx context.Context, chainLen int, rootEntry fs.Directory, mergeDm *dirMeta, concurrency int) (BlockMap, error)
	initFromSnapshot(ctx context.Context, snapshotID string) (*snapshot.Manifest, fs.Directory, metadata, error)
	writeDirToRepo(ctx context.Context, pp parsedPath, dir *dirMeta, writeSubTree bool) error
}
