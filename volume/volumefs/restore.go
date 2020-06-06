package volumefs

import (
	"context"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/volume"
)

// Restore constants.
const (
	DefaultRestoreConcurrency = 4
)

// RestoreArgs contain arguments to the Restore method.
type RestoreArgs struct {
	// The amount of concurrency during restore. 0 assigns a default value.
	Concurrency int
	// The volume manager.
	VolumeManager volume.Manager
	// Profile containing location and credential information for the volume manager.
	VolumeAccessProfile interface{}
}

// Validate checks the arguments for correctness.
func (a *RestoreArgs) Validate() error {
	if a.Concurrency < 0 || a.VolumeManager == nil || a.VolumeAccessProfile == nil {
		return ErrInvalidArgs
	}

	return nil
}

// RestoreResult returns the result of a successful Restore operation.
type RestoreResult struct {
	Snapshot *Snapshot
	BlockIterStats
}

// Restore extracts a snapshot to a volume.
// The volume manager must provide a BlockWriter interface.
func (f *Filesystem) Restore(ctx context.Context, args RestoreArgs) (*RestoreResult, error) {
	if err := args.Validate(); err != nil {
		return nil, err
	}

	f.logger = log(ctx)
	f.logger.Debugf("restore volumeID[%s] VolumeSnapshotID[%s]", f.VolumeID, f.VolumeSnapshotID)

	// early check that the volume manager has a block writer
	if _, err := args.VolumeManager.GetBlockWriter(volume.GetBlockWriterArgs{}); err == volume.ErrNotSupported {
		return nil, err
	}

	man, rootEntry, _, err := f.rp.initFromSnapshot(ctx, f.VolumeSnapshotID)
	if err != nil {
		return nil, err
	}

	chainLen := int(man.Stats.NonCachedFiles)

	if args.Concurrency <= 0 {
		args.Concurrency = DefaultRestoreConcurrency
	}

	bm, err := f.rp.effectiveBlockMap(ctx, chainLen, rootEntry, nil, args.Concurrency)
	if err != nil {
		return nil, err
	}

	// initialize the block writer for real
	gbwArgs := volume.GetBlockWriterArgs{
		VolumeID: f.VolumeID,
		Profile:  args.VolumeAccessProfile,
	}

	bw, err := args.VolumeManager.GetBlockWriter(gbwArgs)
	if err != nil {
		return nil, err
	}

	bi := f.rp.newBlockIter(bm.Iterator())
	defer bi.Close() // nolint:errcheck // no error returned

	err = bw.PutBlocks(ctx, bi)
	if err != nil {
		return nil, err
	}

	ret := &RestoreResult{
		Snapshot:       newSnapshot(f.VolumeID, f.VolumeSnapshotID, man),
		BlockIterStats: bi.BlockIterStats,
	}

	return ret, nil
}

type restoreProcessor interface {
	initFromSnapshot(ctx context.Context, snapshotID string) (*snapshot.Manifest, fs.Directory, metadata, error)
	effectiveBlockMap(ctx context.Context, chainLen int, rootEntry fs.Directory, mergeDm *dirMeta, concurrency int) (BlockMap, error)
	newBlockIter(bmi BlockMapIterator) *blockIter
}
