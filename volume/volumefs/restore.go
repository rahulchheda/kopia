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
	// The identifier of the previous snapshot if this is an incremental.
	PreviousSnapshotID string
	// The amount of concurrency during restore. 0 assigns a default value.
	RestoreConcurrency int
}

// Validate checks the arguments for correctness.
func (a *RestoreArgs) Validate() error {
	if a.PreviousSnapshotID == "" || a.RestoreConcurrency < 0 {
		return ErrInvalidArgs
	}

	return nil
}

// RestoreResult returns the result of a successful Restore operation.
type RestoreResult struct {
	BlockIterStats
}

// Restore extracts a snapshot to a volume.
// The volume manager must provide a BlockWriter interface.
func (f *Filesystem) Restore(ctx context.Context, args RestoreArgs) (*RestoreResult, error) {
	if err := args.Validate(); err != nil {
		return nil, err
	}

	f.logger = log(ctx)

	// early check that the volume manager has a block writer
	if _, err := f.VolumeManager.GetBlockWriter(volume.GetBlockWriterArgs{}); err == volume.ErrNotSupported {
		return nil, err
	}

	man, rootEntry, md, err := f.rp.initFromSnapshot(ctx, args.PreviousSnapshotID)
	if err != nil {
		return nil, err
	}

	chainLen := int(man.Stats.NonCachedFiles)
	f.logger.Debugf("md: %#v l=%d", md, chainLen)

	concurrency := DefaultRestoreConcurrency
	if args.RestoreConcurrency > 0 {
		concurrency = args.RestoreConcurrency
	}

	bm, err := f.rp.effectiveBlockMap(ctx, chainLen, rootEntry, concurrency)
	if err != nil {
		return nil, err
	}

	// initialize the block writer for real
	gbwArgs := volume.GetBlockWriterArgs{
		VolumeID: f.VolumeID,
		Profile:  f.VolumeAccessProfile,
	}

	f.logger.Debugf("gbw: %#v=%#v", gbwArgs, gbwArgs.Profile)

	bw, err := f.VolumeManager.GetBlockWriter(gbwArgs)
	if err != nil {
		f.logger.Debugf("get block writer: %v", err)
		return nil, err
	}

	bi := f.rp.newBlockIter(bm.Iterator())
	defer bi.Close() // nolint:errcheck // no error returned

	err = bw.PutBlocks(ctx, bi)
	if err != nil {
		return nil, err
	}

	ret := &RestoreResult{
		BlockIterStats: bi.BlockIterStats,
	}

	return ret, nil
}

type restoreProcessor interface {
	initFromSnapshot(ctx context.Context, snapshotID string) (*snapshot.Manifest, fs.Directory, metadata, error)
	effectiveBlockMap(ctx context.Context, chainLen int, rootEntry fs.Directory, concurrency int) (BlockMap, error)
	newBlockIter(bmi BlockMapIterator) *blockIter
}
