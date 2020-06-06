package volumefs

import (
	"context"
	"sync"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/volume"
)

// Backup constants.
const (
	DefaultBackupConcurrency = 4
)

// BackupArgs contain arguments to the Backup method.
type BackupArgs struct {
	// The identifier of the previous volume snapshot if this is an incremental.
	PreviousVolumeSnapshotID string
	// The amount of concurrency during backup. 0 assigns a default value.
	Concurrency int
	// If non-zero, the maximum chain length after which automatic compaction takes place.
	MaxChainLength int
	// The volume manager.
	VolumeManager volume.Manager
	// Profile containing location and credential information for the volume manager.
	VolumeAccessProfile interface{}
}

// Validate checks the arguments for correctness.
func (a *BackupArgs) Validate() error {
	if a.Concurrency < 0 || a.MaxChainLength < 0 || a.VolumeManager == nil || a.VolumeAccessProfile == nil {
		return ErrInvalidArgs
	}

	return nil
}

// BackupResult returns the result of a Backup operation
type BackupResult struct {
	Snapshot         *Snapshot
	PreviousSnapshot *Snapshot
	BlockIterStats
	CompactIterStats BlockIterStats
}

// Backup a volume.
// The volume manager must provide a BlockReader interface.
func (f *Filesystem) Backup(ctx context.Context, args BackupArgs) (*BackupResult, error) {
	if err := args.Validate(); err != nil {
		return nil, err
	}

	f.logger = log(ctx)
	f.epoch = time.Now()
	f.logger.Debugf("backup volumeID[%s] VolumeSnapshotID[%s] PreviousVolumeSnapshotID[%s]", f.VolumeID, f.VolumeSnapshotID, args.PreviousVolumeSnapshotID)

	gbrArgs := volume.GetBlockReaderArgs{
		VolumeID:           f.VolumeID,
		SnapshotID:         f.VolumeSnapshotID,
		PreviousSnapshotID: args.PreviousVolumeSnapshotID,
		Profile:            args.VolumeAccessProfile,
	}

	br, err := args.VolumeManager.GetBlockReader(gbrArgs)
	if err != nil {
		return nil, err
	}

	var (
		bis         BlockIterStats
		compactBis  BlockIterStats
		compactDm   *dirMeta
		curDm       *dirMeta
		curMan      *snapshot.Manifest
		prevMan     *snapshot.Manifest
		prevRoot    fs.Directory
		prevRootDm  *dirMeta
		prevSnapMan *snapshot.Manifest
		rootDir     *dirMeta
	)

	if args.PreviousVolumeSnapshotID != "" {
		prevRootDm, prevMan, prevRoot, err = f.bp.linkPreviousSnapshot(ctx, args.PreviousVolumeSnapshotID)
		if err != nil {
			return nil, err
		}

		prevSnapMan = prevMan
	}

	if args.Concurrency <= 0 {
		args.Concurrency = DefaultBackupConcurrency
	}

	if curDm, bis, err = f.bp.backupBlocks(ctx, br, args.Concurrency); err == nil {
		if compactDm, compactBis, err = f.bp.maybeCompactBackup(ctx, args, curDm, prevMan, prevRoot); err == nil {
			if compactDm != nil { // compacted so synthesize full
				curDm, prevMan, prevRootDm = compactDm, nil, nil
			}

			if err = f.bp.writeDirToRepo(ctx, parsedPath{currentSnapshotDirName}, curDm, true); err == nil {
				if rootDir, err = f.bp.createRoot(ctx, curDm, prevRootDm); err == nil {
					curMan, err = f.bp.commitSnapshot(ctx, rootDir, prevMan)
				}
			}
		}
	}

	if err != nil {
		return nil, err
	}

	res := &BackupResult{
		Snapshot:         newSnapshot(f.VolumeID, f.VolumeSnapshotID, curMan),
		BlockIterStats:   bis,
		CompactIterStats: compactBis,
	}
	if prevSnapMan != nil {
		res.PreviousSnapshot = newSnapshot(f.VolumeID, f.prevVolumeSnapshotID, prevSnapMan)
	}

	return res, nil
}

// backupProcessor aids in unit testing
type backupProcessor interface {
	backupBlocks(ctx context.Context, br volume.BlockReader, numWorkers int) (*dirMeta, BlockIterStats, error)
	commitSnapshot(ctx context.Context, rootDir *dirMeta, psm *snapshot.Manifest) (*snapshot.Manifest, error)
	compactBackup(ctx context.Context, curDm *dirMeta, prevRoot fs.Directory, chainLen, concurrency int) (*dirMeta, BlockIterStats, error)
	createRoot(ctx context.Context, curDm, prevRootDm *dirMeta) (*dirMeta, error)
	linkPreviousSnapshot(ctx context.Context, prevVolumeSnapshotID string) (*dirMeta, *snapshot.Manifest, fs.Directory, error)
	maybeCompactBackup(ctx context.Context, args BackupArgs, curDm *dirMeta, prevMan *snapshot.Manifest, prevRoot fs.Directory) (*dirMeta, BlockIterStats, error)
	writeDirToRepo(ctx context.Context, pp parsedPath, dir *dirMeta, writeSubTree bool) error
}

// maybeCompactBackup compacts the in-memory snapshot if possible and necessary.
// On compaction:
//   - New directory returned
// No compaction:
//   - nil directory returned
func (f *Filesystem) maybeCompactBackup(ctx context.Context, args BackupArgs, curDm *dirMeta, prevMan *snapshot.Manifest, prevRoot fs.Directory) (*dirMeta, BlockIterStats, error) {
	var (
		bis         BlockIterStats
		err         error
		chainLen    int
		mustCompact bool
	)

	if prevMan != nil && prevRoot != nil {
		chainLen = getChainLength(prevMan)

		// @TODO: if MinChainLength exceeded used WBC/WDC to decide on compaction

		if args.MaxChainLength > 0 && chainLen >= args.MaxChainLength {
			f.logger.Debugf("Maximum chain length (%d) reached or exceeded: %d", args.MaxChainLength, chainLen)

			mustCompact = true
		}
	}

	if mustCompact {
		f.logger.Debugf("compacting %s", curDm.name)
		curDm, bis, err = f.bp.compactBackup(ctx, curDm, prevRoot, chainLen, args.Concurrency)
	} else {
		curDm = nil
	}

	return curDm, bis, err
}

// backupBlocks writes the volume blocks and the block map hierarchy to the repo.
func (f *Filesystem) backupBlocks(ctx context.Context, br volume.BlockReader, numWorkers int) (*dirMeta, BlockIterStats, error) {
	bis := BlockIterStats{}

	bi, err := br.GetBlocks(ctx)
	if err != nil {
		return nil, bis, err
	}

	bbh := &backupBlocksHelper{}
	bbh.init(f)
	bbh.bp.Iter = bi
	bbh.bp.Worker = bbh.worker
	bbh.bp.NumWorkers = numWorkers

	// process the snapshot blocks
	err = bbh.bp.Run(ctx)
	if err != nil {
		return nil, bis, err
	}

	bis.NumBlocks = int(bbh.bp.NumBlocks)
	bis.MaxBlockAddr = bbh.bp.MaxAddress
	bis.MinBlockAddr = bbh.bp.MinAddress

	return bbh.curRoot, bis, nil
}

// backupBlocksHelper is a helper for backupBlocks
type backupBlocksHelper struct {
	bp        volume.BlockProcessor
	f         *Filesystem
	mux       sync.Mutex
	blockSize int
	bufPool   sync.Pool
	curRoot   *dirMeta
}

func (bbh *backupBlocksHelper) init(f *Filesystem) {
	bbh.f = f
	bbh.bufPool.New = func() interface{} {
		buf := make([]byte, bbh.blockSize) // size determined at runtime

		return &buf
	}
	bbh.curRoot = &dirMeta{
		name: currentSnapshotDirName,
	}
}

func (bbh *backupBlocksHelper) worker(ctx context.Context, block volume.Block) error {
	pp, err := bbh.f.addrToPath(block.Address())
	if err != nil {
		return err
	}

	fm := bbh.ensureFile(pp)
	bbh.f.logger.Debugf("block [%s] sz=%d", pp.String(), block.Size())

	bufPtr := bbh.getBuffer(block.Size())
	defer bbh.releaseBuffer(bufPtr)

	rc, err := block.Get(ctx)
	if err != nil {
		bbh.f.logger.Debugf("block [%s] get: %v", pp.String(), err)
		return err
	}

	oid, sz, err := bbh.f.up.writeFileToRepo(ctx, pp, rc, *bufPtr)
	if err == nil {
		fm.oid = oid

		if int(sz) != block.Size() {
			bbh.f.logger.Debugf("block [%s] writeFileToRepo: wrote %d/%d", pp.String(), sz, block.Size())
		}
	}

	closeErr := rc.Close()
	if closeErr != nil {
		bbh.f.logger.Debugf("block [%s] Close: %v", pp.String(), err)

		if err == nil {
			err = closeErr
		}
	}

	return err
}

func (bbh *backupBlocksHelper) getBuffer(blockSize int) *[]byte {
	if bbh.blockSize == 0 {
		bbh.mux.Lock()
		if bbh.blockSize == 0 {
			bbh.blockSize = blockSize // safe assumption that all blocks have the same size
		}
		bbh.mux.Unlock()
	}

	return bbh.bufPool.Get().(*[]byte)
}

func (bbh *backupBlocksHelper) releaseBuffer(bufPtr *[]byte) {
	bbh.bufPool.Put(bufPtr)
}

func (bbh *backupBlocksHelper) ensureFile(pp parsedPath) *fileMeta {
	bbh.mux.Lock()
	defer bbh.mux.Unlock()

	return bbh.f.ensureFileInTree(bbh.curRoot, pp)
}
