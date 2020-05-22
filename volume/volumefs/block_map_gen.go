package volumefs

import (
	"context"
	"sync"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/parallelwork"
	"github.com/kopia/kopia/repo/object"
)

// makeEffectiveBlockMap recovers the effective block map, a mapping of block address to
// the file objectID containing the block data.
//
// Essentially the process of building the block map involves the following:
//  For each snapshot filesystem in the chain, in order of oldest to newest, do
//    For each leaf directory in the filesystem, do
//      Add the block file's object ID to the block map.
//
// Traversal of later snapshots filesystems can replace entries set by earlier snapshot
// filesystems. // At the end, what is left in the block map is the effective mapping.
//
// Concurrently scanning individual leaf directories can speed up the process of generating
// the map, as long as the scan in any snapshot filesystem does not "get ahead" of the
// earlier filesystem scans of the corresponding leaf directory addresses.
func (f *Filesystem) makeEffectiveBlockMap(ctx context.Context, chainLen int, rootEntry fs.Directory, concurrency int) (BlockMap, error) {
	bmg := &blockMapGenerator{}
	bmg.Init(f, chainLen, concurrency)

	err := bmg.Run(ctx, rootEntry)

	return bmg.bm, err
}

type blockMapGenerator struct {
	f           *Filesystem
	chainLen    int
	concurrency int
	bm          blockMapMutable
	roots       []fs.Directory // latest is first
	mux         sync.Mutex
	curChain    int
	queue       *parallelwork.Queue
}

func (bmg *blockMapGenerator) Init(f *Filesystem, chainLen, concurrency int) {
	bmg.f = f
	bmg.chainLen = chainLen
	bmg.concurrency = concurrency
	bmg.bm = newBTreeMap(f.dirSz)
	bmg.roots = make([]fs.Directory, 0, chainLen+1)
}

func (bmg *blockMapGenerator) Run(ctx context.Context, rootEntry fs.Directory) error {
	if bmg.roots == nil {
		return ErrInternalError
	}

	var err error

	if err = bmg.findRoots(ctx, rootEntry, bmg.chainLen); err == nil {
		for i := len(bmg.roots); i > 0; i-- {
			if err = bmg.processChain(ctx, bmg.roots[i-1], i); err != nil {
				break
			}
		}
	}

	return err
}

func (bmg *blockMapGenerator) findRoots(ctx context.Context, rootEntry fs.Directory, chainIdx int) error {
	var curRoot, prevRoot fs.Directory

	entries, err := rootEntry.Readdir(ctx)
	if err != nil {
		bmg.f.logger.Debugf("rootEntry[%d] readdir: %v", chainIdx, err)
		return err
	}

	for _, ent := range entries {
		de, ok := ent.(fs.Directory)
		if !ok {
			continue
		}

		switch de.Name() {
		case currentSnapshotDirName:
			curRoot = de
		case previousSnapshotDirName:
			prevRoot = de
		}
	}

	if curRoot == nil {
		bmg.f.logger.Debugf("rootEntry[%d] cur missing", chainIdx)

		return ErrInvalidSnapshot
	}

	bmg.roots = append(bmg.roots, curRoot)

	if prevRoot == nil {
		return nil
	}

	return bmg.findRoots(ctx, prevRoot, chainIdx-1)
}

// processChain traverses the filesystem hierarchy of a chain in parallel.
// Note: We cannot use the kopia snapshot_tree_walker to descend the filesystem trees
// because the path from the root to the file encodes the address.
func (bmg *blockMapGenerator) processChain(ctx context.Context, rootEntry fs.Directory, chainIdx int) error {
	bmg.f.logger.Debugf("process chain[%d] concurrency:%d", chainIdx, bmg.concurrency)

	bmg.queue = parallelwork.NewQueue()
	bmg.curChain = chainIdx

	bmg.enqueue(ctx, rootEntry, nil)

	return bmg.queue.Process(bmg.concurrency)
}

func (bmg *blockMapGenerator) enqueue(ctx context.Context, de fs.Directory, pp parsedPath) {
	el := &bmQueueElement{
		bmg: bmg,
		ctx: ctx,
		pp:  pp,
		de:  de,
	}
	bmg.queue.EnqueueBack(el.processDir)
}

type bmQueueElement struct {
	bmg *blockMapGenerator
	ctx context.Context
	pp  parsedPath
	de  fs.Directory
}

func (bqe *bmQueueElement) processDir() error {
	pp := bqe.pp
	f := bqe.bmg.f

	entries, err := bqe.de.Readdir(bqe.ctx)
	if err != nil {
		f.logger.Debugf("Chain[%d] %s readdir: %v", bqe.bmg.curChain, pp.String(), err)

		return err
	}

	for _, ent := range entries {
		de, ok := ent.(fs.Directory)
		if !ok {
			continue
		}

		dpp := pp.Child(de.Name())
		bqe.bmg.enqueue(bqe.ctx, de, dpp)
	}

	bqe.bmg.mux.Lock()
	defer bqe.bmg.mux.Unlock()

	for _, ent := range entries {
		fe, ok := ent.(fs.File)
		if !ok {
			continue
		}

		fpp := pp.Child(fe.Name())
		bam := BlockAddressMapping{f.pathToAddr(fpp), oidOf(fe)}

		f.logger.Debugf("Chain[%d] %s %x %s", bqe.bmg.curChain, fpp.String(), bam.BlockAddr, bam.Oid)
		bqe.bmg.bm.InsertOrReplace(bam)
	}

	return nil
}

func oidOf(entry fs.Entry) object.ID {
	return entry.(object.HasObjectID).ObjectID()
}
