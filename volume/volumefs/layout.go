package volumefs

import (
	"context"
	"fmt"
	"math"
	"path"
)

// The file system is organized into a symmetric tree based on block address.
// See https://kasten.atlassian.net/wiki/spaces/EN/pages/908820586/WIP+Kopia+snapshots+with+volume+CBT+APIs+utilizing+a+filesystem+interface#Symmetrical-tree
// for details on the number of blocks that can be tracked. e.g.
//   Depth     N=128      N=256
//     3         2Mi      16Mi
//     4       256Mi    4096Mi
//
// There is a fixed number of blocks that a symmetric tree can track. To preserve the
// ability to increase the number of blocks in the future while still remaining backward
// compatible we add extra address space potential to top-level directory entries by adding
// leading zeros to their name.  Then in the future we have the choice to add additional
// trees of depth maxTreeDepth-1 or introduce indirect blocks for additional trees of
// depth maxTreeDepth.  See the unit test for illustration.

const (
	maxDirEntries     = int64(256) // must be a power of 2
	maxTreeDepth      = 3
	snapshotBlockSize = int64(1024 * 1024)
	extraTopHexFmtLen = 1
	fileHexFmtLen     = 12 // counts up to 2^48
)

type layoutProperties struct {
	blockSzB int64
	dirSz    int64
	depth    int
	// the rest are computed
	dirSzL2         int
	dirSzMask       int64
	maxBlocks       int64
	maxVolSzB       int64
	dirHexFmtLen    int
	dirTopHexFmtLen int
	fileHexFmtLen   int
}

func (f *Filesystem) setDefaultLayoutProperties() {
	f.layoutProperties.initLayoutProperties(snapshotBlockSize, maxDirEntries, maxTreeDepth)
}

func (l *layoutProperties) initLayoutProperties(snapshotBlockSize, maxDirEntries int64, maxTreeDepth int) {
	l.blockSzB = snapshotBlockSize
	l.dirSz = maxDirEntries
	l.depth = maxTreeDepth
	l.dirSzL2 = int(math.Log2(float64(maxDirEntries)))
	l.dirSzMask = maxDirEntries - 1
	l.maxBlocks = 1 << (maxTreeDepth * l.dirSzL2)
	l.maxVolSzB = l.maxBlocks * l.blockSzB
	l.dirHexFmtLen = int(l.dirSzL2)/4 + (l.dirSzL2%4+3)/4
	l.dirTopHexFmtLen = l.dirHexFmtLen + extraTopHexFmtLen
	l.fileHexFmtLen = fileHexFmtLen // fixed
}

// GetBlockSize returns the snapshot block size
func (f *Filesystem) GetBlockSize() int64 {
	return f.blockSzB
}

type parsedPath []string

func (pp parsedPath) String() string {
	return path.Join(pp...)
}

// addrToPath returns the parsed path for a block file
func (f *Filesystem) addrToPath(blockAddr int64) (parsedPath, error) {
	if blockAddr >= f.maxBlocks || blockAddr < 0 {
		// @TODO: future special case blockAddr > f.maxBlocks with reserved hex digit in top dir
		// Can choose to add additional trees of depth maxTreeDepth-1 or add indirect blocks for
		// additional trees of depth maxTreeDepth.
		return nil, fmt.Errorf("block address out of range")
	}
	pp := make(parsedPath, 0, maxTreeDepth)
	for i, dirHexFmtLen := maxTreeDepth-1, f.dirTopHexFmtLen; i > 0; i, dirHexFmtLen = i-1, f.dirHexFmtLen {
		idx := (blockAddr >> (f.dirSzL2 * i)) & f.dirSzMask
		el := fmt.Sprintf("%0*x", dirHexFmtLen, idx)
		pp = append(pp, el)
	}
	pp = append(pp, fmt.Sprintf("%0*x", f.fileHexFmtLen, blockAddr))
	return pp, nil
}

// lookupDir searches for a directory in the directory hierarchy
func (f *Filesystem) lookupDir(ctx context.Context, pp parsedPath) *dirMeta {
	pDir := f.rootDir
	for i := 0; i < len(pp); i++ {
		sdm := pDir.findSubdir(pp[i])
		if sdm == nil {
			log(ctx).Debugf("dir not found [%s]", pp[:i])
			return nil
		}
		pDir = sdm
	}
	return pDir
}

// lookupFile searches for a file in the directory hierarchy
func (f *Filesystem) lookupFile(ctx context.Context, pp parsedPath) *fileMeta {
	fileIdx := len(pp) - 1
	if pDir := f.lookupDir(ctx, pp[:fileIdx]); pDir != nil {
		if fm := pDir.findFile(pp[fileIdx]); fm != nil {
			return fm
		}
		log(ctx).Debugf("file not found [%s]", pp)
	}
	return nil
}

// ensureFile adds a new file to the directory hierarchy or updates its mTime if it exists
func (f *Filesystem) ensureFile(ctx context.Context, pp parsedPath) {
	fileIdx := len(pp) - 1
	pDir := f.rootDir
	for i := 0; i < fileIdx; i++ {
		sdm := pDir.findSubdir(pp[i])
		if sdm == nil {
			sdm = &dirMeta{
				name:  pp[i],
				mTime: f.epoch,
			}
			pDir.insertSubdir(sdm)
			log(ctx).Debugf("inserted directory(%s)", pp[:i+1])
		}
		pDir = sdm
	}
	fm := pDir.findFile(pp[fileIdx])
	if fm == nil {
		fm = &fileMeta{
			name:  pp[fileIdx],
			mTime: f.epoch,
		}
		pDir.insertFile(fm)
		log(ctx).Debugf("inserted file(%s)", pp)
		return
	}
	fm.mTime = f.epoch
	log(ctx).Debugf("updated file(%s)", pp)
	return
}
