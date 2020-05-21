package volumefs

import (
	"math"
	"path"
	"strconv"
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
	maxDirEntries     = int64(256) // nolint:gomnd // must be a power of 2
	maxTreeDepth      = 4
	snapshotBlockSize = int64(1024 * 16) // nolint:gomnd // this value actually comes from the device
	baseEncoding      = 32
)

type layoutProperties struct {
	blockSzB int64
	dirSz    int64
	depth    int
	// the rest are computed or fixed
	dirSzLog2    int
	dirSzMask    int64
	maxBlocks    int64
	maxVolSzB    int64
	baseEncoding int
}

func (f *Filesystem) setDefaultLayoutProperties() {
	f.layoutProperties.initLayoutProperties(snapshotBlockSize, maxDirEntries, maxTreeDepth)
}

func (l *layoutProperties) initLayoutProperties(snapshotBlockSize, maxDirEntries int64, maxTreeDepth int) {
	l.blockSzB = snapshotBlockSize
	l.dirSz = maxDirEntries
	l.depth = maxTreeDepth
	l.dirSzLog2 = int(math.Log2(float64(maxDirEntries)))
	l.dirSzMask = maxDirEntries - 1
	l.maxBlocks = 1 << (maxTreeDepth * l.dirSzLog2)
	l.maxVolSzB = l.maxBlocks * l.blockSzB
	l.baseEncoding = baseEncoding
}

// GetBlockSize returns the snapshot block size
func (f *Filesystem) GetBlockSize() int64 {
	return f.blockSzB
}

type parsedPath []string

func (pp parsedPath) String() string {
	return path.Join(pp...)
}

func (pp parsedPath) Last() string {
	if len(pp) > 0 {
		return pp[len(pp)-1]
	}

	return ""
}

func (pp parsedPath) Child(name string) parsedPath {
	cpp := make(parsedPath, len(pp)+1)
	for i := 0; i < len(pp); i++ {
		cpp[i] = pp[i]
	}

	cpp[len(pp)] = name

	return cpp
}

func (f *Filesystem) newParsedPath() parsedPath {
	return make(parsedPath, 0, f.depth)
}

// addrToPath returns the parsed path for a block file relative to the block map root directory.
func (f *Filesystem) addrToPath(blockAddr int64) (parsedPath, error) {
	if blockAddr >= f.maxBlocks || blockAddr < 0 {
		return nil, ErrOutOfRange
	}

	pp := f.newParsedPath()

	for i := f.depth - 1; i >= 0; i-- {
		idx := (blockAddr >> (f.dirSzLog2 * i)) & f.dirSzMask
		el := strconv.FormatInt(idx, f.baseEncoding)
		pp = append(pp, el)
	}

	return pp, nil
}

// pathToAddr returns the address from a parsed block file path
func (f *Filesystem) pathToAddr(pp parsedPath) (blockAddr int64) {
	for i := 0; i < len(pp); i++ {
		el, _ := strconv.ParseInt(pp[i], f.baseEncoding, 64)

		blockAddr <<= f.dirSzLog2
		blockAddr |= el & f.dirSzMask
	}

	return
}

// lookupDir searches for a directory in a directory hierarchy
func (f *Filesystem) lookupDir(pDir *dirMeta, pp parsedPath) *dirMeta {
	for i := 0; i < len(pp); i++ {
		sdm := pDir.findSubdir(pp[i])
		if sdm == nil {
			f.logger.Debugf("dir not found [%s]", pp[:i])
			return nil
		}

		pDir = sdm
	}

	return pDir
}

// lookupFile searches for a file in the directory hierarchy
func (f *Filesystem) lookupFile(pDir *dirMeta, pp parsedPath) *fileMeta {
	fileIdx := len(pp) - 1
	if pDir = f.lookupDir(pDir, pp[:fileIdx]); pDir != nil {
		if fm := pDir.findFile(pp[fileIdx]); fm != nil {
			return fm
		}

		f.logger.Debugf("file not found [%s]", pp)
	}

	return nil
}

// ensureFile adds a new file to the specified directory hierarchy if it does not exist.
// It returns the fileMeta.
func (f *Filesystem) ensureFileInTree(pDir *dirMeta, pp parsedPath) *fileMeta {
	fileIdx := len(pp) - 1

	for i := 0; i < fileIdx; i++ {
		sdm := pDir.findSubdir(pp[i])
		if sdm == nil {
			sdm = &dirMeta{
				name: pp[i],
			}

			pDir.insertSubdir(sdm)
			f.logger.Debugf("inserted directory(%s)", pp[:i+1])
		}

		pDir = sdm
	}

	fm := pDir.findFile(pp[fileIdx])
	if fm == nil {
		fm = &fileMeta{
			name: pp[fileIdx],
		}

		pDir.insertFile(fm)
		f.logger.Debugf("inserted file(%s)", pp)

		return fm
	}

	return fm
}
