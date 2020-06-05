package volumefs

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/volume"
)

// BlockIterStats contains statistics on block iterator traversal
type BlockIterStats struct {
	NumBlocks    int
	MinBlockAddr int64
	MaxBlockAddr int64
}

func (bis *BlockIterStats) initStats() {
	bis.MinBlockAddr = int64((^uint64(0) >> 1))
	bis.MaxBlockAddr = -1
}

func (bis *BlockIterStats) String() string {
	if bis.NumBlocks == 0 {
		return "{}"
	}

	return fmt.Sprintf("{NumBlocks:%d,MinBlockAddr:%d,MaxBlockAddr:%d}", bis.NumBlocks, bis.MinBlockAddr, bis.MaxBlockAddr)
}

type blockIter struct {
	f     *Filesystem
	bmi   BlockMapIterator
	atEnd bool
	BlockIterStats
}

var _ volume.BlockIterator = (*blockIter)(nil)

func (f *Filesystem) newBlockIter(bmi BlockMapIterator) *blockIter {
	bi := &blockIter{f: f, bmi: bmi}
	bi.initStats()

	return bi
}

// Next returns the next block. It may return nil without implying exhaustion - AtEnd() must be checked.
func (bi *blockIter) Next(ctx context.Context) volume.Block {
	if bi.atEnd {
		return nil
	}

	bam := bi.bmi.Next()
	if bam.Oid == "" {
		bi.atEnd = true
		return nil
	}

	b := bi.f.getBlock()
	b.BlockAddressMapping = bam
	bi.NumBlocks++

	if b.BlockAddr > bi.MaxBlockAddr {
		bi.MaxBlockAddr = b.BlockAddr
	}

	if b.BlockAddr < bi.MinBlockAddr {
		bi.MinBlockAddr = b.BlockAddr
	}

	return b
}

// AtEnd returns true if the iterator has encountered an error or is exhausted.
func (bi *blockIter) AtEnd() bool {
	return bi.atEnd
}

// Close terminates the iterator and returns any error.
func (bi *blockIter) Close() error {
	bi.atEnd = true
	bi.bmi.Close()

	return nil
}
