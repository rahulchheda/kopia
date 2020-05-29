package volumefs

import (
	"context"
	"io"

	"github.com/kopia/kopia/volume"
)

type block struct {
	f *Filesystem
	BlockAddressMapping
}

var _ volume.Block = (*block)(nil)

func (f *Filesystem) getBlock() *block {
	b := f.blockPool.Get().(*block)
	b.f = f

	return b
}

func (f *Filesystem) putBlock(b *block) {
	f.blockPool.Put(b)
}

// Address returns the block address.
func (b *block) Address() int64 {
	return b.BlockAddr
}

// Size returns the size of the block in bytes.
func (b *block) Size() int {
	return b.f.blockSzB
}

// Release should be called on completion to free the resources associated with the block.
func (b *block) Release() {
	b.f.putBlock(b)
}

// Get returns a reader to read the block data.
func (b *block) Get(ctx context.Context) (io.ReadCloser, error) {
	rc, err := b.f.repo.OpenObject(ctx, b.Oid)
	if err != nil {
		b.f.logger.Debugf("block[%x] OpenObject(%s): %v", b.BlockAddr, b.Oid, err)
		return nil, err
	}

	b.f.logger.Debugf("block[%x] OpenObject(%s)", b.BlockAddr, b.Oid)

	return rc, nil
}
