package volumefs

import (
	"fmt"
	"testing"

	"github.com/kopia/kopia/repo/object"
	vmgr "github.com/kopia/kopia/volume/fake"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestBlockIter(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	profile := &vmgr.ReaderProfile{
		Ranges: []vmgr.BlockAddrRange{},
	}

	f := th.fsForBackupTests(profile)
	f.logger = log(ctx)

	// prime the block map iter with some values and run it in the background
	bmi := &blockMapIter{}
	bmi.init()

	go func() {
		for i := 0; i < 2; i++ {
			bam := BlockAddressMapping{
				BlockAddr: int64(i),
				Oid:       object.ID(fmt.Sprintf("%d", i)),
			}

			if !bmi.trySend(bam) {
				break
			}
		}

		close(bmi.mChan)
	}()

	// test the block iter
	f.maxBlocks = 1000
	f.blockSzB = 4096
	assert.NotNil(f.blockPool.New)

	// constructor and init
	bi := f.newBlockIter(bmi)
	assert.Equal(f, bi.f)
	assert.Equal(bmi, bi.bmi)
	assert.Equal(int64(-1), bi.MaxBlockAddr)
	assert.Equal(f.maxBlocks+1, bi.MinBlockAddr)
	assert.Equal(0, bi.NumBlocks)

	// next (min block)
	b := bi.Next(ctx)
	assert.NotNil(b)
	assert.Equal(int64(0), b.Address())
	assert.Equal(f.blockSzB, b.Size())
	b.Release()
	assert.False(bi.AtEnd())
	assert.Equal(int64(0), bi.MinBlockAddr)
	assert.Equal(int64(0), bi.MaxBlockAddr)
	assert.Equal(1, bi.NumBlocks)

	// next (max block)
	b = bi.Next(ctx)
	assert.NotNil(b)
	assert.Equal(int64(1), b.Address())
	assert.Equal(f.blockSzB, b.Size())
	b.Release()
	assert.False(bi.AtEnd())
	assert.Equal(int64(0), bi.MinBlockAddr)
	assert.Equal(int64(1), bi.MaxBlockAddr)
	assert.Equal(2, bi.NumBlocks)

	// eof
	b = bi.Next(ctx)
	assert.Nil(b)
	assert.True(bi.AtEnd())
	assert.Equal(int64(0), bi.MinBlockAddr)
	assert.Equal(int64(1), bi.MaxBlockAddr)
	assert.Equal(2, bi.NumBlocks)

	// close
	assert.NoError(bi.Close())
	assert.True(bi.AtEnd())
	assert.NoError(bi.Close())
}
