package blockfile

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/kopia/kopia/volume"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestGetBlockReader(t *testing.T) {
	assert := assert.New(t)

	pGood := &Profile{Name: "/block/file"}
	gbwGood := volume.GetBlockReaderArgs{
		VolumeID:   "volID",
		SnapshotID: "snapID",
	}
	gbwCBT := volume.GetBlockReaderArgs{
		VolumeID:           "volID",
		SnapshotID:         "snapID",
		PreviousSnapshotID: "prevSnapID",
	}

	factory := volume.FindManager(VolumeType)
	assert.NotNil(factory)

	errTcs := []struct {
		wp  *Profile
		gbw volume.GetBlockReaderArgs
	}{
		{wp: pGood, gbw: volume.GetBlockReaderArgs{}},
		{wp: nil, gbw: gbwGood},
		{wp: &Profile{}, gbw: gbwGood},
		{wp: pGood, gbw: gbwCBT},
	}
	for i, tc := range errTcs {
		arg := tc.gbw
		if tc.wp != nil {
			arg.Profile = tc.wp
		}

		bw, err := factory.GetBlockReader(arg)
		assert.Error(err, "case %d", i)
		assert.Nil(bw, "case %d", i)
	}

	for _, tc := range []string{"default block size", "specific block size"} {
		t.Logf("Case: %s", tc)
		pGood := &Profile{Name: "/block/file"}
		expBlockSize := DefaultBlockSizeBytes

		switch tc {
		case "specific block size":
			expBlockSize = DefaultBlockSizeBytes * 2
			pGood.DeviceBlockSizeBytes = expBlockSize
		}

		gbwGood.Profile = pGood
		assert.NoError(gbwGood.Validate())
		assert.NoError(pGood.Validate())
		bw, err := factory.GetBlockReader(gbwGood)
		assert.NoError(err)
		assert.NotNil(bw)
		bfm, ok := bw.(*manager)
		assert.True(ok)
		assert.Equal(expBlockSize, bfm.DeviceBlockSizeBytes)
		assert.Equal(pGood.Name, bfm.Name)
		assert.Nil(bfm.logger)
		assert.Zero(bfm.count)
		assert.Nil(bfm.file)
	}
}

// nolint:wsl,gocritic
func TestGetBlockAddressesFakeFile(t *testing.T) {
	assert := assert.New(t)

	assert.NotNil(openFile)
	savedOpenFile := openFile
	defer func() {
		openFile = savedOpenFile
	}()

	bs := 8192
	nonZeroBlock := make([]byte, bs)
	nonZeroBlock[0] = 'x'
	zeroBlock := make([]byte, bs)
	shortEndBlock := make([]byte, bs/2)

	for _, tc := range []string{"open-failure", "stat-failure", "zero-blocks",
		"b-0-b-b", "0-b-0-s", "read-failure"} {
		t.Logf("Case: %s", tc)

		ctx := context.Background()
		bfm := &manager{}
		bfm.DeviceBlockSizeBytes = int64(bs)
		bfm.Name = "foo"

		tdf := &testDevFiler{}
		tof := &testOpenFile{}
		tfi := &testFileInfo{}
		openFile = tof.OpenFile
		tof.retFile = tdf
		tdf.retStatFI = tfi
		tfi.retSz = 2 * int64(bs)

		var expError error

		switch tc {
		case "open-failure":
			expError = ErrCanceled
			tof.retError = expError
			tof.retFile = nil
		case "stat-failure":
			expError = ErrProfileMissing
			tdf.retStatErr = expError
			tdf.retStatFI = nil
		case "b-0-b-b":
			tfi.retSz = 4 * int64(bs)
			tdf.retReadBufs = append(tdf.retReadBufs, nonZeroBlock, zeroBlock, nonZeroBlock, nonZeroBlock)
		case "0-b-0-s":
			tfi.retSz = 3*int64(bs) + int64(len(shortEndBlock))
			tdf.retReadBufs = append(tdf.retReadBufs, zeroBlock, nonZeroBlock, zeroBlock, shortEndBlock)
			tdf.retReadErrs = append(tdf.retReadErrs, nil, nil, nil, io.EOF)
		case "read-failure":
			expError = ErrCapacityExceeded
			tdf.retReadErrs = append(tdf.retReadErrs, expError)
		}

		iter, err := bfm.GetBlocks(ctx)

		var bi *blockIterator
		var ok bool

		if expError == nil {
			assert.NoError(err)
			assert.NotNil(iter)
			bi, ok = iter.(*blockIterator)
			assert.True(ok)
			assert.NotNil(bi.iterBlocks)
			assert.Equal(os.O_RDONLY, tof.inFlags)
		} else {
			assert.Equal(expError, err)
			assert.Nil(iter)
		}

		switch tc {
		case "zero-blocks":
			assert.Len(bi.iterBlocks, 0)
		case "b-0-b-b":
			assert.Len(bi.iterBlocks, 3)
		case "0-b-0-s":
			assert.Len(bi.iterBlocks, 2)
		}
	}
}

// nolint:wsl,gocritic
func TestGetBlockAddressesRealFile(t *testing.T) {
	assert := assert.New(t)

	tmpFile := path.Join(os.TempDir(), "testVolume")
	defer func() {
		os.Remove(tmpFile)
	}()

	bs := 4096
	buf := directio.AlignedBlock(bs)
	buf[0] = 'x'

	for _, tc := range []string{"b-b-b-b", "b-b-b", "zero-blocks"} {
		t.Logf("Case: %s", tc)

		ctx := context.Background()
		bfm := &manager{}
		bfm.DeviceBlockSizeBytes = 2 * int64(bs)
		bfm.Name = tmpFile

		ndb := 0

		switch tc {
		case "b-b-b-b":
			ndb = 4
		case "b-b-b":
			ndb = 3
		}

		os.Remove(tmpFile)
		f, err := openFile(tmpFile, os.O_WRONLY|os.O_CREATE, 0600)
		assert.NoError(err)
		for i := 0; i < ndb; i++ {
			n, wErr := f.WriteAt(buf, int64(i*bs))
			assert.NoError(wErr)
			assert.Equal(bs, n)
		}
		err = f.Close()
		assert.NoError(err)

		iter, err := bfm.GetBlocks(ctx)

		var bi *blockIterator
		var ok bool

		assert.NoError(err)
		assert.NotNil(iter)
		bi, ok = iter.(*blockIterator)
		assert.True(ok)
		assert.NotNil(bi.iterBlocks)

		switch tc {
		case "zero-blocks":
			assert.Len(bi.iterBlocks, 0)
		case "b-b-b-b":
			assert.Len(bi.iterBlocks, ndb/2, "got len %d", len(bi.iterBlocks))
			assert.False(bi.AtEnd())
		case "b-b-b":
			assert.Len(bi.iterBlocks, ndb/2+1, "got len %d", len(bi.iterBlocks))
		}
	}
}

// nolint:wsl,gocritic
func TestGetBlock(t *testing.T) {
	assert := assert.New(t)

	assert.NotNil(openFile)
	savedOpenFile := openFile
	defer func() {
		openFile = savedOpenFile
	}()

	bs := 8192
	ctx := context.Background()
	bfm := &manager{}
	bfm.DeviceBlockSizeBytes = int64(bs)
	bfm.Name = "foo"

	tdf := &testDevFiler{}
	tof := &testOpenFile{}
	openFile = tof.OpenFile
	tof.retFile = tdf

	// Case: first reader
	ba1 := int64(256)
	assert.Equal(0, bfm.count)
	rc1, err := bfm.getBlock(ctx, ba1)
	assert.NoError(err)
	assert.NotNil(rc1)

	r1, ok := rc1.(*Reader)
	assert.True(ok)
	assert.NotNil(r1)
	assert.Equal(bfm, r1.m)
	assert.Equal(ba1*bfm.DeviceBlockSizeBytes, r1.currentOffset)
	assert.EqualValues(bfm.DeviceBlockSizeBytes, r1.remainingBytes)
	assert.NotNil(r1.cancel)

	assert.Equal(1, bfm.count)
	assert.NotNil(bfm.file)

	// Case: overlapping second reader
	ba2 := int64(1024)
	assert.Equal(1, bfm.count)
	rc2, err := bfm.getBlock(ctx, ba2)
	assert.NoError(err)
	assert.NotNil(rc2)

	r2, ok := rc2.(*Reader)
	assert.True(ok)
	assert.NotNil(r2)
	assert.Equal(bfm, r2.m)
	assert.Equal(ba2*bfm.DeviceBlockSizeBytes, r2.currentOffset)
	assert.EqualValues(bfm.DeviceBlockSizeBytes, r2.remainingBytes)
	assert.NotNil(r2.cancel)

	assert.Equal(2, bfm.count)
	assert.NotNil(bfm.file)

	// Case: last close works
	err = rc1.Close()
	assert.NoError(err)
	assert.Equal(1, bfm.count)
	err = rc1.Close()
	assert.NoError(err)

	err = rc2.Close()
	assert.NoError(err)
	assert.Equal(0, bfm.count)
	err = rc2.Close()
	assert.NoError(err)

	// Case: file reopened on subsequent call
	bfm.file = nil
	ba3 := int64(512)
	rc3, err := bfm.getBlock(ctx, ba3)
	assert.NoError(err)
	assert.NotNil(rc3)

	r3, ok := rc3.(*Reader)
	assert.True(ok)
	assert.NotNil(r3)
	assert.Equal(bfm, r3.m)
	assert.Equal(ba3*bfm.DeviceBlockSizeBytes, r3.currentOffset)
	assert.EqualValues(bfm.DeviceBlockSizeBytes, r3.remainingBytes)
	assert.NotNil(r3.cancel)

	assert.Equal(1, bfm.count)
	assert.NotNil(bfm.file)

	err = rc3.Close()
	assert.NoError(err)
	assert.Equal(0, bfm.count)

	// Case: open failure case
	tof.retError = fmt.Errorf("open error")
	baE := int64(4096)
	rcE, err := bfm.getBlock(ctx, baE)
	assert.Regexp("open error", err)
	assert.Nil(rcE)
}

// nolint:wsl,gocritic,goconst
func TestReader(t *testing.T) {
	assert := assert.New(t)

	assert.NotNil(openFile)
	savedOpenFile := openFile
	defer func() {
		openFile = savedOpenFile
	}()

	factory := volume.FindManager(VolumeType)
	assert.NotNil(factory)

	dbs := 4096
	pGood := &Profile{Name: "/block/file", DeviceBlockSizeBytes: int64(dbs)}
	gbwGood := volume.GetBlockReaderArgs{
		VolumeID:   "volID",
		SnapshotID: "snapID",
	}
	gbwGood.Profile = pGood
	assert.NoError(gbwGood.Validate())
	assert.NoError(pGood.Validate())

	// rwCommon cases are done in TestWriter
	for _, tc := range []string{
		"nil buffer",
		"invalid size",
		"at eof",
		"read error",
		"short last block",
		"full block",
		"canceled",
	} {
		t.Logf("Case: %s", tc)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tdf := &testDevFiler{}
		tof := &testOpenFile{}
		openFile = tof.OpenFile
		tof.retFile = tdf

		bw, err := factory.GetBlockReader(gbwGood)
		assert.NoError(err)
		assert.NotNil(bw)
		bfm, ok := bw.(*manager)
		assert.True(ok)
		bfm.Name = "foo"
		assert.Equal(dbs, int(bfm.DeviceBlockSizeBytes))

		ba1 := int64(256)
		assert.Equal(0, bfm.count)
		rc, err := bfm.getBlock(ctx, ba1)
		assert.NoError(err)
		assert.NotNil(rc)

		r, ok := rc.(*Reader)
		assert.True(ok)
		assert.NotNil(r)
		assert.NotNil(r.buf)
		assert.Len(r.buf, dbs, "got %d expected %d", len(r.buf), dbs)

		r.numCalls = 0
		r.currentOffset = 0
		r.remainingBytes = int(bfm.DeviceBlockSizeBytes)

		var expError error
		b := make([]byte, dbs)
		expOffset := dbs
		expN := dbs
		expRemB := 0

		// pre-processing
		switch tc {
		case "nil buffer":
			expError = ErrInvalidSize
			b = nil
		case "invalid size":
			b = make([]byte, dbs-1)
			expError = ErrInvalidSize
		case "at eof":
			r.remainingBytes = 0
			expError = io.EOF
		case "read error":
			expError = ErrProfileMissing
			tdf.retReadErrs = append(tdf.retReadErrs, expError)
		case "short last block":
			tdf.retReadErrs = append(tdf.retReadErrs, io.EOF)
			tdf.retReadBufs = append(tdf.retReadBufs, make([]byte, dbs/2))
			expN = dbs / 2
			expOffset = dbs / 2
			expRemB = 0
		case "full block":
			tdf.retReadBufs = append(tdf.retReadBufs, make([]byte, dbs))
		case "canceled":
			r.m.ioChan <- struct{}{} // acquire sem
			expError = ErrCanceled
			cancel()
		}

		n, err := rc.Read(b)

		// post-processing
		if expError != nil {
			assert.Error(err, tc)
			assert.Equal(expError, err)
			assert.Zero(n, tc)
		} else {
			assert.NoError(err, tc)
			assert.Equal(expN, n)
			assert.Equal(1, tdf.numReadAt)
			assert.EqualValues(expOffset, r.currentOffset)
			assert.Equal(expRemB, r.remainingBytes)
		}

		err = rc.Close()
		assert.NoError(err)

		// post-mortem
		if tc == "canceled" {
			<-r.m.ioChan // release sem
		}
	}
}

// nolint:wsl,gocritic,gocyclo
func TestBlockIterator(t *testing.T) {
	assert := assert.New(t)

	assert.NotNil(openFile)
	savedOpenFile := openFile
	defer func() {
		openFile = savedOpenFile
	}()

	for _, tc := range []string{
		"empty", "1 block", "1 block Get error", "block concurrency", "iter error", "iter empty after close",
	} {

		var blocks []int64
		var expCloseErr error

		bs := 8192
		ctx := context.Background()
		bfm := &manager{}
		bfm.DeviceBlockSizeBytes = int64(bs)
		bfm.Name = "foo"

		tdf := &testDevFiler{}
		tof := &testOpenFile{}
		openFile = tof.OpenFile
		tof.retFile = tdf

		switch tc {
		case "empty":
			blocks = []int64{}
		case "1 block":
			blocks = []int64{5}
		case "1 block Get error":
			blocks = []int64{9}
			tof.retFile = nil
			tof.retError = ErrInvalidSize
		case "block concurrency":
			blocks = []int64{0, 1, 2, 3}
		case "iter error":
			blocks = []int64{0, 1, 2}
		case "iter empty after close":
			blocks = []int64{1}
		}

		bi := bfm.newBlockIterator(blocks)
		assert.Equal(bfm, bi.m)
		assert.Equal(blocks, bi.iterBlocks)
		assert.Equal(0, bi.iterCurrent)

		switch tc {
		case "empty":
			b := bi.Next(ctx)
			assert.Nil(b)
		case "1 block":
			b := bi.Next(ctx)
			assert.NotNil(b)
			assert.False(bi.AtEnd())
			assert.Equal(blocks[0], b.Address())
			assert.EqualValues(bfm.DeviceBlockSizeBytes, b.Size())
			assert.Equal(1, bi.iterCurrent)
			rc, err := b.Get(ctx)
			assert.NoError(err)
			assert.Equal(tdf, bfm.file)
			r, ok := rc.(*Reader)
			assert.True(ok)
			assert.Equal(b.Address()*bfm.DeviceBlockSizeBytes, r.currentOffset)
			b.Release()

			b = bi.Next(ctx)
			assert.Nil(b)
			assert.True(bi.AtEnd())
		case "1 block Get error":
			b := bi.Next(ctx)
			assert.NotNil(b)
			assert.Equal(blocks[0], b.Address())
			assert.EqualValues(bfm.DeviceBlockSizeBytes, b.Size())
			assert.Equal(1, bi.iterCurrent)

			rc, err := b.Get(ctx)
			assert.Equal(tof.retError, err)
			assert.Nil(rc)
			b.Release()

			b = bi.Next(ctx)
			assert.Nil(b)
		case "block concurrency":
			wg := sync.WaitGroup{}
			assert.True(len(blocks)-2 > 0)
			wg.Add(len(blocks) - 2)
			for i := 0; i < len(blocks)-2; i++ {
				b := bi.Next(ctx)
				assert.NotNil(b)

				go func(b volume.Block) {
					time.Sleep(2 * time.Millisecond)
					b.Release()
					wg.Done()
				}(b)
				time.Sleep(time.Millisecond)
			}
			wg.Wait()
			for b := bi.Next(ctx); b != nil; b = bi.Next(ctx) {
				b.Release()
			}
		case "iter error":
			assert.True(len(bi.iterBlocks) > 1)
			b := bi.Next(ctx)
			assert.NotNil(b)
			b.Release()
			expCloseErr = ErrCanceled
			bi.iterError = expCloseErr
			assert.Nil(bi.Next(ctx))
		case "iter empty after close":
			assert.True(len(bi.iterBlocks) > 0)
			bi.Close()
			assert.Nil(bi.Next(ctx))
		}

		closeErr := bi.Close()
		assert.Equal(expCloseErr, closeErr)
		closeErr = bi.Close()               // same behavior
		assert.Equal(expCloseErr, closeErr) // if called again
	}
}
