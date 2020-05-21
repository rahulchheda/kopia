package blockfile

import (
	"context"
	"io"
	"os"
	"path"
	"testing"

	"github.com/kopia/kopia/volume"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestGetBlockWriter(t *testing.T) {
	assert := assert.New(t)

	pGood := &Profile{Name: "/block/file"}
	gbwGood := volume.GetBlockWriterArgs{
		VolumeID: "volID",
	}

	factory := volume.FindManager(VolumeType)
	assert.NotNil(factory)

	errTcs := []struct {
		wp  *Profile
		gbw volume.GetBlockWriterArgs
	}{
		{wp: pGood, gbw: volume.GetBlockWriterArgs{}},
		{wp: nil, gbw: gbwGood},
		{wp: &Profile{}, gbw: gbwGood},
	}
	for i, tc := range errTcs {
		arg := tc.gbw
		if tc.wp != nil {
			arg.Profile = tc.wp
		}

		bw, err := factory.GetBlockWriter(arg)
		assert.Error(err, "case %d", i)
		assert.Nil(bw, "case %d", i)
	}

	for _, tc := range []string{"default block size", "specific block size"} {
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
		bw, err := factory.GetBlockWriter(gbwGood)
		assert.NoError(err)
		assert.NotNil(bw)
		bfm, ok := bw.(*manager)
		assert.True(ok)
		assert.Equal(expBlockSize, bfm.DeviceBlockSizeBytes)
		assert.Equal(pGood.Name, bfm.Name)
		assert.Nil(bfm.logger)
		assert.Zero(bfm.count)
		assert.Nil(bfm.file)
		assert.NotNil(bfm.ioChan)
	}
}

// nolint:wsl,gocritic
func TestPutBlocks(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	factory := volume.FindManager(VolumeType)
	assert.NotNil(factory)

	tmpFile := path.Join(os.TempDir(), "testVolume")
	defer func() {
		os.Remove(tmpFile)
	}()
	os.Remove(tmpFile)

	assert.NotNil(openFile)
	savedOpenFile := openFile
	defer func() {
		openFile = savedOpenFile
	}()

	pGood := &Profile{Name: tmpFile, CreateIfMissing: true}
	gbwGood := volume.GetBlockWriterArgs{
		VolumeID: "volID",
		Profile:  pGood,
	}

	for _, tc := range []string{
		"init: file open", "init: open", "bp run", "file close", "no error",
	} {
		t.Logf("Case: %s", tc)

		bw, err := factory.GetBlockWriter(gbwGood)
		assert.NoError(err)
		assert.NotNil(bw)
		bfm, ok := bw.(*manager)
		assert.True(ok)

		tdf := &testDevFiler{}
		tof := &testOpenFile{}
		openFile = tof.OpenFile
		tof.retFile = tdf
		bi := &testBlockIterator{}

		var expError error

		switch tc {
		case "init: file open":
			bfm.file = tdf
			expError = ErrInUse
		case "init: open":
			expError = ErrInvalidArgs
			tof.retError = expError
			tof.retFile = nil
		case "bp run":
			expError = volume.ErrInvalidArgs
			bfm.Concurrency = -1
		case "file close":
			expError = ErrInvalidSize
			tdf.retCloseErr = expError
		}

		assert.Nil(bfm.bp.Iter)
		err = bfm.PutBlocks(ctx, bi)

		if expError == nil {
			assert.NoError(err)
			assert.Nil(bfm.file)
			bufPtr := bfm.getBuffer()
			assert.NotNil(bufPtr)
			assert.Len(*bufPtr, int(bfm.DeviceBlockSizeBytes))
			bfm.releaseBuffer(bufPtr)
		} else {
			assert.Equal(expError, err)
			if tc != "init: file open" {
				assert.Nil(bfm.file)
			}
		}
	}
}

// nolint:wsl,gocritic
func TestBlockWriter(t *testing.T) {
	assert := assert.New(t)

	factory := volume.FindManager(VolumeType)
	assert.NotNil(factory)

	tmpFile := path.Join(os.TempDir(), "testVolume")
	defer func() {
		os.Remove(tmpFile)
	}()
	os.Remove(tmpFile)

	assert.NotNil(openFile)
	savedOpenFile := openFile
	defer func() {
		openFile = savedOpenFile
	}()

	pGood := &Profile{Name: tmpFile, CreateIfMissing: true}
	gbwGood := volume.GetBlockWriterArgs{
		VolumeID: "volID",
		Profile:  pGood,
	}

	for _, tc := range []string{
		"block get", "read", "empty and beyond buffer", "writeAt", "success", "close", "cancel",
	} {
		t.Logf("Case: %s", tc)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		bw, err := factory.GetBlockWriter(gbwGood)
		assert.NoError(err)
		assert.NotNil(bw)
		bfm, ok := bw.(*manager)
		assert.True(ok)

		tdf := &testDevFiler{}
		tof := &testOpenFile{}
		openFile = tof.OpenFile
		tof.retFile = tdf
		bi := &testBlockIterator{}

		err = bfm.writerInit(ctx, bi)
		assert.NoError(err)
		assert.Equal(tdf, bfm.file)

		trc := &testReadCloser{}
		tb := &testBlock{
			size: int(DefaultBlockSizeBytes),
		}
		tb.retGetRC = trc
		trc.retReadN = []int{tb.size}

		var expError error
		var bwh *blockWriterHelper

		switch tc {
		case "block get":
			expError = ErrInUse
			tb.retGetErr = expError
			tb.retGetRC = nil
		case "read":
			expError = ErrInvalidSize
			trc.retReadErr = []error{expError}
		case "empty and beyond buffer":
			trc.retReadN = []int{0, tb.size + 1}
			expError = ErrCapacityExceeded
		case "writeAt":
			expError = ErrInvalidSize
			tdf.retWriteAtErr = expError
		case "success":
			trc.retReadN = []int{tb.size, 0}
			trc.retReadErr = []error{nil, io.EOF}
		case "close":
			trc.retReadN = []int{tb.size, 0}
			trc.retReadErr = []error{nil, io.EOF}
			expError = ErrCanceled
			trc.retCloseErr = expError
		case "cancel":
			bwh = &blockWriterHelper{}
			bwh.m = bfm
			bwh.ctx = ctx
			bwh.m.ioChan <- struct{}{} // acquire sem
			expError = ErrCanceled
			cancel()
		}

		if bwh == nil {
			err = bfm.blockWriter(ctx, tb)
		} else {
			err = bwh.copyBlockToFile()
		}

		if expError == nil {
			assert.NoError(err)
		} else {
			assert.Equal(expError, err)
		}

		// post-mortem
		if tc == "canceled" {
			<-bwh.m.ioChan // release sem
		}

	}
}
