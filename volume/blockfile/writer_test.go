package blockfile

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"testing"
	"time"

	"github.com/kopia/kopia/volume"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestGetBlockWriter(t *testing.T) {
	assert := assert.New(t)

	pGood := &Profile{Name: "/block/file"}
	gbwGood := volume.GetBlockWriterArgs{
		VolumeID:       "volID",
		BlockSizeBytes: int64(1024 * 1024),
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
		assert.Equal(gbwGood.BlockSizeBytes, bfm.fsBlockSizeBytes)
		assert.Nil(bfm.logger)
		assert.Zero(bfm.count)
		assert.Nil(bfm.file)
		assert.NotNil(bfm.ioChan)

		// block allocation
		b := bw.AllocateBuffer()
		assert.NotNil(b)
		assert.Len(b, int(bfm.DeviceBlockSizeBytes))
	}
}

// nolint:wsl,gocritic
func TestPutBlock(t *testing.T) {
	assert := assert.New(t)

	ctx := context.Background()
	factory := volume.FindManager(VolumeType)
	assert.NotNil(factory)

	tmpFile := path.Join(os.TempDir(), "testVolume")
	defer func() {
		os.Remove(tmpFile)
	}()
	os.Remove(tmpFile)

	pGood := &Profile{Name: tmpFile, CreateIfMissing: true}
	gbwGood := volume.GetBlockWriterArgs{
		VolumeID:       "volID",
		BlockSizeBytes: int64(1024 * 1024),
		Profile:        pGood,
	}

	bw, err := factory.GetBlockWriter(gbwGood)
	assert.NoError(err)
	assert.NotNil(bw)
	bfm, ok := bw.(*manager)
	assert.True(ok)
	bfm.fsBlockSizeBytes = 1024 * 1024

	// Case: first writer
	ba1 := int64(256)
	wc1, err := bw.PutBlock(ctx, ba1)
	assert.NoError(err)
	assert.NotNil(wc1)

	w1, ok := wc1.(*Writer)
	assert.True(ok)
	assert.NotNil(w1)
	assert.Equal(bfm, w1.m)
	assert.Equal(ba1*bfm.fsBlockSizeBytes, w1.currentOffset)
	assert.EqualValues(bfm.fsBlockSizeBytes, w1.remainingBytes)
	assert.NotNil(w1.cancel)

	assert.Equal(1, bfm.count)
	assert.NotNil(bfm.file)

	// Case: overlapping second writer
	ba2 := int64(1024)
	wc2, err := bw.PutBlock(ctx, ba2)
	assert.NoError(err)
	assert.NotNil(wc2)

	w2, ok := wc2.(*Writer)
	assert.True(ok)
	assert.NotNil(w2)
	assert.Equal(bfm, w2.m)
	assert.Equal(ba2*bfm.fsBlockSizeBytes, w2.currentOffset)
	assert.EqualValues(bfm.fsBlockSizeBytes, w2.remainingBytes)
	assert.NotNil(w2.cancel)

	assert.Equal(2, bfm.count)
	assert.NotNil(bfm.file)

	// Case: last close works
	err = wc1.Close()
	assert.NoError(err)
	assert.Equal(1, bfm.count)
	err = wc1.Close()
	assert.NoError(err)

	err = wc2.Close()
	assert.NoError(err)
	assert.Equal(0, bfm.count)
	err = wc2.Close()
	assert.NoError(err)

	// Case: file reopened on subsequent call
	bfm.file = nil
	ba3 := int64(512)
	wc3, err := bw.PutBlock(ctx, ba3)
	assert.NoError(err)
	assert.NotNil(wc3)

	w3, ok := wc3.(*Writer)
	assert.True(ok)
	assert.NotNil(w3)
	assert.Equal(bfm, w3.m)
	assert.Equal(ba3*bfm.fsBlockSizeBytes, w3.currentOffset)
	assert.EqualValues(bfm.fsBlockSizeBytes, w3.remainingBytes)
	assert.NotNil(w3.cancel)

	assert.Equal(1, bfm.count)
	assert.NotNil(bfm.file)

	err = wc3.Close()
	assert.NoError(err)
	assert.Equal(0, bfm.count)

	// Case: open failure case
	assert.NotNil(openFile)
	savedOpenFile := openFile
	defer func() {
		openFile = savedOpenFile
	}()
	tof := &testOpenFile{}
	openFile = tof.OpenFile
	tof.retError = fmt.Errorf("open error")
	baE := int64(4096)
	wcE, err := bw.PutBlock(ctx, baE)
	assert.Regexp("open error", err)
	assert.Nil(wcE)
}

// nolint:wsl,gocritic,goconst
func TestWriter(t *testing.T) {
	assert := assert.New(t)

	factory := volume.FindManager(VolumeType)
	assert.NotNil(factory)

	assert.NotNil(openFile)
	savedOpenFile := openFile
	defer func() {
		openFile = savedOpenFile
	}()

	tdf := &testDevFiler{}
	tof := &testOpenFile{}
	openFile = tof.OpenFile
	tof.retFile = tdf

	pGood := &Profile{Name: "foo", CreateIfMissing: true}
	gbwGood := volume.GetBlockWriterArgs{
		VolumeID:       "volID",
		BlockSizeBytes: int64(1024 * 1024),
		Profile:        pGood,
	}

	bw, err := factory.GetBlockWriter(gbwGood)
	assert.NoError(err)
	assert.NotNil(bw)
	bfm, ok := bw.(*manager)
	assert.True(ok)
	bfm.fsBlockSizeBytes = 1024 * 1024
	assert.NotNil(bfm.ioChan)

	alignedBuf := bfm.AllocateBuffer()
	assert.NotNil(alignedBuf)
	t.Logf("AllocateBuffer(%d): %d", gbwGood.BlockSizeBytes, len(alignedBuf))

	// test cases
	for _, tc := range []string{"nil buffer", "invalid size", "range exceeded", "blocks on channel", "canceled"} {
		t.Logf("Case: %s", tc)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ba1 := int64(256)
		wc, err := bw.PutBlock(ctx, ba1)
		assert.NoError(err)
		assert.NotNil(wc)
		assert.Equal(tdf, bfm.file)
		assert.NotNil(bfm.logger)
		assert.Equal(1, bfm.count)

		w, ok := wc.(*Writer)
		assert.True(ok)
		assert.NotNil(w)

		tdf.retCloseErr = nil
		tdf.retWriteAtErr = nil
		tdf.numWriteAt = 0
		w.numCalls = 0
		w.currentOffset = 0
		w.remainingBytes = int(bfm.fsBlockSizeBytes)
		assert.Len(w.m.ioChan, 0, tc)

		var n int
		b := alignedBuf
		expSize := len(alignedBuf)
		var expError error
		var expCloseError error

		// pre-processing
		switch tc {
		case "nil buffer":
			expError = ErrInvalidSize
			b = nil
		case "invalid size":
			b = make([]byte, bfm.DeviceBlockSizeBytes-1)
			expError = ErrInvalidSize
		case "range exceeded":
			b = alignedBuf
			w.remainingBytes = 0
			expError = ErrCapacityExceeded
		case "blocks on channel":
			w.m.ioChan <- struct{}{} // acquire sem
			tdf.retWriteAtN = expSize
		case "canceled":
			w.m.ioChan <- struct{}{} // acquire sem
			expError = ErrCanceled
			expCloseError = ErrCanceled
			tdf.retCloseErr = expCloseError
		}

		// launch Write in a goroutine
		wg := sync.WaitGroup{}
		wg.Add(1)
		callingWrite := false
		go func() {
			callingWrite = true
			n, err = wc.Write(b)
			wg.Done()
		}()

		for !callingWrite {
			time.Sleep(5 * time.Millisecond)
		}

		// simulate concurrent activity
		switch tc {
		case "blocks on channel":
			for w.numCalls == 0 {
				time.Sleep(5 * time.Millisecond)
			}
			time.Sleep(5 * time.Millisecond)
			assert.Equal(0, tdf.numWriteAt, tc)
			<-w.m.ioChan // release sem
		case "canceled":
			cancel()
		}

		// wait for Write to return
		wg.Wait()

		// post-processing
		if expError != nil {
			assert.Error(err, tc)
			assert.Equal(expError, err)
			assert.Zero(n, tc)
		} else {
			assert.NoError(err, tc)
			assert.Equal(1, tdf.numWriteAt)
			assert.EqualValues(expSize, w.currentOffset)
			assert.Equal(int(bfm.fsBlockSizeBytes-bfm.DeviceBlockSizeBytes), w.remainingBytes)
		}

		err = wc.Close()
		assert.Equal(expCloseError, err)

		// post-mortem
		if tc == "canceled" {
			<-w.m.ioChan // release sem
		}
	}
}
