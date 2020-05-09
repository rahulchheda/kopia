package blockfile

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"testing"

	"github.com/kopia/kopia/volume"

	"github.com/ncw/directio"
	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestGetBlockReader(t *testing.T) {
	assert := assert.New(t)

	pGood := &Profile{Name: "/block/file"}
	gbwGood := volume.GetBlockReaderArgs{
		VolumeID:       "volID",
		SnapshotID:     "snapID",
		BlockSizeBytes: int64(1024 * 1024),
	}
	gbwCBT := volume.GetBlockReaderArgs{
		VolumeID:           "volID",
		SnapshotID:         "snapID",
		PreviousSnapshotID: "prevSnapID",
		BlockSizeBytes:     int64(1024 * 1024),
	}

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

		m := volume.FindManager(VolumeType)
		assert.NotNil(m)

		bw, err := m.GetBlockReader(arg)
		assert.Error(err, "case %d", i)
		assert.Nil(bw, "case %d", i)
	}

	for _, tc := range []string{"default block size", "specific block size"} {
		bfm := &manager{}
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
		assert.Empty(bfm.Name)
		bw, err := bfm.GetBlockReader(gbwGood)
		assert.NoError(err)
		assert.NotNil(bw)
		assert.Equal(expBlockSize, bfm.DeviceBlockSizeBytes)
		assert.Equal(pGood.Name, bfm.Name)
		assert.Equal(gbwGood.BlockSizeBytes, bfm.fsBlockSizeBytes)
		assert.Nil(bfm.logger)
		assert.Zero(bfm.count)
		assert.Nil(bfm.file)

		// call again even with same data is not ok
		bw2, err := bfm.GetBlockReader(gbwGood)
		assert.Equal(ErrAlreadyInitialized, err)
		assert.Nil(bw2)
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
		bfm.fsBlockSizeBytes = int64(bs)
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

		bw, err := bfm.GetBlockAddresses(ctx)

		if expError == nil {
			assert.NoError(err)
			assert.NotNil(bw)
			assert.Equal(os.O_RDONLY, tof.inFlags)
		} else {
			assert.Equal(expError, err)
			assert.Nil(bw)
		}

		switch tc {
		case "zero-blocks":
			assert.Len(bw, 0)
		case "b-0-b-b":
			assert.Len(bw, 3)
		case "0-b-0-s":
			assert.Len(bw, 2)
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
		bfm.fsBlockSizeBytes = 2 * int64(bs)
		bfm.Name = tmpFile

		ndb := 0
		var expError error

		switch tc {
		case "b-b-b-b":
			ndb = 4
		case "b-b-b":
			ndb = 3
		}

		os.Remove(tmpFile)
		f, err := directio.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE, 0600)
		assert.NoError(err)
		for i := 0; i < ndb; i++ {
			n, wErr := f.WriteAt(buf, int64(i*bs))
			assert.NoError(wErr)
			assert.Equal(bs, n)
		}
		err = f.Close()
		assert.NoError(err)

		bw, err := bfm.GetBlockAddresses(ctx)

		if expError == nil {
			assert.NoError(err)
			assert.NotNil(bw)
		} else {
			assert.Equal(expError, err)
			assert.Nil(bw)
		}

		switch tc {
		case "zero-blocks":
			assert.Len(bw, 0)
		case "b-b-b-b":
			assert.Len(bw, ndb/2, "got len %d", len(bw))
		case "b-b-b":
			assert.Len(bw, ndb/2+1, "got len %d", len(bw))
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
	bfm.fsBlockSizeBytes = int64(bs)
	bfm.Name = "foo"

	tdf := &testDevFiler{}
	tof := &testOpenFile{}
	openFile = tof.OpenFile
	tof.retFile = tdf

	// Case: first reader
	ba1 := int64(256)
	assert.Equal(0, bfm.count)
	rc1, err := bfm.GetBlock(ctx, ba1)
	assert.NoError(err)
	assert.NotNil(rc1)

	r1, ok := rc1.(*Reader)
	assert.True(ok)
	assert.NotNil(r1)
	assert.Equal(bfm, r1.m)
	assert.Equal(ba1*bfm.fsBlockSizeBytes, r1.currentOffset)
	assert.EqualValues(bfm.fsBlockSizeBytes, r1.remainingBytes)
	assert.NotNil(r1.cancel)

	assert.Equal(1, bfm.count)
	assert.NotNil(bfm.file)

	// Case: overlapping second reader
	ba2 := int64(1024)
	assert.Equal(1, bfm.count)
	rc2, err := bfm.GetBlock(ctx, ba2)
	assert.NoError(err)
	assert.NotNil(rc2)

	r2, ok := rc2.(*Reader)
	assert.True(ok)
	assert.NotNil(r2)
	assert.Equal(bfm, r2.m)
	assert.Equal(ba2*bfm.fsBlockSizeBytes, r2.currentOffset)
	assert.EqualValues(bfm.fsBlockSizeBytes, r2.remainingBytes)
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
	rc3, err := bfm.GetBlock(ctx, ba3)
	assert.NoError(err)
	assert.NotNil(rc3)

	r3, ok := rc3.(*Reader)
	assert.True(ok)
	assert.NotNil(r3)
	assert.Equal(bfm, r3.m)
	assert.Equal(ba3*bfm.fsBlockSizeBytes, r3.currentOffset)
	assert.EqualValues(bfm.fsBlockSizeBytes, r3.remainingBytes)
	assert.NotNil(r3.cancel)

	assert.Equal(1, bfm.count)
	assert.NotNil(bfm.file)

	err = rc3.Close()
	assert.NoError(err)
	assert.Equal(0, bfm.count)

	// Case: open failure case
	tof.retError = fmt.Errorf("open error")
	baE := int64(4096)
	rcE, err := bfm.GetBlock(ctx, baE)
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

	bs := 8192
	dbs := bs / 2
	pGood := &Profile{Name: "/block/file", DeviceBlockSizeBytes: int64(dbs)}
	gbwGood := volume.GetBlockReaderArgs{
		VolumeID:       "volID",
		SnapshotID:     "snapID",
		BlockSizeBytes: int64(bs),
	}
	gbwGood.Profile = pGood
	assert.NoError(gbwGood.Validate())
	assert.NoError(pGood.Validate())

	// rwCommon cases are done in TestWriter
	for _, tc := range []string{"nil buffer", "invalid size", "at eof", "read error", "short last block", "full block", "canceled"} {
		t.Logf("Case: %s", tc)

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		tdf := &testDevFiler{}
		tof := &testOpenFile{}
		openFile = tof.OpenFile
		tof.retFile = tdf

		bfm := &manager{}
		bfm.fsBlockSizeBytes = int64(bs)
		bfm.Name = "foo"

		bw, err := bfm.GetBlockReader(gbwGood)
		assert.NoError(err)
		assert.NotNil(bw)
		assert.Equal(dbs, int(bfm.DeviceBlockSizeBytes))

		ba1 := int64(256)
		assert.Equal(0, bfm.count)
		rc, err := bw.GetBlock(ctx, ba1)
		assert.NoError(err)
		assert.NotNil(rc)

		r, ok := rc.(*Reader)
		assert.True(ok)
		assert.NotNil(r)
		assert.NotNil(r.buf)
		assert.Len(r.buf, dbs, "got %d expected %d", len(r.buf), dbs)

		r.numCalls = 0
		r.currentOffset = 0
		r.remainingBytes = int(bfm.fsBlockSizeBytes)

		var expError error
		b := make([]byte, bs)
		expOffset := dbs
		expN := dbs
		expRemB := bs - dbs

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
	}
}
