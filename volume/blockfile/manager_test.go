package blockfile

import (
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/kopia/kopia/volume"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestManagerRegistered(t *testing.T) {
	assert := assert.New(t)

	// manager should be registered via init
	m := volume.FindManager(VolumeType)
	assert.NotNil(m)

	assert.Equal(VolumeType, m.Type())

	bfm, ok := m.(*blockfileFactory) // factory structure
	assert.True(ok)
	assert.NotNil(bfm)
}

// nolint:wsl,gocritic
func TestProfile(t *testing.T) {
	assert := assert.New(t)

	errTcs := []Profile{
		{},
		{Name: "/block/file", DeviceBlockSizeBytes: -1},
		{Name: "/block/file", DeviceBlockSizeBytes: 4095},
	}
	for i, tc := range errTcs {
		err := tc.Validate()
		assert.Equal(ErrInvalidArgs, err, "case %d", i)
	}

	okTcs := []Profile{
		{Name: "/block/file"},
		{Name: "/block/file", CreateIfMissing: true},
		{Name: "/block/file", DeviceBlockSizeBytes: 4096},
	}
	for i, tc := range okTcs {
		err := tc.Validate()
		assert.NoError(err, "case %d", i)
	}
}

// nolint:wsl,gocritic
func TestOpenFile(t *testing.T) {
	assert := assert.New(t)

	assert.NotNil(openFile)
	savedOpenFile := openFile
	defer func() {
		openFile = savedOpenFile
	}()

	ctx := context.Background()

	tcs := []string{"write-no-create", "write-create", "read", "fail"}
	for _, tc := range tcs {
		tof := &testOpenFile{}
		openFile = tof.OpenFile
		tof.retFile = os.Stdout

		bfm := &manager{}
		bfm.logger = log(ctx)
		bfm.Name = "/some/file"

		mustLock := true
		forReading := false

		// setup
		switch tc {
		case "write-create":
			bfm.CreateIfMissing = true
		case "read":
			forReading = true
			bfm.CreateIfMissing = true // will be ignored
			tof.retFile = os.Stdin
		case "fail":
			tof.retError = fmt.Errorf("failed")
			tof.retFile = nil
		}

		f, err := bfm.openFile(mustLock, forReading)

		// check
		switch tc {
		case "write-no-create":
			assert.NoError(err, tc)
			assert.Equal(os.Stdout, f, tc)
			assert.Equal(bfm.Name, tof.inName, tc)
			assert.Equal(os.O_WRONLY, tof.inFlags, tc)
			assert.Equal(os.FileMode(0600), tof.inPerms, tc)
		case "write-create":
			assert.NoError(err, tc)
			assert.Equal(os.Stdout, f, tc)
			assert.Equal(bfm.Name, tof.inName, tc)
			assert.Equal(os.O_WRONLY|os.O_CREATE, tof.inFlags, tc)
			assert.Equal(os.FileMode(0600), tof.inPerms, tc)
		case "read":
			assert.NoError(err, tc)
			assert.Equal(os.Stdin, f, tc)
			assert.Equal(bfm.Name, tof.inName, tc)
			assert.Equal(os.O_RDONLY, tof.inFlags, tc)
			assert.Equal(os.FileMode(0600), tof.inPerms, tc)
		case "fail":
			assert.Error(err, tc)
			assert.Nil(f, tc)
		}
	}
}

type testOpenFile struct {
	inName   string
	inFlags  int
	inPerms  os.FileMode
	retFile  devFiler
	retError error
}

func (tof *testOpenFile) OpenFile(name string, flags int, perms os.FileMode) (devFiler, error) {
	tof.inName = name
	tof.inFlags = flags
	tof.inPerms = perms

	return tof.retFile, tof.retError
}

type testDevFiler struct {
	retCloseErr error

	retReadBufs [][]byte
	retReadErrs []error
	numReadAt   int

	retStatFI  os.FileInfo
	retStatErr error

	retWriteAtN   int
	retWriteAtErr error
	numWriteAt    int
}

func (tdf *testDevFiler) Close() error {
	return tdf.retCloseErr
}

func (tdf *testDevFiler) ReadAt(b []byte, off int64) (n int, err error) {
	tdf.numReadAt++

	if len(tdf.retReadErrs) > 0 {
		err, tdf.retReadErrs = tdf.retReadErrs[0], tdf.retReadErrs[1:]
	}

	if len(tdf.retReadBufs) > 0 {
		var buf []byte

		buf, tdf.retReadBufs = tdf.retReadBufs[0], tdf.retReadBufs[1:]
		copy(b, buf)
		n = len(buf)
	}

	return
}

func (tdf *testDevFiler) Stat() (os.FileInfo, error) {
	return tdf.retStatFI, tdf.retStatErr
}

func (tdf *testDevFiler) WriteAt(b []byte, off int64) (int, error) {
	tdf.numWriteAt++

	return tdf.retWriteAtN, tdf.retWriteAtErr
}

// testFileInfo implements os.FileInfo
type testFileInfo struct {
	retSz int64
}

func (tfi *testFileInfo) Name() string {
	return ""
}

func (tfi *testFileInfo) Size() int64 {
	return tfi.retSz
}

func (tfi *testFileInfo) Mode() os.FileMode {
	return 0
}

func (tfi *testFileInfo) ModTime() time.Time {
	return time.Time{}
}

func (tfi *testFileInfo) IsDir() bool {
	return false
}

func (tfi *testFileInfo) Sys() interface{} {
	return nil
}

type testBlockIterator struct {
	retCloseE error
}

// nolint:wsl
func (tbi *testBlockIterator) Next(ctx context.Context) volume.Block {
	return nil
}

func (tbi *testBlockIterator) AtEnd() bool {
	return true
}

func (tbi *testBlockIterator) Close() error {
	return tbi.retCloseE
}

type testBlock struct {
	addr     int64
	size     int
	released bool

	retGetRC  io.ReadCloser
	retGetErr error
}

func (tb *testBlock) Address() int64 {
	return tb.addr
}

func (tb *testBlock) Size() int {
	return tb.size
}

func (tb *testBlock) Get(ctx context.Context) (io.ReadCloser, error) {
	return tb.retGetRC, tb.retGetErr
}

func (tb *testBlock) Release() {
	tb.released = true
}

type testReadCloser struct {
	retCloseErr error
	closeCalled bool

	retReadN   []int
	retReadErr []error
	readB      []byte
}

func (rc *testReadCloser) Read(b []byte) (int, error) {
	rc.readB = b
	n := 0

	if len(rc.retReadN) > 0 {
		n, rc.retReadN = rc.retReadN[0], rc.retReadN[1:]
	}

	var err error

	if len(rc.retReadErr) > 0 {
		err, rc.retReadErr = rc.retReadErr[0], rc.retReadErr[1:]
	}

	return n, err
}

func (rc *testReadCloser) Close() error {
	rc.closeCalled = true
	return rc.retCloseErr
}
