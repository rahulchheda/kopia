package volumefs

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/volume"
	"github.com/kopia/kopia/volume/blockfile"
	vmgr "github.com/kopia/kopia/volume/fake"

	"github.com/stretchr/testify/assert"
)

// Test harness based on upload_test.go, repotesting_test.go, snapshot_test.go, etc.
const (
	masterPassword = "foofoofoofoofoofoofoofoo"
)

type volFsTestHarness struct {
	t       *testing.T
	repoDir string
	repo    repo.Repository
	ft      *faketime.TimeAdvance
	retFS   *Filesystem
}

// nolint:wsl,gocritic
func newVolFsTestHarness(t *testing.T) (context.Context, *volFsTestHarness) {
	ctx := testlogging.Context(t)

	repoDir, err := ioutil.TempDir("", "kopia-repo")
	if err != nil {
		panic("cannot create temp directory: " + err.Error())
	}

	storage, err := filesystem.New(ctx, &filesystem.Options{
		Path: repoDir,
	})

	if err != nil {
		panic("cannot create storage directory: " + err.Error())
	}

	if initerr := repo.Initialize(ctx, storage, &repo.NewRepositoryOptions{}, masterPassword); initerr != nil {
		panic("unable to create repository: " + initerr.Error())
	}

	configFile := filepath.Join(repoDir, ".kopia.config")
	if conerr := repo.Connect(ctx, configFile, storage, masterPassword, nil); conerr != nil {
		panic("unable to connect to repository: " + conerr.Error())
	}

	ft := faketime.NewTimeAdvance(time.Date(2020, time.April, 28, 0, 0, 0, 0, time.UTC))

	rep, err := repo.Open(ctx, configFile, masterPassword, &repo.Options{
		TimeNowFunc: ft.NowFunc(),
	})

	if err != nil {
		panic("unable to open repository: " + err.Error())
	}

	th := &volFsTestHarness{
		t:       t,
		repoDir: repoDir,
		repo:    rep,
		ft:      ft,
	}

	return ctx, th
}

func (th *volFsTestHarness) cleanup() {
	os.RemoveAll(th.repoDir)
	th.t.Log("Test harness cleanup done")
}

// nolint:wsl,gocritic
func (th *volFsTestHarness) fsForBackupTests(profile interface{}) *Filesystem {
	if th.retFS != nil {
		return th.retFS
	}

	assert := assert.New(th.t)

	mgr := volume.FindManager(vmgr.VolumeType)
	assert.NotNil(mgr)

	if profile == nil { // test does not need the profile but validate does
		profile = &mgr
	}

	fa := &FilesystemArgs{
		Repo:                th.repo,
		VolumeManager:       mgr,
		VolumeID:            "volID",
		VolumeSnapshotID:    "volSnapID",
		VolumeAccessProfile: profile,
	}
	th.t.Logf("%#v", fa)
	assert.NoError(fa.Validate())

	f, err := New(fa)
	assert.NoError(err)
	assert.NotNil(f)

	return f
}

// nolint:wsl,gocritic
func (th *volFsTestHarness) fsForRestoreTests(p *blockfile.Profile) *Filesystem {
	assert := assert.New(th.t)

	mgr := volume.FindManager(blockfile.VolumeType)
	assert.NotNil(mgr)

	fa := &FilesystemArgs{
		Repo:                th.repo,
		VolumeManager:       mgr,
		VolumeID:            "volID",
		VolumeSnapshotID:    "volSnapID1",
		VolumeAccessProfile: p,
	}
	th.t.Logf("%#v", fa)
	assert.NoError(fa.Validate())

	f, err := New(fa)
	assert.NoError(err)
	assert.NotNil(f)

	return f
}

type testVM struct {
	retGbrBR volume.BlockReader
	retGbrE  error
	gbrA     volume.GetBlockReaderArgs

	retGbwBW []volume.BlockWriter
	retGbwE  []error
	gbwA     volume.GetBlockWriterArgs
}

// Type returns the volume type.
func (vm *testVM) Type() string {
	return "testVM"
}

// GetBlockReader returns a BlockReader for a particular volume.
// Optional - will return ErrNotSupported if unavailable.
func (vm *testVM) GetBlockReader(args volume.GetBlockReaderArgs) (volume.BlockReader, error) {
	vm.gbrA = args
	return vm.retGbrBR, vm.retGbrE
}

// GetBlockWriter returns a BlockWriter for a particular volume.
// Optional - will return ErrNotSupported if unavailable.
// nolint:wsl
func (vm *testVM) GetBlockWriter(args volume.GetBlockWriterArgs) (volume.BlockWriter, error) {
	vm.gbwA = args
	var bw volume.BlockWriter
	var err error
	if len(vm.retGbwBW) > 0 {
		bw, vm.retGbwBW = vm.retGbwBW[0], vm.retGbwBW[1:]
	}
	if len(vm.retGbwE) > 0 {
		err, vm.retGbwE = vm.retGbwE[0], vm.retGbwE[1:]
	}
	return bw, err
}

type testBW struct {
	inPutBlocksBI   volume.BlockIterator
	retPutBlocksErr error
}

var _ volume.BlockWriter = (*testBW)(nil)

func (bw *testBW) PutBlocks(ctx context.Context, bi volume.BlockIterator) error {
	bw.inPutBlocksBI = bi
	return bw.retPutBlocksErr
}

type testBR struct {
	retGetBlocksE  error
	retGetBlocksBI volume.BlockIterator
}

var _ volume.BlockReader = (*testBR)(nil)

func (br *testBR) GetBlocks(ctx context.Context) (volume.BlockIterator, error) {
	return br.retGetBlocksBI, br.retGetBlocksE
}

type testDirEntry struct {
	name string

	retReadDirE   fs.Entries
	retReadDirErr error
}

var _ fs.Directory = (*testDirEntry)(nil)

func (e *testDirEntry) IsDir() bool {
	return true
}

func (e *testDirEntry) Name() string {
	return e.name
}

func (e *testDirEntry) Mode() os.FileMode {
	return 0555 | os.ModeDir
}

func (e *testDirEntry) Size() int64 {
	return 8
}

func (e *testDirEntry) Sys() interface{} {
	return nil
}

func (e *testDirEntry) ModTime() time.Time {
	return time.Now()
}

func (e *testDirEntry) Owner() fs.OwnerInfo {
	return fs.OwnerInfo{}
}

func (e *testDirEntry) Summary() *fs.DirectorySummary {
	return nil
}

func (e *testDirEntry) Child(ctx context.Context, name string) (fs.Entry, error) {
	return fs.ReadDirAndFindChild(ctx, e, name)
}

func (e *testDirEntry) Readdir(ctx context.Context) (fs.Entries, error) {
	return e.retReadDirE, e.retReadDirErr
}

type testFileEntry struct {
	name       string
	oid        object.ID
	size       int64
	retOpenR   fs.Reader
	retOpenErr error
}

var _ fs.File = (*testFileEntry)(nil)

func (e *testFileEntry) IsDir() bool {
	return false
}

func (e *testFileEntry) Name() string {
	return e.name
}

func (e *testFileEntry) Mode() os.FileMode {
	return 0555 // nolint:gomnd
}

func (e *testFileEntry) Size() int64 {
	return e.size
}

func (e *testFileEntry) Sys() interface{} {
	return nil
}

func (e *testFileEntry) ModTime() time.Time {
	return time.Now()
}

func (e *testFileEntry) Owner() fs.OwnerInfo {
	return fs.OwnerInfo{}
}

func (e *testFileEntry) Open(ctx context.Context) (fs.Reader, error) {
	return e.retOpenR, e.retOpenErr
}

func (e *testFileEntry) ObjectID() object.ID {
	return e.oid
}

type testReader struct {
	t *testing.T

	retCloseErr error
	closeCalled bool

	retReadN     int
	retReadErr   error
	readRemBytes int
	cnt          int

	retEntryE   fs.Entry
	retEntryErr error
}

var _ fs.Reader = (*testReader)(nil)
var _ object.Reader = (*testReader)(nil)

func (tr *testReader) Close() error {
	tr.closeCalled = true
	return tr.retCloseErr
}

func (tr *testReader) Read(b []byte) (int, error) {
	retN := tr.retReadN
	retE := tr.retReadErr

	tr.cnt++
	if tr.cnt > 10 {
		return 0, io.ErrUnexpectedEOF
	}

	if tr.retReadErr == nil && retN == 0 {
		if tr.readRemBytes > 0 {
			if len(b) < tr.readRemBytes {
				retN = len(b)
			} else {
				retN = tr.readRemBytes
			}

			tr.readRemBytes -= retN
		} else {
			retE = io.EOF
		}

		tr.t.Logf("*** Read(%d) -> %d %d", len(b), retN, tr.readRemBytes)
	}

	return retN, retE
}

func (tr *testReader) Seek(offset int64, whence int) (int64, error) {
	return 0, nil
}

func (tr *testReader) Entry() (fs.Entry, error) {
	return tr.retEntryE, tr.retEntryErr
}

func (tr *testReader) Length() int64 {
	return 0
}

type testRC struct {
	retCloseErr error
	closeCalled bool

	retReadN   []int
	retReadErr []error
	readB      []byte
}

func (rc *testRC) Read(b []byte) (int, error) {
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

func (rc *testRC) Close() error {
	rc.closeCalled = true
	return rc.retCloseErr
}

type testWC struct {
	retCloseErr error
	closeCalled bool

	retWriteN   int
	retWriteErr error
	sumWriteN   int

	retResultID object.ID
	retResultE  error

	isDir bool
	desc  string
}

var _ io.WriteCloser = (*testWC)(nil)
var _ object.Writer = (*testWC)(nil)

func (wc *testWC) Close() error {
	wc.closeCalled = true
	return wc.retCloseErr
}

func (wc *testWC) Write(b []byte) (int, error) {
	retN := wc.retWriteN
	if wc.retWriteErr == nil && retN == 0 {
		retN = len(b)
		wc.sumWriteN += retN
	}

	return retN, wc.retWriteErr
}

func (wc *testWC) Result() (object.ID, error) {
	return wc.retResultID, wc.retResultE
}

type testRepo struct {
	inOoID object.ID
	retOoR object.Reader
	retOoE error

	inNowO   object.WriterOptions
	retNowW  object.Writer
	genNowW  bool
	genIDCnt int
	genW     []*testWC // effective
	genUseW  []*testWC // specified; sparse

	retFE error
}

var _ repository = (*testRepo)(nil)

func (r *testRepo) OpenObject(ctx context.Context, id object.ID) (object.Reader, error) {
	r.inOoID = id
	return r.retOoR, r.retOoE
}

// nolint:wsl
func (r *testRepo) NewObjectWriter(ctx context.Context, opt object.WriterOptions) object.Writer {
	r.inNowO = opt
	if r.genNowW { // generate writers on the fly based on option
		isDir := opt.Prefix == "k"
		tw := &testWC{retResultID: r.genID(isDir), isDir: isDir, desc: opt.Description}
		idx := r.genIDCnt
		if len(r.genUseW) > idx && r.genUseW[idx] != nil {
			w := r.genUseW[idx]
			w.isDir = isDir
			w.desc = opt.Description
			if w.retResultE == nil {
				w.retResultID = tw.retResultID
			}
			tw = w
		}
		r.genW = append(r.genW, tw)

		return tw
	}

	return r.retNowW
}

func (r *testRepo) Flush(ctx context.Context) error {
	return r.retFE
}

func (r *testRepo) genID(isDir bool) object.ID {
	var id string
	if isDir {
		id = fmt.Sprintf("kc7b05277dfab817d3c4c5e945d09%04x", r.genIDCnt)
	} else {
		id = fmt.Sprintf("c7b05277dfab817d3c4c5e945d09%04x", r.genIDCnt)
	}
	r.genIDCnt++

	return object.ID(id)
}

type testBlockMap struct {
	inFindBa  int64
	retFindID object.ID

	retIteratorBmi BlockMapIterator
}

func (bm *testBlockMap) Find(blockAddr int64) object.ID {
	bm.inFindBa = blockAddr
	return bm.retFindID
}

func (bm *testBlockMap) Iterator() BlockMapIterator {
	return bm.retIteratorBmi
}
