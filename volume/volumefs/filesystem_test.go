package volumefs // nolint

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/faketime"
	"github.com/kopia/kopia/internal/mockfs"
	"github.com/kopia/kopia/internal/testlogging"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/blob/filesystem"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/kopia/kopia/volume"
	vmgr "github.com/kopia/kopia/volume/fake"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestFilesystemArgs(t *testing.T) {
	assert := assert.New(t)

	_, th := newVolFsTestHarness(t)
	defer th.cleanup()

	mgr := volume.FindManager(vmgr.VolumeType)
	assert.NotNil(mgr)

	tcs := []FilesystemArgs{
		{},
		{Repo: th.repo},
		{Repo: th.repo, VolumeManager: mgr},
		{Repo: th.repo, VolumeManager: mgr, VolumeID: "volid"},
		{Repo: th.repo, VolumeManager: mgr, VolumeID: "volid", VolumeSnapshotID: "volSnapID"},
	}
	for i, tc := range tcs {
		assert.Error(tc.Validate(), "case %d", i)
		f, err := New(&tc) // nolint:scopelint
		assert.Error(err)
		assert.Nil(f)
	}

	fa := FilesystemArgs{
		Repo:                th.repo,
		VolumeManager:       mgr,
		VolumeID:            "volID",
		VolumeSnapshotID:    "volSnapID1",
		VolumeAccessProfile: &mgr, // just to make it not-nil
	}
	t.Logf("%#v", fa)
	assert.NoError(fa.Validate())
	si := fa.SourceInfo()
	assert.Equal(th.repo.Hostname(), si.Host)
	assert.Equal(th.repo.Username(), si.UserName)
	assert.Equal(path.Join("/volumefs", fa.VolumeID), si.Path)
}

// nolint:wsl,gocritic
func TestSnapshotAnalysis(t *testing.T) {
	assert := assert.New(t)

	expSA := SnapshotAnalysis{
		BlockSizeBytes:   1000,
		Bytes:            20000,
		NumBlocks:        1000,
		NumDirs:          100,
		ChainLength:      2,
		ChainedBytes:     80000,
		ChainedNumBlocks: 2000,
		ChainedNumDirs:   200,
	}

	curStats := snapshot.Stats{
		TotalDirectoryCount: expSA.NumDirs + expSA.ChainedNumDirs,
		TotalFileCount:      expSA.NumBlocks + expSA.ChainedNumBlocks,
		TotalFileSize:       expSA.Bytes + expSA.ChainedBytes,

		ExcludedFileCount:     expSA.ChainedNumBlocks,
		ExcludedTotalFileSize: expSA.ChainedBytes,
		ExcludedDirCount:      expSA.ChainedNumDirs,

		CachedFiles:    int32(expSA.BlockSizeBytes),
		NonCachedFiles: int32(expSA.ChainLength),
	}
	curSM := &snapshot.Manifest{
		Stats: curStats,
	}
	s := Snapshot{
		Current: curSM,
	}

	assert.Equal(expSA, s.Analyze())
	assert.Equal(SnapshotAnalysis{}, (&Snapshot{}).Analyze())
}

// Test harness based on upload_test.go, repotesting_test.go, snapshot_test.go, etc.
const (
	masterPassword     = "foofoofoofoofoofoofoofoo"
	defaultPermissions = 0777
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
func (th *volFsTestHarness) addSnapshot(ctx context.Context, bal []int64) *snapshot.Manifest {
	assert := assert.New(th.t)
	sourceDir := mockfs.NewDirectory()

	f := th.fsForBackupTests(nil) // for utility methods

	// create directories first
	dirMap := map[string]struct{}{}
	for _, ba := range bal {
		pp, err := f.addrToPath(ba)
		assert.NoError(err)
		// th.t.Logf("Looking at %s", pp)
		for i := 0; i < len(pp)-1; i++ { // not including the file
			dir := pp[0 : i+1].String()
			if _, ok := dirMap[dir]; ok {
				continue
			}
			// th.t.Logf("%d: Creating dir %s", i, dir)
			sourceDir.AddDir(dir, defaultPermissions)
			dirMap[dir] = struct{}{}
		}
	}
	// create files
	// Assumption here is that if does not matter if the size does not match the dir entry size
	for _, ba := range bal {
		pp, err := f.addrToPath(ba)
		assert.NoError(err)
		sourceDir.AddFile(pp.String(), []byte(fmt.Sprintf("File at %s", pp)), defaultPermissions)
	}

	// Add the metadata files
	md := f.metadata()
	for _, pp := range md.metadataFiles() { // TBD handle previous snap id
		sourceDir.AddFile(pp.String(), []byte{}, defaultPermissions)
	}

	th.ft.Advance(1 * time.Hour)
	u := snapshotfs.NewUploader(th.repo)
	policyTree := policy.BuildTree(nil, policy.DefaultPolicy)
	snapManifest, err := u.Upload(ctx, sourceDir, policyTree, f.SourceInfo())
	if err != nil {
		th.t.Fatalf("failed to create snapshot: %v", err.Error())
	}
	th.t.Logf("snapshot manifest: %#v", snapManifest)

	// Save the manifest
	manID := th.mustSaveSnapshot(snapManifest)
	th.t.Logf("manifestID: %s", manID)

	return snapManifest
}

func (th *volFsTestHarness) mustSaveSnapshot(snapManifest *snapshot.Manifest) manifest.ID {
	th.t.Helper()

	manID, err := snapshot.SaveSnapshot(testlogging.Context(th.t), th.repo, snapManifest)
	if err != nil {
		th.t.Fatalf("error saving snapshot: %v", err)
	}

	return manID
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
		VolumeSnapshotID:    "volSnapID1",
		VolumeAccessProfile: profile,
	}
	th.t.Logf("%#v", fa)
	assert.NoError(fa.Validate())

	f, err := New(fa)
	assert.NoError(err)
	assert.NotNil(f)

	return f
}

// // nolint:wsl,gocritic
// func (th *volFsTestHarness) fsForRestoreTests(p *blockfile.Profile) *Filesystem {
// 	assert := assert.New(th.t)

// 	mgr := volume.FindManager(blockfile.VolumeType)
// 	assert.NotNil(mgr)

// 	fa := &FilesystemArgs{
// 		Repo:                th.repo,
// 		VolumeManager:       mgr,
// 		VolumeID:            "volID",
// 		VolumeSnapshotID:    "volSnapID1",
// 		VolumeAccessProfile: p,
// 	}
// 	th.t.Logf("%#v", fa)
// 	assert.NoError(fa.Validate())

// 	f, err := New(fa)
// 	assert.NoError(err)
// 	assert.NotNil(f)

// 	return f
// }

// nolint:wsl,gocritic
// func (th *volFsTestHarness) compareInternalAndExternalTrees(ctx context.Context, f *Filesystem, rootEntry fs.Directory) {
// 	assert := assert.New(th.t)

// 	var checkNode func(dir fs.Directory, ppp parsedPath)

// 	checkNode = func(dir fs.Directory, ppp parsedPath) {
// 		msg := fmt.Sprintf("node [%s]", ppp)
// 		th.t.Logf("Entered dir [%s]", ppp)
// de, ok := dir.(*dirEntry)
// assert.True(ok, msg)
// assert.NotNil(de, msg)
// assert.NotNil(de.m, msg)
// assert.Equal(f, de.f, msg)
// assert.Equal(0555|os.ModeDir, dir.Mode(), msg)
// assert.Equal(de.m.mTime, dir.ModTime(), msg)
// if ppp != nil {
// must be in the metadata tree
// ldm := f.lookupDir(ctx, ppp)
// assert.Equal(de.m, ldm, msg)
// th.t.Logf("Found [%s] in meta", ppp)
// } else {
// 	assert.Equal(f.rootDir, de.m)
// 	th.t.Logf("Found [%s] in meta", ppp)
// }
// 		entries, err := dir.Readdir(ctx)
// 		assert.NoError(err, msg)
// 		th.t.Logf("Dir [%s] has %d entries", ppp, len(entries))
// 		// assert.Equal(len(de.m.subdirs)+len(de.m.files), len(entries), msg)
// 		cnt := 0
// 		for _, entry := range entries {
// 			epp := append(ppp, entry.Name())
// 			msg := fmt.Sprintf("node [%s]", epp) // nolint:govet
// 			if fsd, ok := entry.(fs.Directory); ok {
// 				cnt++
// 				assert.True(entry.IsDir(), msg)
// 				checkNode(fsd, epp)
// 			} else {
// 				assert.False(entry.IsDir(), msg)
// 			}
// 		}
// 		for _, entry := range entries {
// 			epp := append(ppp, entry.Name())
// 			msg := fmt.Sprintf("node %s", epp) // nolint:govet
// 			if fsf, ok := entry.(fs.File); ok {
// 				cnt++
// 				assert.NotNil(fsf, msg)
// 				assert.False(entry.IsDir(), msg)
// 				// fe, ok := fsf.(*fileEntry)
// 				// assert.True(ok, msg)
// 				// assert.NotNil(fe, msg)
// 				// assert.Equal(f, fe.f, msg)
// 				// assert.NotNil(fe.m, msg)
// 				// lfm := f.lookupFile(f.rootDir, epp)
// 				// // assert.Equal(fe.m, lfm, msg)
// 				// th.t.Logf("Found [%s] in meta", epp)
// 				// if lfm.isMeta() {
// 				// 	assert.Equal(int64(0), fsf.Size())
// 				// } else {
// 				// 	assert.Equal(f.blockSzB, fsf.Size())
// 				// }
// 				// assert.Equal(os.FileMode(0555), fsf.Mode(), msg)
// 				// assert.Equal(fe.m.mTime, fsf.ModTime(), msg)

// 				// fsr, err := fsf.Open(ctx)
// 				// assert.NoError(err, msg)
// 				// defer fsr.Close()

// 				// assert.NotNil(fsr, msg)
// 				// fr, ok := fsr.(*fileReader)
// 				// assert.True(ok, msg)
// 				// assert.NotNil(fr, msg)
// 				// assert.Equal(fe, fr.fe, msg)
// 				// le, err := fsr.Entry()
// 				// assert.NoError(err)
// 				// assert.Equal(fsf, le, msg)
// 				// buf := make([]byte, f.blockSzB/4)
// 				// bytesRead := 0
// 				// for {
// 				// 	var n int
// 				// 	n, err = fsr.Read(buf)
// 				// 	if err != nil {
// 				// 		break
// 				// 	}
// 				// 	bytesRead += n
// 				// }
// 				// assert.True(err == io.EOF, msg)
// 				// if lfm.isMeta(f) {
// 				// 	assert.Equal(0, bytesRead)
// 				// }
// 			} else {
// 				assert.True(entry.IsDir(), msg)
// 			}
// 		}
// 		assert.Equal(len(entries), cnt, msg)
// 	}
// 	checkNode(rootEntry, nil)
// }

type testVM struct {
	retGbrBR volume.BlockReader
	retGbrE  error
	gbrA     volume.GetBlockReaderArgs
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
func (vm *testVM) GetBlockWriter(args volume.GetBlockWriterArgs) (volume.BlockWriter, error) {
	return nil, nil
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
	// Mixed entries only found in the top directory
	return nil, e.retReadDirErr
}

type testFileEntry struct {
	name       string
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

type testWC struct {
	t *testing.T

	retCloseErr error
	closeCalled bool

	retWriteN   int
	retWriteErr error
}

var _ io.WriteCloser = (*testWC)(nil)

func (wc *testWC) Close() error {
	wc.closeCalled = true
	return wc.retCloseErr
}

func (wc *testWC) Write(b []byte) (int, error) {
	retN := wc.retWriteN
	if wc.retWriteErr == nil && retN == 0 {
		retN = len(b)
		wc.t.Logf("*** Write(%d)", len(b))
	}

	return retN, wc.retWriteErr
}

type testRepo struct {
	inOoID object.ID
	retOoR object.Reader
	retOoE error

	inNowO  object.WriterOptions
	retNowW object.Writer

	retFE error
}

func (r *testRepo) OpenObject(ctx context.Context, id object.ID) (object.Reader, error) {
	r.inOoID = id
	return r.retOoR, r.retOoE
}

func (r *testRepo) NewObjectWriter(ctx context.Context, opt object.WriterOptions) object.Writer {
	r.inNowO = opt
	return r.retNowW
}

func (r *testRepo) Flush(ctx context.Context) error {
	return r.retFE
}

// func (dm *dirMeta) descend(cb func(*dirMeta, parsedPath, interface{})) {
// 	dm.postOrderWalk(nil, cb)
// }

// func (dm *dirMeta) postOrderWalk(ppp parsedPath, cb func(*dirMeta, parsedPath, interface{})) {
// 	pp := parsedPath{}
// 	if ppp != nil {
// 		pp = append(ppp, dm.name) // nolint:gocritic
// 	}

// 	cb(dm, pp, true)

// 	for _, sdm := range dm.subdirs {
// 		sdm.postOrderWalk(pp, cb)
// 	}

// 	for _, fm := range dm.files {
// 		pp = append(pp, fm.name)
// 		cb(dm, pp, fm)
// 		pp = pp[:len(pp)-1]
// 	}

// 	cb(dm, pp, false)
// }
