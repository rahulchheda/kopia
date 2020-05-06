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
	assert.Equal(path.Join("/volumefs", vmgr.VolumeType, fa.VolumeID), si.Path)
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
}

// nolint:wsl,gocritic
func (th *volFsTestHarness) addSnapshot(ctx context.Context, bal []int64) *snapshot.Manifest {
	assert := assert.New(th.t)
	sourceDir := mockfs.NewDirectory()

	f := th.filesystem(nil) // for utility methods

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
	sourceDir.AddFile(fmt.Sprintf(metaFmtX, metaBlockSzB, f.GetBlockSize()), []byte{}, defaultPermissions)

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

// newFilesystem helper
// nolint:wsl,gocritic
func (th *volFsTestHarness) filesystem(profile interface{}) *Filesystem {
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

// nolint:wsl,gocritic
func (th *volFsTestHarness) compareInternalAndExternalTrees(ctx context.Context, f *Filesystem, rootEntry fs.Directory) {
	assert := assert.New(th.t)

	var checkNode func(dir fs.Directory, ppp parsedPath)

	checkNode = func(dir fs.Directory, ppp parsedPath) {
		msg := fmt.Sprintf("node [%s]", ppp)
		th.t.Logf("Entered dir [%s]", ppp)
		de, ok := dir.(*dirEntry)
		assert.True(ok, msg)
		assert.NotNil(de, msg)
		assert.NotNil(de.m, msg)
		assert.Equal(f, de.f, msg)
		assert.Equal(0555|os.ModeDir, dir.Mode(), msg)
		assert.Equal(de.m.mTime, dir.ModTime(), msg)
		if ppp != nil {
			// must be in the metadata tree
			ldm := f.lookupDir(ctx, ppp)
			assert.Equal(de.m, ldm, msg)
			th.t.Logf("Found [%s] in meta", ppp)
		} else {
			assert.Equal(f.rootDir, de.m)
			th.t.Logf("Found [%s] in meta", ppp)
		}
		entries, err := dir.Readdir(ctx)
		assert.NoError(err, msg)
		th.t.Logf("Dir [%s] has %d entries", ppp, len(entries))
		assert.Equal(len(de.m.subdirs)+len(de.m.files), len(entries), msg)
		cnt := 0
		for _, entry := range entries {
			epp := append(ppp, entry.Name())
			msg := fmt.Sprintf("node [%s]", epp) // nolint:govet
			if fsd, ok := entry.(fs.Directory); ok {
				cnt++
				assert.True(entry.IsDir(), msg)
				checkNode(fsd, epp)
			} else {
				assert.False(entry.IsDir(), msg)
			}
		}
		for _, entry := range entries {
			epp := append(ppp, entry.Name())
			msg := fmt.Sprintf("node %s", epp) // nolint:govet
			if fsf, ok := entry.(fs.File); ok {
				cnt++
				assert.NotNil(fsf, msg)
				assert.False(entry.IsDir(), msg)
				fe, ok := fsf.(*fileEntry)
				assert.True(ok, msg)
				assert.NotNil(fe, msg)
				assert.Equal(f, fe.f, msg)
				assert.NotNil(fe.m, msg)
				lfm := f.lookupFile(ctx, epp)
				assert.Equal(fe.m, lfm, msg)
				th.t.Logf("Found [%s] in meta", epp)
				if lfm.isMeta(f) {
					assert.Equal(int64(0), fsf.Size())
				} else {
					assert.Equal(f.blockSzB, fsf.Size())
				}
				assert.Equal(os.FileMode(0555), fsf.Mode(), msg)
				assert.Equal(fe.m.mTime, fsf.ModTime(), msg)

				fsr, err := fsf.Open(ctx)
				assert.NoError(err, msg)
				defer fsr.Close()

				assert.NotNil(fsr, msg)
				fr, ok := fsr.(*fileReader)
				assert.True(ok, msg)
				assert.NotNil(fr, msg)
				assert.Equal(fe, fr.fe, msg)
				le, err := fsr.Entry()
				assert.NoError(err)
				assert.Equal(fsf, le, msg)
				buf := make([]byte, f.blockSzB/4)
				bytesRead := 0
				for {
					var n int
					n, err = fsr.Read(buf)
					if err != nil {
						break
					}
					bytesRead += n
				}
				assert.True(err == io.EOF, msg)
				if lfm.isMeta(f) {
					assert.Equal(0, bytesRead)
				}
			} else {
				assert.True(entry.IsDir(), msg)
			}
		}
		assert.Equal(len(entries), cnt, msg)
	}
	checkNode(rootEntry, nil)
}
