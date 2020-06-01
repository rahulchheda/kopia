package volumefs // nolint

import (
	"path"
	"testing"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestFilesystemArgs(t *testing.T) {
	assert := assert.New(t)

	_, th := newVolFsTestHarness(t)
	defer th.cleanup()

	tcs := []FilesystemArgs{
		{},
		{Repo: th.repo},
		{Repo: th.repo, VolumeID: "volid"},
	}
	for i, tc := range tcs {
		assert.Error(tc.Validate(), "case %d", i)
		f, err := New(&tc) // nolint:scopelint
		assert.Error(err)
		assert.Nil(f)
	}

	fa := FilesystemArgs{
		Repo:             th.repo,
		VolumeID:         "volID",
		VolumeSnapshotID: "volSnapID1",
	}
	t.Logf("%#v", fa)
	assert.NoError(fa.Validate())
	si := fa.SourceInfo()
	assert.Equal(th.repo.Hostname(), si.Host)
	assert.Equal(th.repo.Username(), si.UserName)
	assert.Equal(path.Join("/volume", fa.VolumeID), si.Path)
}

// nolint:wsl,gocritic
func TestCreateRoot(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	f := th.fs()
	f.logger = log(ctx)

	cur := &dirMeta{name: currentSnapshotDirName}
	prev := &dirMeta{name: previousSnapshotDirName}

	// case: success, no prev
	dm, err := f.createRoot(ctx, cur, nil)
	assert.NoError(err)
	assert.NotNil(dm)
	assert.Equal("/", dm.name)
	foundCur := false
	for _, e := range dm.subdirs {
		if e.name == cur.name {
			foundCur = true
			break
		}
	}
	assert.True(foundCur)

	// case: success, with prev
	dm, err = f.createRoot(ctx, cur, prev)
	assert.NoError(err)
	assert.NotNil(dm)
	foundCur = false
	foundPrev := false
	for _, e := range dm.subdirs {
		if e.name == cur.name {
			foundCur = true
		}
		if e.name == prev.name {
			foundPrev = true
		}
	}
	assert.True(foundCur)
	assert.True(foundPrev)

	// case: metadata write failure
	tWC := &testWC{}
	tWC.retResultE = ErrInternalError
	tRepo := &testRepo{}
	tRepo.retNowW = tWC
	f.repo = tRepo
	dm, err = f.createRoot(ctx, cur, prev)
	assert.Equal(ErrInternalError, err)
	assert.Nil(dm)
}
