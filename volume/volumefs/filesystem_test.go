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
