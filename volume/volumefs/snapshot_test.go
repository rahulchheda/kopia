package volumefs

import (
	"context"
	"testing"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	vmgr "github.com/kopia/kopia/volume/fake"

	"github.com/stretchr/testify/assert"
)

// nolint:gocritic
func TestFindSnapshotManifest(t *testing.T) {
	assert := assert.New(t)

	saveSL := snapshotLister

	defer func() {
		snapshotLister = saveSL
	}()

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	f := th.fsForBackupTests(&vmgr.ReaderProfile{})

	manifests := []*snapshot.Manifest{
		{RootEntry: &snapshot.DirEntry{ObjectID: "snap0"}},
		{RootEntry: &snapshot.DirEntry{ObjectID: "snap1"}},
		{RootEntry: &snapshot.DirEntry{ObjectID: "snap2"}},
	}

	for _, tc := range []string{
		"list error", "not found", "found",
	} {
		t.Logf("Case: %s", tc)

		tsl := &testSnapshotLister{}
		tsl.retM = manifests
		snapshotLister = tsl.ListSnapshots

		var expError error

		oid := manifests[1].RootObjectID()

		switch tc {
		case "list error":
			expError = ErrInternalError
			tsl.retM = nil
			tsl.retE = expError
		case "not found":
			oid = "not-in-list"
			expError = ErrSnapshotNotFound
		}

		m, err := f.findSnapshotManifest(ctx, oid)

		if expError == nil {
			assert.NoError(err)
			assert.Equal(oid, m.RootObjectID())
		} else {
			assert.Error(err)
			assert.Regexp(expError.Error(), err.Error())
			assert.Nil(m)
		}
	}
}

type testSnapshotLister struct {
	inR repo.Repository
	inS snapshot.SourceInfo

	retM []*snapshot.Manifest
	retE error
}

// nolint:gocritic
func (sl *testSnapshotLister) ListSnapshots(ctx context.Context, repo repo.Repository, si snapshot.SourceInfo) ([]*snapshot.Manifest, error) {
	sl.inR = repo
	sl.inS = si

	return sl.retM, sl.retE
}
