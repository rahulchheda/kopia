package volumefs

import (
	"context"
	"fmt"
	"testing"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	vmgr "github.com/kopia/kopia/volume/fake"

	"github.com/stretchr/testify/assert"
)

// nolint:gocritic
func TestInitFromSnapshot(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	f := th.fsForBackupTests(&vmgr.ReaderProfile{})
	f.logger = log(ctx)

	manifests := []*snapshot.Manifest{
		{RootEntry: &snapshot.DirEntry{ObjectID: "k2313ef907f3b250b331aed988802e4c5", Type: snapshot.EntryTypeDirectory}},
		{RootEntry: &snapshot.DirEntry{ObjectID: "k72d04e1f67ac38946b29c146fbea44fc", Type: snapshot.EntryTypeDirectory}},
		{RootEntry: &snapshot.DirEntry{ObjectID: "kf9fb0e450bba821a1c35585d48eaff04", Type: snapshot.EntryTypeFile}}, // not-a-dir
	}

	expMD, _, expEntries := generateTestMetaData()

	for _, tc := range []string{
		"invalid-snapid", "list error", "not found", "not-a-dir", "metadata error", "initialized",
	} {
		t.Logf("Case: %s", tc)

		tsp := &testSnapshotProcessor{}
		tsp.retLsM = manifests
		f.sp = tsp

		var expError error

		oid := string(manifests[1].RootObjectID())

		switch tc {
		case "invalid-snapid":
			oid = "not-a-snap-id"
			expError = fmt.Errorf("invalid contentID suffix")
		case "list error":
			expError = ErrInternalError
			tsp.retLsM = nil
			tsp.retLsE = expError
		case "not found":
			oid = "k785f33b8bcccffa4aac1dabb89cf5a71"
			expError = ErrSnapshotNotFound
		case "not-a-dir":
			oid = "kf9fb0e450bba821a1c35585d48eaff04"
			expError = ErrInvalidSnapshot
		case "metadata error":
			expError = ErrOutOfRange
			tsp.retSrEntry = &testDirEntry{retReadDirErr: expError}
		case "initialized":
			tsp.retSrEntry = &testDirEntry{retReadDirE: expEntries}
		}

		m, de, md, err := f.initFromSnapshot(ctx, oid)

		if expError == nil {
			assert.NoError(err)
			assert.NotNil(m)
			assert.NotNil(de)
			assert.EqualValues(oid, m.RootObjectID())

			if tc == "initialized" {
				assert.Equal(expMD, md)
			}
		} else {
			assert.Error(err)
			assert.Regexp(expError.Error(), err.Error())
			assert.Nil(m)
		}
	}
}

type testSnapshotProcessor struct {
	inLsR  repo.Repository
	inLsS  snapshot.SourceInfo
	retLsM []*snapshot.Manifest
	retLsE error

	retSrEntry fs.Entry
}

var _ snapshotProcessor = (*testSnapshotProcessor)(nil)

// nolint:gocritic
func (tsp *testSnapshotProcessor) ListSnapshots(ctx context.Context, repo repo.Repository, si snapshot.SourceInfo) ([]*snapshot.Manifest, error) {
	tsp.inLsR = repo
	tsp.inLsS = si

	if tsp.retLsE != nil {
		sh := &snapshotHelper{} // call the real thing to check that it works

		m, err := sh.ListSnapshots(ctx, repo, snapshot.SourceInfo{})

		if err != nil || len(m) == 0 { // appears to never fail!
			return nil, tsp.retLsE
		}

		panic("failed to fail")
	}

	return tsp.retLsM, tsp.retLsE
}

// nolint:gocritic
func (tsp *testSnapshotProcessor) SnapshotRoot(rep repo.Repository, man *snapshot.Manifest) (fs.Entry, error) {
	if tsp.retSrEntry != nil {
		return tsp.retSrEntry, nil
	}

	sh := &snapshotHelper{} // call the real thing

	return sh.SnapshotRoot(rep, man)
}
