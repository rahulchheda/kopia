package volumefs

import (
	"context"
	"fmt"
	"testing"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"

	"github.com/stretchr/testify/assert"
)

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

// nolint:wsl,gocritic
func TestCreateTreeFromBlockMap(t *testing.T) {
	assert := assert.New(t)

	f := &Filesystem{}
	f.logger = log(context.Background())
	f.initLayoutProperties(16384, 256, 4)

	bmm := newBTreeMap(256)
	addresses := []int64{
		0, 0xff, 0x1000, 0x10ff, 0xa000, 0xb000, 0xc001, 0x1000a, 0x2000b,
	}
	for _, ba := range addresses {
		bam := BlockAddressMapping{
			BlockAddr: ba,
			Oid:       object.ID(fmt.Sprintf("oid-for-%x", ba)),
		}
		bmm.InsertOrReplace(bam)
	}

	dm, err := f.createTreeFromBlockMap(bmm)
	assert.NoError(err)
	assert.NotNil(dm)

	for _, ba := range addresses {
		pp, err := f.addrToPath(ba) // nolint:govet
		assert.NoError(err)
		fm := f.lookupFile(dm, pp)
		assert.NotNil(fm)
		assert.Equal(object.ID(fmt.Sprintf("oid-for-%x", ba)), fm.oid)
	}

	// cause a failure
	bam := BlockAddressMapping{
		BlockAddr: f.maxBlocks + 1,
		Oid:       object.ID(fmt.Sprintf("oid-for-%x", f.maxBlocks+1)),
	}
	bmm.InsertOrReplace(bam)

	dm, err = f.createTreeFromBlockMap(bmm)
	assert.Error(err)
	assert.Nil(dm)
}

// nolint:wsl,gocritic
func TestEntry(t *testing.T) {
	assert := assert.New(t)

	f := &Filesystem{}
	f.logger = log(context.Background())
	rootDm := &dirMeta{}

	for _, pp := range []parsedPath{
		{"00", "10", "20", "f0001"},
		{"00", "11", "20", "f0102"},
		{"01", "12", "23", "f1230"},
	} {
		t.Logf("Case: %s", pp)

		fm := f.ensureFileInTree(rootDm, pp)
		assert.NotNil(fm)
		assert.Equal(pp.Last(), fm.name)
		assert.False(fm.isMeta())

		for i := 1; i < len(pp); i++ {
			dm := f.lookupDir(rootDm, pp[0:i])
			t.Logf("** %d [%s] [%s]", i, pp[0:i], dm.name)
			assert.NotNil(dm)
			assert.Equal(pp[i-1], dm.name)

			dm.oid = "DM-OID"
			dm.summary = &fs.DirectorySummary{}
			expSDE := &snapshot.DirEntry{
				Name:       dm.name,
				Type:       snapshot.EntryTypeDirectory,
				ObjectID:   dm.oid,
				DirSummary: dm.summary,
			}
			assert.Equal(expSDE, dm.snapshotDirEntry())

			efm := f.lookupFile(rootDm, pp[0:i])
			assert.Nil(efm)
		}

		fm2 := f.lookupFile(rootDm, pp)
		assert.Equal(fm, fm2)

		dm := f.lookupDir(rootDm, pp)
		assert.Nil(dm)

		fm3 := f.ensureFileInTree(rootDm, pp)
		assert.NotNil(fm3)
		assert.Equal(fm, fm3)

		fm.oid = "FM-OID"
		expSDE := &snapshot.DirEntry{
			Name:     fm.name,
			Type:     snapshot.EntryTypeFile,
			ObjectID: fm.oid,
		}
		assert.Equal(expSDE, fm.snapshotDirEntry())
	}

	dm := f.lookupDir(rootDm, parsedPath{"00", "not", "in", "tree"})
	assert.Nil(dm)
}
