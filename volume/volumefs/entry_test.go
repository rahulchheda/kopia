package volumefs

import (
	"context"
	"testing"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"

	"github.com/stretchr/testify/assert"
)

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
