package volumefs

import (
	"context"
	"testing"

	"github.com/kopia/kopia/fs"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestMetadataRecovery(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	f := th.fs()
	f.logger = log(ctx)

	for _, tc := range []struct {
		name string
		md   metadata
	}{
		{"meta:BlockSzB:4000", metadata{BlockSzB: 0x4000}},
		{"meta:DirSz:ff", metadata{DirSz: 0xff}},
		{"meta:Depth:6", metadata{Depth: 6}},
		{"meta:VolSnapID:vol:3000:424", metadata{VolSnapID: "vol:3000:424"}},
		{"meta:VolPrevSnapID::22:4", metadata{VolPrevSnapID: ":22:4"}},
		{"meta:Foo:4000", metadata{}},
		{"notmeta", metadata{}},
	} {
		t.Logf("Case: %s", tc.name)

		var (
			md      metadata
			emptyMd metadata
		)

		md.recoverMetadataFromFilename(tc.name)
		assert.Equal(tc.md, md)

		pps := md.metadataFiles()
		if md != emptyMd {
			assert.Len(pps, 1)
			assert.Equal([]parsedPath{parsedPath([]string{tc.name})}, pps)
		} else {
			assert.Empty(pps)
		}
	}

	expMD, expPPs, entries := generateTestMetaData()
	assert.Equal(expPPs, expMD.metadataFiles())

	var md metadata
	md.recoverMetadataFromEntries(entries)
	assert.Equal(expMD, md)
	assert.Equal(expPPs, md.metadataFiles())

	de := &testDirEntry{retReadDirE: entries}
	md2, err := f.recoverMetadataFromDirEntry(context.Background(), de)
	assert.NoError(err)
	assert.Equal(expMD, md2)

	// test creation of metadata files
	dm := &dirMeta{}
	f.setMetadata(expMD) // so no empty fields
	err = f.createMetadataFiles(ctx, dm)
	assert.NoError(err)
	for _, n := range md.metadataFiles() {
		foundFile := false
		for _, fm := range dm.files {
			if fm.name == n.Last() {
				foundFile = true
				break
			}
		}
		assert.True(foundFile, "file: %s", n)
	}

	// test failure to create metadata file
	tU := &testUploader{}
	tU.retWriteFileE = ErrInternalError
	f.up = tU
	err = f.createMetadataFiles(ctx, dm)
	assert.Equal(ErrInternalError, err)
}

func generateTestMetaData() (metadata, []parsedPath, fs.Entries) {
	// Must set all fields
	md := metadata{
		BlockSzB:      0x8000,
		DirSz:         0xff,
		Depth:         6,
		VolSnapID:     "volumeXXX:999",
		VolPrevSnapID: "volumeXXX:998",
	}
	pps := []parsedPath{ // match the above
		parsedPath([]string{"meta:BlockSzB:8000"}),
		parsedPath([]string{"meta:DirSz:ff"}),
		parsedPath([]string{"meta:Depth:6"}),
		parsedPath([]string{"meta:VolSnapID:volumeXXX:999"}),
		parsedPath([]string{"meta:VolPrevSnapID:volumeXXX:998"}),
	}

	entries := fs.Entries{}
	for _, pp := range pps {
		entries = append(entries, &testFileEntry{name: pp[0]})
	}

	return md, pps, entries
}
