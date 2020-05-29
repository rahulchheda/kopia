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

	f := &Filesystem{}
	de := &testDirEntry{retReadDirE: entries}
	md2, err := f.recoverMetadataFromDirEntry(context.Background(), de)
	assert.NoError(err)
	assert.Equal(expMD, md2)
}

func generateTestMetaData() (metadata, []parsedPath, fs.Entries) {
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
