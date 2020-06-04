package volumefs

import (
	"context"
	"testing"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestCompactArgs(t *testing.T) {
	assert := assert.New(t)

	badTcs := []CompactArgs{
		{Concurrency: -1},
	}
	for i, tc := range badTcs {
		t.Logf("Case: %d", i)
		assert.Equal(ErrInvalidArgs, tc.Validate())
	}

	goodTcs := []CompactArgs{
		{},
		{Concurrency: 10},
	}
	for i, tc := range goodTcs {
		t.Logf("Case: %d", i)
		assert.NoError(tc.Validate())
	}
}

// nolint:wsl,gocritic,goconst
func TestCompact(t *testing.T) {
	assert := assert.New(t)

	man := &snapshot.Manifest{
		RootEntry: &snapshot.DirEntry{
			ObjectID:   "k2313ef907f3b250b331aed988802e4c5",
			Type:       snapshot.EntryTypeDirectory,
			DirSummary: &fs.DirectorySummary{},
		},
		Stats: snapshot.Stats{
			TotalFileCount: 10,
			NonCachedFiles: 1, // chain len
		},
	}
	rootEntry := &testDirEntry{}

	for _, tc := range []string{
		"invalid args", "ifs error", "ebm error non-default concurrency", "ctfbm error", "wdtr error", "cr error", "cs error", "success",
	} {
		t.Logf("Case: %s", tc)

		ctx, th := newVolFsTestHarness(t)
		defer th.cleanup()

		ca := CompactArgs{}
		bmi := newBTreeMap(4).Iterator()
		tbm := &testBlockMap{}
		tbm.retIteratorBmi = bmi
		curDm := &dirMeta{name: currentSnapshotDirName}
		rootDir := &dirMeta{}
		curMan := &snapshot.Manifest{}
		curMan.Stats.TotalFileCount = 3
		tcp := &testCompactProcessor{}
		tcp.retIfsMan = man
		tcp.retIfsDir = rootEntry
		tcp.retEbmBm = tbm
		tcp.retCtfBmDm = curDm
		tcp.retCrDm = rootDir
		tcp.retCsS = curMan

		f := th.fs()
		f.cp = tcp

		var expError error
		expConcurrency := DefaultCompactConcurrency

		switch tc {
		case "invalid args":
			ca.Concurrency = -1
			expError = ErrInvalidArgs
		case "ifs error":
			expError = ErrInvalidSnapshot
			tcp.retIfsE = expError
		case "ebm error non-default concurrency":
			expConcurrency = DefaultCompactConcurrency * 2
			ca.Concurrency = expConcurrency
			expError = ErrOutOfRange
			tcp.retEbmE = expError
			tcp.retEbmBm = nil
		case "ctfbm error":
			expError = ErrInternalError
			tcp.retCtfBmE = expError
		case "wdtr error":
			expError = ErrInternalError
			tcp.retWdrE = expError
		case "cr error":
			expError = ErrOutOfRange
			tcp.retCrE = expError
		case "cs error":
			expError = ErrInvalidSnapshot
			tcp.retCsE = expError
		}

		cur, prev, err := f.Compact(ctx, ca)

		if expError == nil {
			assert.NoError(err)
			assert.NotNil(cur)
			assert.NotNil(prev)
			assert.Equal(curMan, cur.Manifest)
			assert.Equal(man, prev.Manifest)
			assert.NotZero(cur.CurrentNumBlocks)
			assert.Equal(curMan.Stats.TotalFileCount, cur.CurrentNumBlocks)
			assert.NotZero(prev.CurrentNumBlocks)
			assert.Equal(man.Stats.TotalFileCount, prev.CurrentNumBlocks)
		} else {
			assert.Error(expError, err)
			assert.Nil(cur)
			assert.Nil(prev)
		}

		switch tc {
		case "ebm error non-default concurrency", "success":
			assert.Equal(expConcurrency, tcp.inEbmC)
		}
	}

}

type testCompactProcessor struct {
	inIfsS    string
	retIfsMan *snapshot.Manifest
	retIfsDir fs.Directory
	retIfsMd  metadata
	retIfsE   error

	inEbmCl  int
	inEbmR   fs.Directory
	inEbmC   int
	retEbmBm BlockMap
	retEbmE  error

	inCrCDm *dirMeta
	inCrPDm *dirMeta
	retCrDm *dirMeta
	retCrE  error

	inCsRDm *dirMeta
	inCsPS  *snapshot.Manifest
	retCsS  *snapshot.Manifest
	retCsE  error

	inCtfBmB   BlockMap
	retCtfBmDm *dirMeta
	retCtfBmE  error

	inWdrPp  parsedPath
	inWdrDm  *dirMeta
	inWdrWst bool
	retWdrE  error
}

var _ compactProcessor = (*testCompactProcessor)(nil)

func (tcp *testCompactProcessor) initFromSnapshot(ctx context.Context, snapshotID string) (*snapshot.Manifest, fs.Directory, metadata, error) {
	tcp.inIfsS = snapshotID
	return tcp.retIfsMan, tcp.retIfsDir, tcp.retIfsMd, tcp.retIfsE
}

func (tcp *testCompactProcessor) effectiveBlockMap(ctx context.Context, chainLen int, rootEntry fs.Directory, concurrency int) (BlockMap, error) {
	tcp.inEbmCl = chainLen
	tcp.inEbmR = rootEntry
	tcp.inEbmC = concurrency

	return tcp.retEbmBm, tcp.retEbmE
}

func (tcp *testCompactProcessor) createRoot(ctx context.Context, curDm, prevRootDm *dirMeta) (*dirMeta, error) {
	tcp.inCrCDm = curDm
	tcp.inCrPDm = prevRootDm

	return tcp.retCrDm, tcp.retCrE
}

func (tcp *testCompactProcessor) commitSnapshot(ctx context.Context, rootDir *dirMeta, psm *snapshot.Manifest) (*snapshot.Manifest, error) {
	tcp.inCsRDm = rootDir
	tcp.inCsPS = psm

	return tcp.retCsS, tcp.retCsE
}

func (tcp *testCompactProcessor) createTreeFromBlockMap(bm BlockMap) (*dirMeta, error) {
	tcp.inCtfBmB = bm
	return tcp.retCtfBmDm, tcp.retCtfBmE
}

func (tcp *testCompactProcessor) writeDirToRepo(ctx context.Context, pp parsedPath, dir *dirMeta, writeSubTree bool) error {
	tcp.inWdrPp = pp
	tcp.inWdrDm = dir
	tcp.inWdrWst = writeSubTree

	return tcp.retWdrE
}
