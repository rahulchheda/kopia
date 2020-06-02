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
