package volumefs

import (
	"context"
	"testing"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/volume"
	"github.com/kopia/kopia/volume/blockfile"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestRestoreArgs(t *testing.T) {
	assert := assert.New(t)

	mgr := volume.FindManager(blockfile.VolumeType)
	assert.NotNil(mgr)

	badTcs := []RestoreArgs{
		{},
		{RestoreConcurrency: -1},
		{VolumeManager: mgr},
	}
	for i, tc := range badTcs {
		t.Logf("Case: %d", i)
		assert.Equal(ErrInvalidArgs, tc.Validate())
	}

	goodTcs := []RestoreArgs{
		{}, // not really empty - manager/profile added in loop
		{RestoreConcurrency: 10},
	}
	for i, tc := range goodTcs {
		t.Logf("Case: %d", i)
		tc.VolumeManager = mgr
		tc.VolumeAccessProfile = &mgr // just has to be not-nil for Validate()
		assert.NoError(tc.Validate())
	}
}

// nolint:wsl,gocritic,goconst
func TestRestore(t *testing.T) {
	assert := assert.New(t)

	mgr := volume.FindManager(blockfile.VolumeType)
	assert.NotNil(mgr)
	profile := &blockfile.Profile{}
	man := &snapshot.Manifest{
		RootEntry: &snapshot.DirEntry{
			ObjectID:   "k2313ef907f3b250b331aed988802e4c5",
			Type:       snapshot.EntryTypeDirectory,
			DirSummary: &fs.DirectorySummary{},
		},
		Stats: snapshot.Stats{
			NonCachedFiles: 1, // chain len
		},
	}
	rootEntry := &testDirEntry{}

	for _, tc := range []string{
		"invalid args", "no gbw", "ifs error", "ebm error", "gbw error",
		"PutBlocks error", "success default concurrency", "success explicit concurrency",
	} {
		t.Logf("Case: %s", tc)

		ctx, th := newVolFsTestHarness(t)
		defer th.cleanup()

		ra := RestoreArgs{VolumeAccessProfile: profile}
		tbw := &testBW{}
		tvm := &testVM{}
		tvm.retGbwBW = []volume.BlockWriter{nil, tbw}
		tvm.retGbwE = []error{blockfile.ErrInvalidArgs, nil}
		bmi := newBTreeMap(4).Iterator()
		tbm := &testBlockMap{}
		tbm.retIteratorBmi = bmi
		bi := &blockIter{bmi: bmi, atEnd: true}
		bi.BlockIterStats = BlockIterStats{NumBlocks: 4, MinBlockAddr: 1, MaxBlockAddr: 230}
		trp := &testRestoreProcessor{}
		trp.retIfsMan = man
		trp.retIfsDir = rootEntry
		trp.retEbmBm = tbm
		trp.retNbiBi = bi

		f := th.fs()
		assert.Equal(f, f.bp)
		ra.VolumeManager = tvm
		f.rp = trp

		var expError error
		expConcurrency := DefaultRestoreConcurrency

		switch tc {
		case "invalid args":
			expError = ErrInvalidArgs
			ra = RestoreArgs{}
		case "no gbw":
			expError = volume.ErrNotSupported
			tvm.retGbwBW = nil
			tvm.retGbwE = []error{expError}
		case "ifs error":
			expError = ErrInvalidSnapshot
			trp.retIfsE = expError
		case "ebm error":
			expError = ErrOutOfRange
			trp.retEbmE = expError
			trp.retEbmBm = nil
		case "gbw error":
			expError = ErrOutOfRange
			tvm.retGbwBW = nil
			tvm.retGbwE[1] = expError
		case "PutBlocks error":
			expError = ErrInternalError
			tbw.retPutBlocksErr = expError
		case "success explicit concurrency":
			expConcurrency += 2
			ra.RestoreConcurrency = expConcurrency
		}

		res, err := f.Restore(ctx, ra)

		if expError == nil {
			assert.NoError(err)
			assert.Equal(bi.BlockIterStats, res.BlockIterStats)
			assert.Equal(bi, tbw.inPutBlocksBI)
			assert.Equal(bmi, trp.inNbiB)
			expGbwArgs := volume.GetBlockWriterArgs{VolumeID: f.VolumeID, Profile: ra.VolumeAccessProfile}
			assert.Equal(expGbwArgs, tvm.gbwA)
			assert.Equal(int(man.Stats.NonCachedFiles), trp.inEbmCl)
			assert.Equal(rootEntry, trp.inEbmR)
			assert.Equal(expConcurrency, trp.inEbmC)
			assert.Equal(f.VolumeSnapshotID, trp.inIfsS)
		} else {
			assert.Error(expError, err)
			assert.Nil(res)
		}
	}

}

type testRestoreProcessor struct {
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

	inNbiB   BlockMapIterator
	retNbiBi *blockIter
}

func (trp *testRestoreProcessor) initFromSnapshot(ctx context.Context, snapshotID string) (*snapshot.Manifest, fs.Directory, metadata, error) {
	trp.inIfsS = snapshotID
	return trp.retIfsMan, trp.retIfsDir, trp.retIfsMd, trp.retIfsE
}

func (trp *testRestoreProcessor) effectiveBlockMap(ctx context.Context, chainLen int, rootEntry fs.Directory, concurrency int) (BlockMap, error) {
	trp.inEbmCl = chainLen
	trp.inEbmR = rootEntry
	trp.inEbmC = concurrency

	return trp.retEbmBm, trp.retEbmE
}

func (trp *testRestoreProcessor) newBlockIter(bmi BlockMapIterator) *blockIter {
	trp.inNbiB = bmi
	return trp.retNbiBi
}
