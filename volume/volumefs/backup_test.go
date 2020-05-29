package volumefs

import (
	"context"
	"testing"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/volume"
	vmgr "github.com/kopia/kopia/volume/fake"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic,goconst
func TestBackup(t *testing.T) {
	assert := assert.New(t)

	profile := &vmgr.ReaderProfile{
		Ranges: []vmgr.BlockAddrRange{
			{Start: 0, Count: 1},
			{Start: 0x100, Count: 1},
			{Start: 0x111ff, Count: 1},
		},
	}

	for _, tc := range []string{
		"invalid args",
		"no gbr", "link error", "bb error default concurrency", "cr error non-default concurrency",
		"cs error", "success",
	} {
		ctx, th := newVolFsTestHarness(t)
		defer th.cleanup()

		ba := BackupArgs{}
		tbr := &testBR{}
		tvm := &testVM{}
		tvm.retGbrBR = tbr
		cDm := &dirMeta{}
		pDm := &dirMeta{}
		rDm := &dirMeta{}
		cMan := &snapshot.Manifest{}
		pMan := &snapshot.Manifest{}
		tbp := &testBackupProcessor{}
		tbp.retLpsDm = pDm
		tbp.retLpsS = pMan
		tbp.retBbDm = cDm
		tbp.retCrDm = rDm
		tbp.retCsS = cMan

		f := th.fsForBackupTests(profile)
		assert.Equal(f, f.bp)
		f.VolumeManager = tvm

		f.bp = tbp

		var expError error
		// var argsInvalid bool

		switch tc {
		case "invalid args":
			ba.PreviousSnapshotID = "foo"
			ba.PreviousVolumeSnapshotID = ""
			expError = ErrInvalidArgs
		case "no gbr":
			expError = volume.ErrNotSupported
			tvm.retGbrBR = nil
			tvm.retGbrE = expError
		case "link error":
			ba.PreviousSnapshotID = "previousSnapshotID"
			ba.PreviousVolumeSnapshotID = "previousVolumeSnapshotID"
			expError = ErrInvalidSnapshot
			tbp.retLpsE = expError
			tbp.retLpsDm = nil
			tbp.retLpsS = nil
		case "bb error default concurrency":
			expError = ErrInternalError
			tbp.retBbE = expError
		case "cr error non-default concurrency":
			ba.PreviousSnapshotID = "previousSnapshotID"
			ba.PreviousVolumeSnapshotID = "previousVolumeSnapshotID"
			ba.BackupConcurrency = 2 * DefaultBackupConcurrency
			expError = ErrOutOfRange
			tbp.retCrE = expError
		case "cs error":
			expError = ErrInvalidSnapshot
			tbp.retCsE = expError
			tbp.retCsS = nil
		case "success":
			ba.PreviousSnapshotID = "previousSnapshotID"
			ba.PreviousVolumeSnapshotID = "previousVolumeSnapshotID"
		}

		snap, err := f.Backup(ctx, ba)

		if expError == nil {
			assert.NoError(err)
			assert.NotNil(snap)
		} else {
			assert.Error(expError, err)
			assert.Nil(snap)
		}

		switch tc {
		case "link error":
			assert.Equal(ba.PreviousSnapshotID, tbp.inLpsPsID)
			assert.Equal(ba.PreviousVolumeSnapshotID, tbp.inLpsPvsID)
		case "bb error default concurrency":
			assert.Equal(DefaultBackupConcurrency, tbp.inBbNw)
			assert.Equal(tbr, tbp.inBbBR)
		case "cr error non-default concurrency":
			assert.Equal(2*DefaultBackupConcurrency, tbp.inBbNw)
			assert.Equal(cDm, tbp.inCrCDm)
			assert.Equal(pDm, tbp.inCrPDm)
		case "cs error":
			assert.Equal(rDm, tbp.inCsRDm)
		case "success":
			assert.Equal(pMan, tbp.inCsPS)
			assert.Equal(cMan, snap.Current)
		}
	}

}

// nolint:wsl,gocritic
func TestBackupCreateRoot(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	f := th.fsForBackupTests(nil)
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
func TestBackupLinkPreviousSnapshot(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	manifests := []*snapshot.Manifest{
		{RootEntry: &snapshot.DirEntry{ObjectID: "k2313ef907f3b250b331aed988802e4c5", Type: snapshot.EntryTypeDirectory, DirSummary: &fs.DirectorySummary{}}},
	}
	expMD, _, expEntries := generateTestMetaData()

	for _, tc := range []string{
		"previous snapshot not found", "volSnapID mismatch", "linked",
	} {
		t.Logf("Case: %s", tc)

		tsp := &testSnapshotProcessor{}
		tsp.retLsM = manifests
		tsp.retSrEntry = &testDirEntry{retReadDirE: expEntries}

		f := th.fsForBackupTests(nil)
		f.logger = log(ctx)
		f.sp = tsp
		f.setMetadata(metadata{}) // clear all

		var expError error
		oid := string(manifests[0].RootObjectID())
		prevVolSnapshotID := expMD.VolSnapID

		switch tc {
		case "previous snapshot not found":
			oid = "k785f33b8bcccffa4aac1dabb89cf5a71"
			expError = ErrSnapshotNotFound
		case "volSnapID mismatch":
			prevVolSnapshotID += "foo"
			expError = ErrInvalidSnapshot
		}

		dm, man, err := f.linkPreviousSnapshot(ctx, oid, prevVolSnapshotID)

		if expError == nil {
			assert.NoError(err)
			assert.Equal(manifests[0], man)
			expDM := &dirMeta{
				name:    previousSnapshotDirName,
				oid:     manifests[0].RootEntry.ObjectID,
				summary: manifests[0].RootEntry.DirSummary,
			}
			assert.Equal(expDM, dm)
			assert.Equal(prevVolSnapshotID, f.prevVolumeSnapshotID)
			assert.Equal(expMD.BlockSzB, f.blockSzB)
			assert.Equal(expMD.DirSz, f.dirSz)
			assert.Equal(expMD.Depth, f.depth)
		} else {
			assert.Error(err)
			assert.Regexp(expError.Error(), err.Error())
			assert.Nil(dm)
			assert.Nil(man)
		}
	}
}

// nolint:wsl,gocritic
func TestBackupBlocks(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	for _, tc := range []string{
		// helper tests
		"invalid addr", "block get error", "file write error", "size mismatch + close error", "no error",
		// bb tests
		"GetBlocks error", "run error", "write dir error", "success",
	} {
		t.Logf("Case: %s", tc)

		f := th.fsForBackupTests(nil)
		f.logger = log(ctx)
		f.blockSzB = 4096

		tR := &testReader{}
		tR.retReadN = f.blockSzB
		tRepo := &testRepo{}
		tRepo.retOoR = tR
		f.repo = tRepo
		tU := &testUploader{}
		tU.retWriteFileID = object.ID("file-oid")
		f.up = tU
		bmi := &blockMapIter{}
		bmi.init()
		close(bmi.mChan)
		bi := &blockIter{f: f, atEnd: true, bmi: bmi} // already at end
		tBR := &testBR{}
		tBR.retGetBlocksBI = bi

		b := &block{
			f:                   f,
			BlockAddressMapping: BlockAddressMapping{BlockAddr: 1},
		}

		var expError, err error
		var dm *dirMeta
		expRClose := true
		bbhTest := true
		numWorkers := 1

		switch tc {
		case "invalid addr":
			b.BlockAddr = -1
			expError = ErrOutOfRange
			expRClose = false
		case "block get error":
			expError = ErrInternalError
			tRepo.retOoE = expError
			tRepo.retOoR = nil
			expRClose = false
		case "file write error":
			expError = ErrOutOfRange
			tU.retWriteFileE = expError
			tU.retWriteFileID = object.ID("")
		case "size mismatch + close error":
			expError = ErrInvalidArgs
			tR.retCloseErr = expError
			tU.retWriteFileSz = int64(b.Size() - 1)
		case "GetBlocks error":
			bbhTest = false
			expError = ErrInvalidArgs
			tBR.retGetBlocksE = expError
		case "run error":
			bbhTest = false
			expError = ErrInvalidArgs
			numWorkers = -1
		case "write dir error":
			bbhTest = false
			expError = ErrInvalidArgs
			tU.retWriteDirE = expError
		case "success":
			bbhTest = false
		}

		bbh := &backupBlocksHelper{}
		if bbhTest {
			bbh.init(f)
			assert.Equal(f, bbh.f)
			assert.Equal(&dirMeta{name: bbh.curRoot.name}, bbh.curRoot)
			assert.NotNil(bbh.bufPool.New)
			err = bbh.worker(ctx, b)
		} else {
			dm, err = f.backupBlocks(ctx, tBR, numWorkers)
		}

		if expError == nil {
			assert.NoError(err)
			if bbhTest {
				pp, err := f.addrToPath(b.Address()) // nolint:govet
				assert.NoError(err)
				fm := f.lookupFile(bbh.curRoot, pp)
				assert.NotNil(fm)
				assert.Equal(tU.retWriteFileID, fm.oid)
			} else {
				assert.NotNil(dm)
				assert.Equal(&dirMeta{name: currentSnapshotDirName}, dm)
			}
		} else {
			assert.Error(err)
			assert.Regexp(expError.Error(), err.Error())
		}

		if bbhTest {
			assert.Equal(expRClose, tR.closeCalled)
		}
	}
}

type testBackupProcessor struct {
	inLpsPsID  string
	inLpsPvsID string
	retLpsDm   *dirMeta
	retLpsE    error
	retLpsS    *snapshot.Manifest

	inBbBR  volume.BlockReader
	inBbNw  int
	retBbDm *dirMeta
	retBbE  error

	inCrCDm *dirMeta
	inCrPDm *dirMeta
	retCrDm *dirMeta
	retCrE  error

	inCsRDm *dirMeta
	inCsPS  *snapshot.Manifest
	retCsS  *snapshot.Manifest
	retCsE  error
}

var _ backupProcessor = (*testBackupProcessor)(nil)

func (tbp *testBackupProcessor) linkPreviousSnapshot(ctx context.Context, previousSnapshotID, prevVolumeSnapshotID string) (*dirMeta, *snapshot.Manifest, error) {
	tbp.inLpsPsID = previousSnapshotID
	tbp.inLpsPvsID = prevVolumeSnapshotID

	return tbp.retLpsDm, tbp.retLpsS, tbp.retLpsE
}

func (tbp *testBackupProcessor) backupBlocks(ctx context.Context, br volume.BlockReader, numWorkers int) (*dirMeta, error) {
	tbp.inBbBR = br
	tbp.inBbNw = numWorkers

	return tbp.retBbDm, tbp.retBbE
}

func (tbp *testBackupProcessor) createRoot(ctx context.Context, curDm, prevRootDm *dirMeta) (*dirMeta, error) {
	tbp.inCrCDm = curDm
	tbp.inCrPDm = prevRootDm

	return tbp.inCrCDm, tbp.retCrE
}

func (tbp *testBackupProcessor) commitSnapshot(ctx context.Context, rootDir *dirMeta, psm *snapshot.Manifest) (*snapshot.Manifest, error) {
	tbp.inCsRDm = rootDir
	tbp.inCsPS = psm

	return tbp.retCsS, tbp.retCsE
}
