package volumefs

import (
	"context"
	"testing"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/volume"
	"github.com/kopia/kopia/volume/fake"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestBackupArgs(t *testing.T) {
	assert := assert.New(t)

	mgr := volume.FindManager(fake.VolumeType)
	assert.NotNil(mgr)

	badTcs := []BackupArgs{
		{},
		{Concurrency: -1},
		{MaxChainLength: -1},
		{VolumeManager: mgr},
	}
	for i, tc := range badTcs {
		t.Logf("Case: %d", i)
		assert.Equal(ErrInvalidArgs, tc.Validate())
	}

	goodTcs := []BackupArgs{
		{}, // not really empty - manager/profile added in loop
		{Concurrency: 10},
		{PreviousVolumeSnapshotID: "volSnapID"},
		{PreviousVolumeSnapshotID: "volSnapID", Concurrency: 10},
	}
	for i, tc := range goodTcs {
		t.Logf("Case: %d", i)
		tc.VolumeManager = mgr
		tc.VolumeAccessProfile = &mgr // just has to be not-nil for Validate()
		assert.NoError(tc.Validate())
	}
}

// nolint:wsl,gocritic,goconst,gocyclo
func TestBackup(t *testing.T) {
	assert := assert.New(t)

	mgr := volume.FindManager(fake.VolumeType)
	assert.NotNil(mgr)
	profile := &fake.ReaderProfile{
		Ranges: []fake.BlockAddrRange{
			{Start: 0, Count: 1},
			{Start: 0x100, Count: 1},
			{Start: 0x111ff, Count: 1},
		},
	}

	for _, tc := range []string{
		"invalid args", "no gbr", "link error", "bb error default concurrency",
		"cr error non-default concurrency", "cs error", "write dir error", "success", "compact",
	} {
		t.Logf("Case: %s", tc)

		ctx, th := newVolFsTestHarness(t)
		defer th.cleanup()

		ba := BackupArgs{
			VolumeManager:       mgr,
			VolumeAccessProfile: profile,
		}
		tbr := &testBR{}
		tvm := &testVM{}
		tvm.retGbrBR = tbr
		cDm := &dirMeta{name: "current"}
		pDm := &dirMeta{name: "previous"}
		rDm := &dirMeta{name: "root"}
		cMan := &snapshot.Manifest{}
		cMan.Stats.TotalFileCount = 10
		pMan := &snapshot.Manifest{}
		setChainLength(pMan, 5)
		setChainLength(cMan, 6)
		tbp := &testBackupProcessor{}
		tbp.retLpsDm = pDm
		tbp.retLpsS = pMan
		tbp.retLpsRe = &testDirEntry{}
		tbp.retBbDm = cDm
		tbp.retCrDm = rDm
		tbp.retCsS = cMan

		f := th.fs()
		assert.Equal(f, f.bp)
		ba.VolumeManager = tvm
		f.bp = tbp

		var expError error

		switch tc {
		case "invalid args":
			ba.Concurrency = -1
			expError = ErrInvalidArgs
		case "no gbr":
			expError = volume.ErrNotSupported
			tvm.retGbrBR = nil
			tvm.retGbrE = expError
		case "link error":
			ba.PreviousVolumeSnapshotID = "previousVolumeSnapshotID"
			expError = ErrInvalidSnapshot
			tbp.retLpsE = expError
			tbp.retLpsDm = nil
			tbp.retLpsS = nil
		case "bb error default concurrency":
			expError = ErrInternalError
			tbp.retBbE = expError
		case "cr error non-default concurrency":
			ba.PreviousVolumeSnapshotID = "previousVolumeSnapshotID"
			ba.Concurrency = 2 * DefaultBackupConcurrency
			expError = ErrOutOfRange
			tbp.retCrE = expError
		case "cs error":
			expError = ErrInvalidSnapshot
			tbp.retCsE = expError
			tbp.retCsS = nil
		case "write dir error":
			expError = ErrInvalidArgs
			tbp.retWdrE = expError
		case "success":
			ba.PreviousVolumeSnapshotID = "previousVolumeSnapshotID"
		case "compact":
			ba.PreviousVolumeSnapshotID = "previousVolumeSnapshotID"
			tbp.retMcbD = &dirMeta{}
			tbp.retMcbBis.initStats()
		}

		res, err := f.Backup(ctx, ba)

		if expError == nil {
			assert.NoError(err)
			assert.NotNil(res)
		} else {
			assert.Error(expError, err)
			assert.Nil(res)
		}

		switch tc {
		case "link error":
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
		case "success", "compact":
			expSnap := &Snapshot{
				VolumeID:         f.VolumeID,
				VolumeSnapshotID: f.VolumeSnapshotID,
				Manifest:         cMan,
				SnapshotAnalysis: SnapshotAnalysis{
					CurrentNumBlocks: 10,
					ChainLength:      getChainLength(cMan),
				},
			}
			assert.Equal(expSnap, res.Snapshot)
			assert.Equal(tbp.retBbBis, res.BlockIterStats)
			assert.Equal(tbp.retMcbBis, res.CompactIterStats)
			if tc == "compact" {
				assert.Nil(tbp.inCrPDm)
				assert.Nil(tbp.inCsPS)
			} else {
				assert.Equal(pDm, tbp.inCrPDm)
				assert.Equal(pMan, tbp.inCsPS)
			}
		}
	}
}

// nolint:wsl,gocritic,goconst
func TestMaybeCompactBackup(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	man := &snapshot.Manifest{}
	setChainLength(man, 5)
	root := &testDirEntry{}

	for _, tc := range []string{
		"no previous", "< max-chain-len", ">= max-chain-len", "cb error",
	} {
		t.Logf("Case: %s", tc)

		ba := BackupArgs{Concurrency: 4}
		prevMan := man
		prevRoot := root
		curDm := &dirMeta{}

		tbp := &testBackupProcessor{}
		tbp.retCbBis.initStats()
		tbp.retCbDm = &dirMeta{}

		f := th.fs()
		f.logger = log(ctx)
		assert.NotNil(f.bp)
		f.bp = tbp

		var expError, err error
		var expDm *dirMeta
		expBis := BlockIterStats{}

		switch tc {
		case "no previous":
			prevMan = nil
			prevRoot = nil
		case "< max-chain-len":
			ba.MaxChainLength = getChainLength(prevMan) + 1
		case ">= max-chain-len":
			ba.MaxChainLength = getChainLength(prevMan)
			expBis = tbp.retCbBis
			expDm = tbp.retCbDm
		case "cb error":
			ba.MaxChainLength = getChainLength(prevMan) - 1
			tbp.retCbBis = BlockIterStats{}
			tbp.retCbDm = nil
			expError = ErrInternalError
			tbp.retCbE = expError
		}

		dm, bis, err := f.maybeCompactBackup(ctx, ba, curDm, prevMan, prevRoot)

		if expError == nil {
			assert.NoError(err)
			assert.Equal(expDm, dm)
			assert.Equal(expBis, bis)

			switch tc {
			case ">= max-chain-len":
				assert.Equal(getChainLength(prevMan), tbp.inCbCl)
				assert.Equal(ba.Concurrency, tbp.inCbC)
			}
		} else {
			assert.Error(err)
			assert.Regexp(expError.Error(), err.Error())
			assert.Nil(dm)
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
		"GetBlocks error", "run error", "success",
	} {
		t.Logf("Case: %s", tc)

		f := th.fs()
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
		var bis BlockIterStats
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
			dm, bis, err = f.backupBlocks(ctx, tBR, numWorkers)
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
				assert.Equal(int64((^uint64(0) >> 1)), bis.MinBlockAddr)
				assert.Equal(int64(-1), bis.MaxBlockAddr)
				assert.Equal(0, bis.NumBlocks)
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
	inLpsPvsID string
	retLpsDm   *dirMeta
	retLpsRe   fs.Directory
	retLpsE    error
	retLpsS    *snapshot.Manifest

	inBbBR   volume.BlockReader
	inBbNw   int
	retBbDm  *dirMeta
	retBbBis BlockIterStats
	retBbE   error

	inCrCDm *dirMeta
	inCrPDm *dirMeta
	retCrDm *dirMeta
	retCrE  error

	inCsRDm *dirMeta
	inCsPS  *snapshot.Manifest
	retCsS  *snapshot.Manifest
	retCsE  error

	inCbDm   *dirMeta
	inCbPr   fs.Directory
	inCbCl   int
	inCbC    int
	retCbDm  *dirMeta
	retCbBis BlockIterStats
	retCbE   error

	inMcbA    BackupArgs
	inMcbD    *dirMeta
	inMcbPm   *snapshot.Manifest
	inMcbPr   fs.Directory
	retMcbD   *dirMeta
	retMcbBis BlockIterStats
	retMcbE   error

	inWdrPp  parsedPath
	inWdrDm  *dirMeta
	inWdrWst bool
	retWdrE  error
}

var _ backupProcessor = (*testBackupProcessor)(nil)

func (tbp *testBackupProcessor) linkPreviousSnapshot(ctx context.Context, prevVolumeSnapshotID string) (*dirMeta, *snapshot.Manifest, fs.Directory, error) {
	tbp.inLpsPvsID = prevVolumeSnapshotID

	return tbp.retLpsDm, tbp.retLpsS, tbp.retLpsRe, tbp.retLpsE
}

func (tbp *testBackupProcessor) backupBlocks(ctx context.Context, br volume.BlockReader, numWorkers int) (*dirMeta, BlockIterStats, error) {
	tbp.inBbBR = br
	tbp.inBbNw = numWorkers

	return tbp.retBbDm, tbp.retBbBis, tbp.retBbE
}

func (tbp *testBackupProcessor) createRoot(ctx context.Context, curDm, prevRootDm *dirMeta) (*dirMeta, error) {
	tbp.inCrCDm = curDm
	tbp.inCrPDm = prevRootDm

	return tbp.retCrDm, tbp.retCrE
}

func (tbp *testBackupProcessor) commitSnapshot(ctx context.Context, rootDir *dirMeta, psm *snapshot.Manifest) (*snapshot.Manifest, error) {
	tbp.inCsRDm = rootDir
	tbp.inCsPS = psm

	return tbp.retCsS, tbp.retCsE
}

func (tbp *testBackupProcessor) compactBackup(ctx context.Context, curDm *dirMeta, prevRoot fs.Directory, chainLen, concurrency int) (*dirMeta, BlockIterStats, error) {
	tbp.inCbDm = curDm
	tbp.inCbPr = prevRoot
	tbp.inCbCl = chainLen
	tbp.inCbC = concurrency

	if tbp.retCbDm == nil && tbp.retCbE == nil {
		return curDm, BlockIterStats{}, nil
	}

	return tbp.retCbDm, tbp.retCbBis, tbp.retCbE
}

func (tbp *testBackupProcessor) maybeCompactBackup(ctx context.Context, args BackupArgs, curDm *dirMeta, prevMan *snapshot.Manifest, prevRoot fs.Directory) (*dirMeta, BlockIterStats, error) {
	tbp.inMcbA = args
	tbp.inMcbD = curDm
	tbp.inMcbPm = prevMan
	tbp.inMcbPr = prevRoot

	return tbp.retMcbD, tbp.retMcbBis, tbp.retMcbE
}

func (tbp *testBackupProcessor) writeDirToRepo(ctx context.Context, pp parsedPath, dir *dirMeta, writeSubTree bool) error {
	tbp.inWdrPp = pp
	tbp.inWdrDm = dir
	tbp.inWdrWst = writeSubTree

	return tbp.retWdrE
}
