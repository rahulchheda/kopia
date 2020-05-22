package volumefs

import (
	"context"
	"io"
	"testing"

	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/volume"
	vmgr "github.com/kopia/kopia/volume/fake"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
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
// func analyseAddresses(t *testing.T, f *Filesystem, prefix string, bal []int64, prev map[string]struct{}) map[string]struct{} { // nolint:unparam
// 	assert := assert.New(t)
// 	updated := map[string]struct{}{}

// 	for _, ba := range bal {
// 		pp, err := f.addrToPath(ba)
// 		assert.NoError(err)

// 		for i := 0; i < len(pp)-1; i++ { // don't consider the file in this loop
// 			dir := pp[0 : i+1].String()
// 			if _, ok := updated[dir]; ok {
// 				continue
// 			}

// 			if prev != nil { // don't count if present in prev
// 				if _, ok := prev[dir]; ok {
// 					// t.Logf("%s: Dir in previous [%s]", prefix, dir)
// 					continue
// 				}
// 				// if not in prev then parent must be modified too
// 				pDir := pp[0:i].String()
// 				// t.Logf("%s: dir pDir updated [%s]", prefix, pDir)
// 				updated[pDir] = struct{}{}
// 			}

// 			// t.Logf("%s: Dir updated [%s]", prefix, dir)
// 			updated[dir] = struct{}{}
// 		}

// 		fn := pp.String()
// 		if prev != nil { // if file not in prev then parent must be modified too
// 			if _, ok := prev[fn]; !ok {
// 				pDir := pp[0 : len(pp)-1].String()
// 				// t.Logf("%s: file pDir updated [%s]", prefix, pDir)
// 				updated[pDir] = struct{}{}
// 			}
// 		}

// 		// t.Logf("%s: File updated [%s]", prefix, fn)
// 		updated[fn] = struct{}{}
// 	}

// 	return updated
// }

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

type testUploader struct {
	inWriteDirPP parsedPath
	inWriteDirDm *dirMeta
	inWriteDirST bool
	retWriteDirE error

	inWriteFilePP  parsedPath
	inWriteFileRC  io.Reader
	inWriteFileBuf []byte
	retWriteFileID object.ID
	retWriteFileSz int64
	retWriteFileE  error
}

func (tu *testUploader) writeDirToRepo(ctx context.Context, pp parsedPath, dir *dirMeta, writeSubTree bool) error {
	tu.inWriteDirPP = pp
	tu.inWriteDirDm = dir
	tu.inWriteDirST = writeSubTree

	return tu.retWriteDirE
}

func (tu *testUploader) writeFileToRepo(ctx context.Context, pp parsedPath, src io.Reader, buf []byte) (object.ID, int64, error) {
	tu.inWriteFilePP = pp
	tu.inWriteFileRC = src
	tu.inWriteFileBuf = buf

	return tu.retWriteFileID, tu.retWriteFileSz, tu.retWriteFileE
}
