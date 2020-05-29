package volumefs

import (
	"context"
	"io"
	"testing"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
	vmgr "github.com/kopia/kopia/volume/fake"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestWriteFileToRepo(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	profile := &vmgr.ReaderProfile{
		Ranges: []vmgr.BlockAddrRange{},
	}

	for _, tc := range []string{
		"no src", "src", "copy err", "writer err",
	} {
		t.Logf("Case: %s", tc)

		var expError error
		var src io.Reader
		var buf []byte
		pp := parsedPath{"0", "1", "1"}
		expSz := 4096

		tRC := &testRC{}
		tRC.retReadN = []int{expSz}
		tRC.retReadErr = []error{nil, io.EOF}
		tWC := &testWC{}
		tWC.retWriteN = expSz
		tWC.retResultID = object.ID("oid")
		tRepo := &testRepo{}
		tRepo.retNowW = tWC
		f := th.fsForBackupTests(profile)
		f.repo = tRepo
		f.logger = log(ctx)
		f.layoutProperties.initLayoutProperties(expSz, 8, 2) // reset for this test

		switch tc {
		case "no src":
			expSz = 0
		case "src":
			src = tRC
		case "copy err":
			src = tRC
			expError = ErrInternalError
			tRC.retReadErr = []error{expError}
			tRC.retReadN = nil
		case "writer err":
			expError = ErrInvalidSnapshot
			tWC.retResultE = expError
			tWC.retResultID = object.ID("")
		}

		id, sz, err := f.writeFileToRepo(ctx, pp, src, buf)

		if expError == nil {
			assert.NoError(err)
			assert.Equal(tWC.retResultID, id)
			assert.Equal(int64(expSz), sz)
			expWO := object.WriterOptions{
				Description: "FILE:" + pp.Last(),
				AsyncWrites: 1,
			}
			assert.Equal(expWO, tRepo.inNowO)
			assert.True(tWC.closeCalled)
		} else {
			assert.Equal(expError, err)
		}
	}
}

// nolint:wsl,gocritic,gocyclo
func TestWriteDirToRepo(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	profile := &vmgr.ReaderProfile{
		Ranges: []vmgr.BlockAddrRange{},
	}

	for _, tc := range []string{
		"root only", "subtree", "subtree error",
		"subdir missing oid", "subdir missing summary", "file missing oid",
		"write error", "result error",
	} {
		t.Logf("Case: %s", tc)

		tRepo := &testRepo{}
		tRepo.genNowW = true
		f := th.fsForBackupTests(profile)
		f.repo = tRepo
		f.logger = log(ctx)
		f.layoutProperties.initLayoutProperties(4096, 8, 2) // reset for this test

		f0 := &fileMeta{name: "0", oid: tRepo.genID(false)}
		f1 := &fileMeta{name: "1", oid: tRepo.genID(false)}
		f3 := &fileMeta{name: "3", oid: tRepo.genID(false)}
		c0_0_0 := &dirMeta{name: "0", files: []*fileMeta{f0, f1}}
		c0_0_1 := &dirMeta{name: "1", files: []*fileMeta{f3}}
		c0_0 := &dirMeta{name: "0", subdirs: []*dirMeta{c0_0_0, c0_0_1}}
		c0 := &dirMeta{name: currentSnapshotDirName, subdirs: []*dirMeta{c0_0}}
		rootDir := &dirMeta{subdirs: []*dirMeta{c0}}

		var expError, err error
		writeSubTree := false
		pp := parsedPath{}
		dm := rootDir
		expW := 1

		err = f.createMetadataFiles(ctx, rootDir)
		assert.NoError(err)

		expW += len(tRepo.genW) // account for the metadata files

		switch tc {
		case "root only":
			// all subdirs need oids and summaries so fake them
			for i := range dm.subdirs {
				dm.subdirs[i].oid = tRepo.genID(true)
				dm.subdirs[i].summary = &fs.DirectorySummary{}
			}
		case "subtree":
			writeSubTree = true
			expW += 4 // number c* dirs
		case "subtree error":
			writeSubTree = true
			expError = ErrOutOfRange
			tRepo.genUseW = make([]*testWC, expW+4)
			tRepo.genUseW[expW+3] = &testWC{retResultE: expError}
		case "subdir missing oid":
			expError = ErrInternalError
			for i := range dm.subdirs {
				dm.subdirs[i].summary = &fs.DirectorySummary{}
			}
		case "subdir missing summary":
			expError = ErrInternalError
			for i := range dm.subdirs {
				dm.subdirs[i].oid = tRepo.genID(true)
			}
		case "file missing oid":
			for i := range dm.subdirs {
				dm.subdirs[i].oid = tRepo.genID(true)
				dm.subdirs[i].summary = &fs.DirectorySummary{}
			}
			expError = ErrInternalError
			for i := range dm.files {
				dm.files[i].oid = ""
			}
		case "write error":
			tRepo.genNowW = false
			expError = ErrInvalidArgs
			tWC := &testWC{retWriteErr: expError}
			tRepo.retNowW = tWC
			for i := range dm.subdirs {
				dm.subdirs[i].oid = tRepo.genID(true)
				dm.subdirs[i].summary = &fs.DirectorySummary{}
			}
		case "result error":
			tRepo.genNowW = false
			expError = ErrInvalidArgs
			tWC := &testWC{retResultE: expError}
			tRepo.retNowW = tWC
			for i := range dm.subdirs {
				dm.subdirs[i].oid = tRepo.genID(true)
				dm.subdirs[i].summary = &fs.DirectorySummary{}
			}
		}

		err = f.writeDirToRepo(ctx, pp, dm, writeSubTree)

		if expError == nil {
			assert.NoError(err)
			expWO := object.WriterOptions{
				Description: "DIR:" + dm.name,
				Prefix:      "k",
			}
			assert.Equal(expWO, tRepo.inNowO)
			assert.Equal(expW, len(tRepo.genW))
			for _, tw := range tRepo.genW {
				assert.True(tw.closeCalled)
				t.Logf("** W %s %d", tw.desc, tw.sumWriteN)
			}
			assert.NotNil(dm.summary)
			assert.NotEmpty(dm.oid)
		} else {
			assert.Error(err)
			assert.Regexp(expError.Error(), err.Error())
		}
	}
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
