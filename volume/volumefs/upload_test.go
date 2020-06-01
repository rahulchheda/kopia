package volumefs

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
	"testing"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestWriteFileToRepo(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

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
		f := th.fs()
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

	for _, tc := range []string{
		"root only", "subtree", "subtree error",
		"subdir missing oid", "subdir missing summary", "file missing oid",
		"write error", "result error",
	} {
		t.Logf("Case: %s", tc)

		tRepo := &testRepo{}
		tRepo.genNowW = true
		f := th.fs()
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
			tRepo.saveWrites = true
		case "subtree":
			writeSubTree = true
			expW += 4 // number c* dirs
		case "subtree error":
			writeSubTree = true
			expError = ErrOutOfRange
			tRepo.genUseW = make([]*testWC, tRepo.genIDCnt+2)
			tRepo.genUseW[tRepo.genIDCnt+1] = &testWC{retResultE: expError}
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
			if tc == "root only" {
				tw := tRepo.genW[len(tRepo.genW)-1]
				assert.True(tw.saveWrites)
				var dirMan *snapshot.DirManifest
				err = json.NewDecoder(&tw.savedWrites).Decode(&dirMan)
				assert.NoError(err)
				assert.Equal(directoryStreamType, dirMan.StreamType)
				assert.Equal(expW, len(dirMan.Entries))
			}
		} else {
			assert.Error(err)
			assert.Regexp(expError.Error(), err.Error())
		}
	}
}

// this is not really a test but a measurement of directory manifest JSON encoding for various
// directory sizes.
// nolint:wsl,gocritic
func TestEstimateDirManifestSizes(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	oneKi := 1024
	for _, tc := range []struct {
		encoding      int // name encoding
		numEntries    int // dir size
		avgSzPerEntry int
		orderKi       int // dir block size, rounded up
		pctOvhd       int // percentage overhead of the JSON encoding
	}{
		{16, 256, 183, 46, 422},
		{16, 512, 183, 92, 415},
		{16, 1024, 183, 183, 411},
		{16, 2048, 183, 366, 410},
		{16, 4096, 183, 732, 409},

		{32, 256, 183, 46, 422},
		{32, 512, 183, 92, 421},
		{32, 1024, 183, 183, 420},
		{32, 2048, 183, 366, 414},
		{32, 4096, 183, 732, 411},

		{36, 256, 183, 46, 423},
		{36, 512, 183, 92, 421},
		{36, 1024, 183, 183, 420},
		{36, 2048, 183, 365, 416}, // slight O reduction
		{36, 4096, 183, 731, 412}, // slight O reduction
	} {
		t.Logf("Case: %d", tc.numEntries)

		f := &Filesystem{}
		f.logger = log(ctx)

		// Only use directory entries: they have a larger oid than for files + dir summary
		strSz := 0 // essential data len
		dm := &dirMeta{name: fmt.Sprintf("%x", tc.numEntries)}
		for i := 0; i < tc.numEntries; i++ {
			sdm := &dirMeta{
				name:    strconv.FormatInt(int64(i), tc.encoding),
				oid:     "k52f18655d20ab0ba242af328d4a966be",
				summary: &fs.DirectorySummary{},
			}
			dm.insertSubdir(sdm)
			strSz += len(sdm.name) + len(sdm.oid)
		}
		sde := dm.snapshotDirEntry()
		sde.DirSummary = &fs.DirectorySummary{ // fake with large numbers
			TotalFileSize:  1 << 62,
			TotalFileCount: 1 << 40,
			TotalDirCount:  1 << 10,
		}
		// construct the manifest and intercept the byte stream after JSON encoding
		tWC := &testWC{}
		tWC.saveWrites = true
		tRepo := &testRepo{}
		tRepo.retNowW = tWC
		f.repo = tRepo
		err := f.writeDirToRepo(ctx, parsedPath{}, dm, false)
		assert.NoError(err)

		actualBytes := tWC.savedWrites.Len()
		avg := (actualBytes + tc.numEntries - 1) / tc.numEntries // round up
		order := (actualBytes + oneKi - 1) / oneKi               // round up
		overhead := ((actualBytes - strSz) * 100) / strSz
		t.Logf("E:%d #:%d AB=%d Avg=%d Order=%d Ovhd:%d", tc.encoding, tc.numEntries, actualBytes, avg, order, overhead)
		// assert.Equal(tc.avgSzPerEntry, avg)
		// assert.Equal(tc.orderKi, order)
		// assert.Equal(tc.pctOvhd, overhead)
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
