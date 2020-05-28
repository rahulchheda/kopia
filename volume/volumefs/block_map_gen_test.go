package volumefs

import (
	"testing"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/internal/parallelwork"
	"github.com/kopia/kopia/repo/object"
	vmgr "github.com/kopia/kopia/volume/fake"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestBmgInit(t *testing.T) {
	assert := assert.New(t)

	f := &Filesystem{}
	f.dirSz = 256

	bmg := &blockMapGenerator{}
	bmg.Init(f, 2, 1)

	assert.Equal(f, bmg.f)
	assert.Equal(2, bmg.chainLen)
	assert.Equal(1, bmg.concurrency)
	assert.NotNil(bmg.bm)
	assert.True(len(bmg.roots) == 0)
	assert.True(cap(bmg.roots) == bmg.chainLen+1)
}

// nolint:wsl,gocritic
func TestBmgFindRoots(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	profile := &vmgr.ReaderProfile{
		Ranges: []vmgr.BlockAddrRange{},
	}

	tRepo := &testRepo{}

	f := th.fsForBackupTests(profile)
	f.repo = tRepo
	f.dirSz = 256
	f.logger = log(ctx)

	entriesPrevOnly := fs.Entries{
		&testFileEntry{name: "foo"}, // skipped
		&testDirEntry{name: previousSnapshotDirName},
	}
	entriesCur0 := fs.Entries{
		&testDirEntry{name: currentSnapshotDirName},
	}
	entriesCur1 := fs.Entries{
		&testDirEntry{name: currentSnapshotDirName},
		&testDirEntry{name: previousSnapshotDirName, retReadDirE: entriesCur0},
	}

	for _, tc := range []string{
		"readdir error", "no current root", "no previous", "recurse",
	} {
		bmg := &blockMapGenerator{}
		bmg.Init(f, 2, 1)

		de := &testDirEntry{}
		chainIdx := 0
		var expError error

		switch tc {
		case "readdir error":
			de.retReadDirE = nil
			expError = ErrInternalError
			de.retReadDirErr = expError
		case "no current root":
			de.retReadDirE = entriesPrevOnly
			expError = ErrInvalidSnapshot
		case "no previous":
			de.retReadDirE = entriesCur0
		case "recurse":
			de.retReadDirE = entriesCur1
			chainIdx = 1
		}

		err := bmg.findRoots(ctx, de, chainIdx)

		if expError == nil {
			assert.NoError(err)

			switch tc {
			case "no previous":
				assert.Equal(1, len(bmg.roots))
				assert.Equal(entriesCur0[0], bmg.roots[0])
			case "recurse":
				assert.Equal(2, len(bmg.roots))
				assert.Equal(entriesCur0[0], bmg.roots[0])
				assert.Equal(entriesCur1[0], bmg.roots[1])
			}
		} else {
			assert.Equal(expError, err)
		}
	}
}

// nolint:wsl,gocritic
func TestBmgProcessChain(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	profile := &vmgr.ReaderProfile{
		Ranges: []vmgr.BlockAddrRange{},
	}

	tRepo := &testRepo{}

	f := th.fsForBackupTests(profile)
	f.repo = tRepo
	f.dirSz = 8
	f.depth = 2
	f.logger = log(ctx)

	d0_0_0 := fs.Entries{
		&testFileEntry{name: "0", oid: "oid0"},
		&testFileEntry{name: "1", oid: "oid1"},
	}
	d0_0 := fs.Entries{
		&testDirEntry{name: "0", retReadDirE: d0_0_0},
	}
	d0 := fs.Entries{
		&testDirEntry{name: "0", retReadDirE: d0_0},
	}

	for _, tc := range []string{
		"readdir error", "non-leaf", "leaf", "tree",
	} {
		bmg := &blockMapGenerator{}
		bmg.Init(f, 2, 1)
		assert.NotNil(bmg.bm)

		var expError error
		var pp parsedPath
		de := &testDirEntry{}

		switch tc {
		case "readdir error":
			de.retReadDirE = nil
			expError = ErrInternalError
			de.retReadDirErr = expError
		case "non-leaf":
			de.retReadDirE = d0_0
		case "leaf":
			de.retReadDirE = d0_0_0
			pp = parsedPath{"0", "0", "0"}
		case "tree":
			de.retReadDirE = d0
		}

		var err error

		if tc != "tree" {
			tq := &testPWQ{}
			bmg.queue = tq
			el := bmg.enqueue(ctx, de, pp)
			assert.Equal(bmg, el.bmg)
			assert.Equal(pp, el.pp)
			assert.Equal(de, el.de)
			err = tq.inEnqueueBackC()
		} else {
			err = bmg.processChain(ctx, de, 1)
		}

		if expError == nil {
			assert.NoError(err)
			switch tc {
			case "non-leaf":
				bi := bmg.bm.Iterator()
				defer bi.Close()
				bam := bi.Next()
				assert.Equal(object.ID(""), bam.Oid)
			case "leaf", "tree":
				bi := bmg.bm.Iterator()
				defer bi.Close()
				bam := bi.Next()
				assert.Equal(int64(0), bam.BlockAddr)
				assert.Equal(object.ID("oid0"), bam.Oid)
				bam = bi.Next()
				assert.Equal(int64(1), bam.BlockAddr)
				assert.Equal(object.ID("oid1"), bam.Oid)
				bam = bi.Next()
				assert.Equal(object.ID(""), bam.Oid)
			}
		} else {
			assert.Equal(expError, err)
		}
	}
}

// nolint:wsl,gocritic
func TestEffectiveBlockMap(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	profile := &vmgr.ReaderProfile{
		Ranges: []vmgr.BlockAddrRange{},
	}

	tRepo := &testRepo{}

	f := th.fsForBackupTests(profile)
	f.repo = tRepo
	f.initLayoutProperties(4096, 8, 2) // reset for UT
	f.logger = log(ctx)

	p0_0_0 := fs.Entries{
		&testFileEntry{name: "0", oid: "oid0p"},
		&testFileEntry{name: "1", oid: "oid1p"},
	}
	p0_0 := fs.Entries{
		&testDirEntry{name: "0", retReadDirE: p0_0_0},
	}
	p0 := fs.Entries{
		&testDirEntry{name: "0", retReadDirE: p0_0},
	}
	pRoot := fs.Entries{
		&testDirEntry{name: currentSnapshotDirName, retReadDirE: p0},
	}

	c0_0_0 := fs.Entries{
		&testFileEntry{name: "0", oid: "oid0c"},
	}
	c0_0_1 := fs.Entries{
		&testFileEntry{name: "3", oid: "oid11c"},
	}
	c0_0 := fs.Entries{
		&testDirEntry{name: "0", retReadDirE: c0_0_0},
		&testDirEntry{name: "1", retReadDirE: c0_0_1},
	}
	c0 := fs.Entries{
		&testDirEntry{name: "0", retReadDirE: c0_0},
	}
	cRoot := fs.Entries{
		&testDirEntry{name: currentSnapshotDirName, retReadDirE: c0},
		&testDirEntry{name: previousSnapshotDirName, retReadDirE: pRoot},
	}

	b0 := fs.Entries{
		&testDirEntry{name: "0", retReadDirErr: ErrOutOfRange},
	}
	badRoot := fs.Entries{
		&testDirEntry{name: currentSnapshotDirName, retReadDirE: b0},
	}

	for _, tc := range []string{
		"no chain", "chain", "findRoots error", "process chain error",
	} {
		t.Logf("Case: %s", tc)

		bmg := &blockMapGenerator{}
		bmg.Init(f, 2, 1)
		assert.NotNil(bmg.bm)

		var expError error
		de := &testDirEntry{}
		chainLen := 0

		switch tc {
		case "no chain":
			de.retReadDirE = pRoot
		case "chain":
			de.retReadDirE = cRoot
			chainLen = 1
		case "findRoots error":
			expError = ErrOutOfRange
			de.retReadDirErr = expError
		case "process chain error":
			expError = ErrOutOfRange
			de.retReadDirE = badRoot
		}

		// concurrency > max directory depth to exercise the parallelwork.NewQueue() boundary condition.
		bm, err := f.effectiveBlockMap(ctx, chainLen, de, 32)

		if expError == nil {
			assert.NoError(err)
			switch tc {
			case "no chain":
				bi := bm.Iterator()
				defer bi.Close()
				bam := bi.Next()
				assert.Equal(int64(0), bam.BlockAddr)
				assert.Equal(object.ID("oid0p"), bam.Oid)
				bam = bi.Next()
				assert.Equal(int64(1), bam.BlockAddr)
				assert.Equal(object.ID("oid1p"), bam.Oid)
				bam = bi.Next()
				assert.Equal(object.ID(""), bam.Oid)
			case "chain":
				bi := bm.Iterator()
				defer bi.Close()
				bam := bi.Next()
				assert.Equal(int64(0), bam.BlockAddr)
				assert.Equal(object.ID("oid0c"), bam.Oid)
				bam = bi.Next()
				assert.Equal(int64(1), bam.BlockAddr)
				assert.Equal(object.ID("oid1p"), bam.Oid)
				bam = bi.Next()
				assert.Equal(int64(11), bam.BlockAddr)
				assert.Equal(object.ID("oid11c"), bam.Oid)
				bam = bi.Next()
				assert.Equal(object.ID(""), bam.Oid)
			}
		} else {
			assert.Equal(expError, err)
		}
	}
}

type testPWQ struct {
	inEnqueueBackC parallelwork.CallbackFunc

	retProcessE error
	inProcessW  int
}

func (q *testPWQ) EnqueueBack(callback parallelwork.CallbackFunc) {
	q.inEnqueueBackC = callback
}

func (q *testPWQ) Process(workers int) error {
	q.inProcessW = workers
	return q.retProcessE
}
