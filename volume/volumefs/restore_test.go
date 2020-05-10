package volumefs

import (
	"fmt"
	"os"
	"path"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/kopia/kopia/volume/blockfile"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestRestore(t *testing.T) {

	// internal "restorer" interface to fake doRestore

}

// nolint:wsl,gocritic
func TestDoRestore(t *testing.T) {
	// provide a fake bw and a fake dir with 0 entries
}

// nolint:wsl,gocritic,gocyclo
func TestRestoreHelper(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	tmpFile := path.Join(os.TempDir(), "testVolume")
	defer func() {
		os.Remove(tmpFile)
	}()
	os.Remove(tmpFile)

	dbs := 4096
	p := &blockfile.Profile{
		Name:                 tmpFile,
		CreateIfMissing:      true,
		DeviceBlockSizeBytes: int64(dbs),
	}
	fr := th.fsForRestoreTests(p)
	fr.logger = log(ctx)

	blocks := []int64{0, 0x10, 0x100, 0x1000, 0x10000, 0x100000} // sorted
	fb := th.fsForBackupTests(nil)
	th.retFS = fb // so we know the layout used in the snap
	snap := th.addSnapshot(ctx, blocks)
	fr.initFromSnapshot(ctx, snap.RootObjectID().String())

	for _, tc := range []string{
		"init", "terminate-on-error", "set-error", "wrote-bytes",
		"restoreDir", "restoreDir-abort-dir", "restoreDir-abort-file", "restoreDir-readdir-error",
		"worker-aborted", "worker-stop", "worker-pb-error", "worker-fe-open-err",
		"worker-wc-write-err", "worker-wrote-bytes", "worker-writer-close-error", "worker-reader-close-error",
	} {
		t.Logf("Case: %s", tc)

		tbw := &testBW{dbs: dbs}
		numWorkers := 1
		mustCloseFileChan := true

		// setup
		switch tc {
		case "init":
			numWorkers = 2
		case "restoreDir-abort-dir":
			fallthrough
		case "restoreDir":
			numWorkers = len(blocks)
		case "restoreDir-abort-file":
			numWorkers = len(blocks) / 2
		}

		rh := &restoreHelper{}
		rh.init(fr, tbw, numWorkers)

		// test
		switch tc {
		case "init":
			assert.Equal(fr, rh.f)
			assert.Equal(tbw, rh.bw)
			assert.NotNil(rh.fileChan)
			assert.NotNil(rh.stopChan)
			for i := 0; i < numWorkers; i++ {
				rh.fileChan <- restoreFileData{}
			}
			assert.Equal(numWorkers, len(rh.fileChan))
			for i := 0; i < numWorkers; i++ {
				<-rh.fileChan
			}
		case "terminate-on-error":
			rh.err = ErrInvalidArgs
			close(rh.stopChan)
			// will not block in terminate()
		case "set-error":
			rh.setError(ErrInvalidArgs)
			rh.setError(ErrOutOfRange)
			assert.Equal(ErrInvalidArgs, rh.err)
			<-rh.stopChan // is closed, terminate will not block
		case "wrote-bytes":
			rh.wroteBytes(10)
			rh.wroteBytes(20)
			assert.Equal(int64(30), rh.BytesWritten)
		case "restoreDir":
			rh.restoreDir(ctx, fr.previousRootEntry, nil)
			assert.True(rh.NumDirs > 0)
			assert.Equal(len(blocks), rh.NumFiles)
			assert.Equal(len(blocks), len(rh.fileChan))
			foundBlocks := []int64{}
			for i := 0; i < len(blocks); i++ {
				rfd := <-rh.fileChan
				assert.NotNil(rfd.fe)
				assert.True(rfd.blockAddr >= 0)
				foundBlocks = append(foundBlocks, rfd.blockAddr)
			}
			sort.Slice(foundBlocks, func(i, j int) bool { return foundBlocks[i] < foundBlocks[j] })
			assert.Equal(blocks, foundBlocks)
			assert.Equal(blocks[0], rh.MinBlockAddr)
			assert.Equal(blocks[len(blocks)-1], rh.MaxBlockAddr)
			assert.True(rh.NumDirEntries > 0)
		case "restoreDir-abort-dir":
			rh.setError(ErrOutOfRange)
			rh.restoreDir(ctx, fr.previousRootEntry, nil)
			assert.True(rh.NumDirs == 1)
		case "restoreDir-abort-file":
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				rh.restoreDir(ctx, fr.previousRootEntry, nil)
				wg.Done()
			}()
			for len(rh.fileChan) != numWorkers {
				time.Sleep(5 * time.Millisecond)
			}
			rh.setError(ErrInvalidArgs)
			wg.Wait()
			assert.Equal(numWorkers+1, rh.NumFiles)
		case "restoreDir-readdir-error":
			tde := &testDirEntry{}
			tde.retReadDirErr = ErrOutOfRange
			rh.restoreDir(ctx, tde, nil)
			assert.True(rh.NumDirs == 1)
			assert.Regexp(tde.retReadDirErr.Error(), rh.err.Error())
			assert.Zero(rh.NumDirEntries)
		case "worker-aborted":
			rh.setError(ErrInvalidArgs)
			rh.workerBody(ctx)
		case "worker-stop":
			rh.fileChan <- restoreFileData{}
			rh.workerBody(ctx)
			assert.NoError(rh.err)
		case "worker-pb-error":
			tbw.putBlockErr = fmt.Errorf("pb-error")
			rh.fileChan <- restoreFileData{blockAddr: 0xabc, fe: &testFileEntry{}}
			rh.workerBody(ctx)
			assert.Error(rh.err)
			assert.Regexp(tbw.putBlockErr.Error(), rh.err.Error())
		case "worker-fe-open-err":
			twc := &testWC{}
			tbw.putBlockWC = twc
			tfe := &testFileEntry{}
			tfe.retOpenErr = ErrInvalidArgs
			rh.fileChan <- restoreFileData{blockAddr: 0xabc, fe: tfe}
			rh.workerBody(ctx)
			assert.Error(rh.err)
			assert.Regexp(tfe.retOpenErr.Error(), rh.err.Error())
			assert.True(twc.closeCalled)
		case "worker-wc-write-err":
			twc := &testWC{}
			tbw.putBlockWC = twc
			tr := &testReader{}
			tr.retReadErr = ErrInvalidArgs
			tfe := &testFileEntry{}
			tfe.retOpenR = tr
			rh.fileChan <- restoreFileData{blockAddr: 0xabc, fe: tfe}
			rh.workerBody(ctx)
			assert.Error(rh.err)
			assert.Regexp(tr.retReadErr.Error(), rh.err.Error())
			assert.True(twc.closeCalled)
			assert.True(tr.closeCalled)
		case "worker-wrote-bytes":
			twc := &testWC{}
			twc.t = t
			tbw.putBlockWC = twc
			tr := &testReader{}
			tr.t = t
			tr.readRemBytes = 2 * dbs
			tfe := &testFileEntry{}
			tfe.retOpenR = tr
			rh.fileChan <- restoreFileData{blockAddr: 0xabc, fe: tfe}
			rh.stopWorkers()
			mustCloseFileChan = false
			rh.workerBody(ctx)
			assert.NoError(rh.err)
			assert.Equal(int64(2*dbs), rh.BytesWritten)
			assert.True(twc.closeCalled)
			assert.True(tr.closeCalled)
		case "worker-writer-close-error":
			twc := &testWC{}
			twc.t = t
			twc.retCloseErr = ErrOutOfRange
			tbw.putBlockWC = twc
			tr := &testReader{}
			tr.t = t
			tr.readRemBytes = 2 * dbs
			tfe := &testFileEntry{}
			tfe.retOpenR = tr
			rh.fileChan <- restoreFileData{blockAddr: 0xabc, fe: tfe}
			rh.stopWorkers()
			mustCloseFileChan = false
			rh.workerBody(ctx)
			assert.Error(rh.err)
			assert.Regexp(twc.retCloseErr.Error(), rh.err.Error())
			assert.Equal(int64(2*dbs), rh.BytesWritten)
			assert.True(twc.closeCalled)
			assert.True(tr.closeCalled)
		case "worker-reader-close-error":
			twc := &testWC{}
			twc.t = t
			tbw.putBlockWC = twc
			tr := &testReader{}
			tr.t = t
			tr.readRemBytes = 2 * dbs
			tr.retCloseErr = ErrInvalidArgs
			tfe := &testFileEntry{}
			tfe.retOpenR = tr
			rh.fileChan <- restoreFileData{blockAddr: 0xabc, fe: tfe}
			rh.stopWorkers()
			mustCloseFileChan = false
			rh.workerBody(ctx)
			assert.Error(rh.err)
			assert.Regexp(tr.retCloseErr.Error(), rh.err.Error())
			assert.Equal(int64(2*dbs), rh.BytesWritten)
			assert.True(twc.closeCalled)
			assert.True(tr.closeCalled)
		}

		// teardown
		if mustCloseFileChan {
			assert.NotPanics(func() {
				rh.stopWorkers()
			})
		}
		assert.NotPanics(func() {
			rh.terminate()
		})
	}

	assert.True(true)
}
