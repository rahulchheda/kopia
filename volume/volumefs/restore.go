package volumefs

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/volume"

	"github.com/pkg/errors"
)

// Restore constants.
const (
	DefaultRestoreConcurrency = 4
)

// RestoreStats contain restore statistics
type RestoreStats struct {
	BytesWritten  int64
	NumDirs       int
	NumFiles      int
	NumDirEntries int64
	MinBlockAddr  int64
	MaxBlockAddr  int64
}

// Restore extracts a snapshot to a volume.
// The provided volume manager must provide a BlockWriter interface.
func (f *Filesystem) Restore(ctx context.Context, prevSnapID string) (RestoreStats, error) {
	if prevSnapID == "" {
		return RestoreStats{}, ErrInvalidArgs
	}

	f.logger = log(ctx)

	// early check that the volume manager has a block writer
	if _, err := f.VolumeManager.GetBlockWriter(volume.GetBlockWriterArgs{}); err == volume.ErrNotSupported {
		return RestoreStats{}, err
	}

	if err := f.initFromSnapshot(ctx, prevSnapID); err != nil {
		return RestoreStats{}, err
	}

	// now that the metadata is loaded we can initialize the block writer
	gbwArgs := volume.GetBlockWriterArgs{
		VolumeID:       f.VolumeID,
		BlockSizeBytes: f.GetBlockSize(),
		Profile:        f.VolumeAccessProfile,
	}

	f.logger.Debugf("gbw: %#v=%#v", gbwArgs, gbwArgs.Profile)

	bw, err := f.VolumeManager.GetBlockWriter(gbwArgs)
	if err != nil {
		f.logger.Debugf("get block writer: %v", err)
		return RestoreStats{}, err
	}

	numWorkers := DefaultRestoreConcurrency
	if f.RestoreConcurrency > 0 {
		numWorkers = f.RestoreConcurrency
	}

	return f.doRestore(ctx, bw, numWorkers)
}

// doRestore restores a filesystem using the provided BlockWriter and
// the specified degree of concurrency.
func (f *Filesystem) doRestore(ctx context.Context, bw volume.BlockWriter, numWorkers int) (RestoreStats, error) {
	rh := &restoreHelper{}

	rh.init(f, bw, numWorkers)

	defer rh.terminate()

	// start session workers on goroutines
	wg := sync.WaitGroup{}

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)

		go func() {
			rh.workerBody(ctx)
			wg.Done()
		}()
	}

	tB := time.Now()

	// walk the tree and generate the block addresses to restore
	rh.restoreDir(ctx, f.previousRootEntry, nil)

	// stop the workers
	rh.stopWorkers()

	// wait for the workers
	wg.Wait()

	tA := time.Now()

	f.logger.Debugf("restore complete: workers[%d] dur[%s] %#v err[%v]", numWorkers, tA.Sub(tB), rh.RestoreStats, rh.err)

	return rh.RestoreStats, rh.err
}

// restoreHelper is a helper for Restore
type restoreHelper struct {
	f        *Filesystem
	bw       volume.BlockWriter
	fileChan chan restoreFileData
	stopChan chan struct{}
	mux      sync.Mutex
	err      error
	RestoreStats
}

type restoreFileData struct {
	fe        fs.File
	blockAddr int64
}

func (r *restoreHelper) init(f *Filesystem, bw volume.BlockWriter, numWorkers int) {
	r.f = f
	r.bw = bw
	r.fileChan = make(chan restoreFileData, numWorkers)
	r.stopChan = make(chan struct{})
	r.MinBlockAddr = f.maxBlocks
	r.MaxBlockAddr = -1
}

// setError aborts the restore session on the first error posted
func (r *restoreHelper) setError(err error) {
	r.mux.Lock()
	defer r.mux.Unlock()

	if r.err == nil {
		r.err = err // first one wins
		close(r.stopChan)
	}
}

func (r *restoreHelper) terminate() {
	r.mux.Lock()
	defer r.mux.Unlock()

	if r.err == nil { // not aborted
		close(r.stopChan)
	}
}

func (r *restoreHelper) wroteBytes(n int64) {
	r.mux.Lock()
	defer r.mux.Unlock()

	r.BytesWritten += n
}

// restoreDir descends the snapshot subtree and collects the files to be restored
func (r *restoreHelper) restoreDir(ctx context.Context, de fs.Directory, ppp parsedPath) {
	r.NumDirs++

	pp := parsedPath{}

	if ppp != nil {
		pp = append(ppp, de.Name()) // nolint:gocritic
	}

	entries, err := de.Readdir(ctx)
	if err != nil {
		r.setError(errors.Wrap(err, fmt.Sprintf("error reading %s", pp)))
		return
	}

	r.NumDirEntries += int64(len(entries))

	r.f.logger.Debugf("restoreDir [%s] => #%d", pp, len(entries))

	for _, entry := range entries {
		sde, ok := entry.(fs.Directory)
		if !ok {
			continue
		}
		select {
		case <-r.stopChan:
			return // aborted
		default:
		}
		r.restoreDir(ctx, sde, pp)
	}

	for _, entry := range entries {
		fe, ok := entry.(fs.File)
		if !ok {
			continue
		}

		name := fe.Name()
		isMeta, _, _ := r.f.isMetaFile(name)

		if isMeta {
			continue
		}

		r.NumFiles++

		fpp := append(pp, name)
		blockAddr := r.f.pathToAddr(fpp)
		rfd := restoreFileData{
			fe:        fe,
			blockAddr: blockAddr,
		}

		if blockAddr < r.MinBlockAddr {
			r.MinBlockAddr = blockAddr
		}

		if blockAddr > r.MaxBlockAddr {
			r.MaxBlockAddr = blockAddr
		}

		select {
		case r.fileChan <- rfd:
		case <-r.stopChan:
			return // aborted
		}
	}
}

// workerBody should run in its own goroutine.
// Multiple such goroutines can run concurrently.
func (r *restoreHelper) workerBody(ctx context.Context) {
	buf := r.bw.AllocateBuffer()
	r.f.logger.Debugf("allocated buffer of len %d", len(buf))

	for {
		var rfd restoreFileData

		select {
		case rfd = <-r.fileChan:
			if rfd.fe == nil {
				return
			}
		case <-r.stopChan:
			return // aborted
		}

		wc, err := r.bw.PutBlock(ctx, rfd.blockAddr)
		if err != nil {
			r.f.logger.Debugf("PutBlock 0x%0x: %v", rfd.blockAddr, err)
			r.setError(err)

			return
		}

		defer r.closeAndLogError(wc, "wc")

		fsr, err := rfd.fe.Open(ctx)
		if err != nil {
			r.f.logger.Debugf("Open 0x%0x: %v", rfd.blockAddr, err)
			r.setError(err)

			return
		}

		defer r.closeAndLogError(fsr, "fsr")

		n, err := io.CopyBuffer(wc, fsr, buf)
		if err != nil {
			r.f.logger.Debugf("Copy 0x%0x: %v", rfd.blockAddr, err)
			r.setError(err)

			return
		}

		r.wroteBytes(n)
	}
}

func (r *restoreHelper) closeAndLogError(c io.Closer, what string) {
	if err := c.Close(); err != nil {
		r.f.logger.Debugf("close %s error: %v", what, err)
		r.setError(err)
	}
}

func (r *restoreHelper) stopWorkers() {
	close(r.fileChan)
}
