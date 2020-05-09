package volumefs

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/kopia/kopia/volume"

	"github.com/pkg/errors"
)

// Restore constants.
const (
	DefaultRestoreConcurrency = 4
)

// Restore extracts a snapshot to a volume.
// The provided volume manager must provide a BlockWriter interface.
func (f *Filesystem) Restore(ctx context.Context, previousSnapshotID string) error {
	if previousSnapshotID == "" {
		return ErrInvalidArgs
	}

	f.logger = log(ctx)

	// early check that the volume manager has a block writer
	_, err := f.VolumeManager.GetBlockWriter(volume.GetBlockWriterArgs{})
	if err == volume.ErrNotSupported {
		return err
	}

	f.previousRootEntry, err = f.findPreviousSnapshot(ctx, previousSnapshotID)
	if err != nil {
		return err
	}

	if err = f.recoverMetadataFromRootEntry(ctx, f.previousRootEntry); err != nil { // nolint:gocritic
		return err
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
		f.logger.Errorf("get block writer: %v", err)
		return err
	}

	numWorkers := DefaultRestoreConcurrency
	if f.RestoreConcurrency > 0 {
		numWorkers = f.RestoreConcurrency
	}

	rs := &restoreSession{}

	rs.init(f, bw, numWorkers)

	defer rs.terminate()

	// start session workers on goroutines
	wg := sync.WaitGroup{}

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)

		go func() {
			rs.workerBody(ctx)
			wg.Done()
		}()
	}

	tB := time.Now()

	// walk the tree and generate the block addresses to restore
	rs.restoreDir(ctx, f.previousRootEntry, nil)

	// stop the workers
	rs.stopWorkers()

	// wait for the workers
	wg.Wait()

	tA := time.Now()

	f.logger.Debugf("restore complete: workers[%d] dur[%s] bytes[0%016x] err[%v]", numWorkers, tA.Sub(tB), rs.bytesWritten, rs.err)

	return rs.err
}

func (f *Filesystem) findPreviousSnapshot(ctx context.Context, prevSnapID string) (fs.Directory, error) {
	var err error              // nolint:wsl
	var man *snapshot.Manifest // nolint:wsl
	var rootEntry fs.Entry     // nolint:wsl
	var rootOID object.ID      // nolint:wsl

	if rootOID, err = object.ParseID(prevSnapID); err == nil {
		if man, err = f.findSnapshotManifest(ctx, rootOID); err == nil {
			if rootEntry, err = snapshotfs.SnapshotRoot(f.Repo, man); err == nil {
				if !rootEntry.IsDir() {
					return nil, fmt.Errorf("expected rootEntry to be a directory") // nolint
				}

				dfe := rootEntry.(fs.Directory)

				return dfe, nil
			}
		}
	}

	return nil, err
}

// scanGetSnapshotManifest returns the latest complete manifest containing the specified snapshot ID.
func (f *Filesystem) findSnapshotManifest(ctx context.Context, oid object.ID) (*snapshot.Manifest, error) {
	man, err := snapshot.ListSnapshots(ctx, f.Repo, f.SourceInfo())
	if err != nil {
		return nil, err
	}

	var latest *snapshot.Manifest

	for _, m := range man {
		if m.RootObjectID() == oid && m.IncompleteReason == "" && (latest == nil || m.StartTime.After(latest.StartTime)) {
			latest = m
		}
	}

	if latest != nil {
		log(ctx).Debugf("found manifest %s modTime:%s", latest.ID, latest.RootEntry.ModTime)
		return latest, nil
	}

	return nil, fmt.Errorf("manifest not found")
}

// restoreSession is a helper for Restore
type restoreSession struct {
	f            *Filesystem
	bw           volume.BlockWriter
	fileChan     chan restoreFileData
	stopChan     chan struct{}
	mux          sync.Mutex
	err          error
	bytesWritten int64
}

type restoreFileData struct {
	fe        fs.File
	blockAddr int64
}

func (r *restoreSession) init(f *Filesystem, bw volume.BlockWriter, numWorkers int) {
	r.f = f
	r.bw = bw
	r.fileChan = make(chan restoreFileData, numWorkers)
	r.stopChan = make(chan struct{})
}

// setError aborts the restore session on the first error posted
func (r *restoreSession) setError(err error) {
	r.mux.Lock()
	defer r.mux.Unlock()

	if r.err == nil {
		r.err = err // first one wins
		close(r.stopChan)
	}
}

func (r *restoreSession) terminate() {
	r.mux.Lock()
	defer r.mux.Unlock()

	if r.err == nil { // not aborted
		close(r.stopChan)
	}
}

func (r *restoreSession) wroteBytes(n int64) {
	r.mux.Lock()
	defer r.mux.Unlock()

	r.bytesWritten += n
}

// restoreDir descends the snapshot subtree and collects the files to be restored
func (r *restoreSession) restoreDir(ctx context.Context, de fs.Directory, ppp parsedPath) {
	var err error

	pp := parsedPath{}

	if ppp != nil {
		pp = append(ppp, de.Name()) // nolint:gocritic
	}

	entries, err := de.Readdir(ctx)
	if err != nil {
		r.setError(errors.Wrap(err, fmt.Sprintf("error reading %s", pp)))
		return
	}

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

		fpp := append(pp, name)
		blockAddr := r.f.pathToAddr(fpp)
		rfd := restoreFileData{
			fe:        fe,
			blockAddr: blockAddr,
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
func (r *restoreSession) workerBody(ctx context.Context) {
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
			r.f.logger.Errorf("PutBlock 0x%0x: %v", rfd.blockAddr, err)
			r.setError(err)

			return
		}

		defer r.closeAndLogError(wc, "wc")

		fsr, err := rfd.fe.Open(ctx)
		if err != nil {
			r.f.logger.Errorf("Open 0x%0x: %v", rfd.blockAddr, err)
			r.setError(err)

			return
		}

		defer r.closeAndLogError(fsr, "fsr")

		n, err := io.CopyBuffer(wc, fsr, buf)
		if err != nil {
			r.f.logger.Errorf("Copy 0x%0x: %v", rfd.blockAddr, err)
			r.setError(err)

			return
		}

		r.wroteBytes(n)
	}
}

func (r *restoreSession) closeAndLogError(c io.Closer, what string) {
	if err := c.Close(); err != nil {
		r.f.logger.Errorf("close %s error: %v", what, err)
	}
}

func (r *restoreSession) stopWorkers() {
	close(r.fileChan)
}
