// Package volumefs presents a representation of a volume's block address space and is
// intended to support snapshots over block devices in a manner that works within the
// existing snapshot management paradigm, including its garbage collection mechanism.
//
// It uses the abstraction of a volume.Manager to access volume data blocks, albeit in
// a snapshot optimized block size defined by the filesystem itself. This "snapshot
// block" size will typically be a multiple of the volume block size.  The filesystem
// represents snapshot blocks as "files" named for their snapshot block address. Files
// are organized in a directory hierarchy to address scale. See layout.go for details.
//
// The first snapshot of the volume backs up all volume blocks (typically only the ones
// written out at this time). On subsequent snapshots the filesystem supports volumes
// with underlying support for change-block-tracking: only the snapshot blocks changed
// since the previous volume snapshot need be considered, as the filesystem recovers
// its previous state from the repository.
package volumefs

import (
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/volume"
)

var log = logging.GetContextLoggerFunc("volume/filesystem")

// package errors.
var (
	ErrOutOfRange       = fmt.Errorf("block address out of range")
	ErrInvalidArgs      = fmt.Errorf("invalid arguments")
	ErrInternalError    = fmt.Errorf("internal error")
	ErrInvalidSnapshot  = fmt.Errorf("invalid snapshot")
	ErrSnapshotNotFound = fmt.Errorf("snapshot not found")
)

// FilesystemArgs contains arguments to create a filesystem
type FilesystemArgs struct {
	// The repository.
	Repo repo.Repository
	// The volume manager.
	VolumeManager volume.Manager
	// The identifier of the volume being backed up or restored.
	VolumeID string
	// The identifier of the volume snapshot being backed up or restored.
	VolumeSnapshotID string
	// Profile containing location and credential information needed to access the volume.
	VolumeAccessProfile interface{}
}

// Validate checks for required fields
func (a *FilesystemArgs) Validate() error {
	if a.Repo == nil || a.VolumeManager == nil || a.VolumeID == "" || a.VolumeSnapshotID == "" || a.VolumeAccessProfile == nil {
		return ErrInvalidArgs
	}

	return nil
}

// SourceInfo generates a snapshotSourceInfo
func (a *FilesystemArgs) SourceInfo() snapshot.SourceInfo {
	return snapshot.SourceInfo{
		Path:     path.Join("/volume", a.VolumeID),
		Host:     a.Repo.Hostname(),
		UserName: a.Repo.Username(),
	}
}

// Filesystem is a pseudo-filesystem mapping a provider volume block
// address space to the snapshot block address space.
type Filesystem struct {
	FilesystemArgs
	layoutProperties
	prevVolumeSnapshotID string
	epoch                time.Time // all changes stamped with this time
	logger               logging.Logger
	blockPool            sync.Pool
	bp                   backupProcessor
	rp                   restoreProcessor
	up                   uploader
	sp                   snapshotProcessor
	repo                 repository
}

// repository is the subset of repo.Repository commands used
type repository interface {
	OpenObject(ctx context.Context, id object.ID) (object.Reader, error)
	NewObjectWriter(ctx context.Context, opt object.WriterOptions) object.Writer
	Flush(ctx context.Context) error
}

// New returns a new volume filesystem
func New(args *FilesystemArgs) (*Filesystem, error) {
	if err := args.Validate(); err != nil {
		return nil, err
	}

	f := &Filesystem{}
	f.FilesystemArgs = *args
	f.blockPool.New = func() interface{} {
		return new(block)
	}

	f.setDefaultLayoutProperties() // block size may be reset from previous repo snapshot

	f.bp = f
	f.rp = f
	f.up = f
	f.sp = &snapshotHelper{}
	f.repo = f.Repo

	return f, nil
}

// createRoot creates the root directory with references to current, previous and meta.
func (f *Filesystem) createRoot(ctx context.Context, curDm, prevRootDm *dirMeta) (*dirMeta, error) {
	rootDir := &dirMeta{
		name: "/",
	}

	rootDir.insertSubdir(curDm)

	if prevRootDm != nil {
		rootDir.insertSubdir(prevRootDm)
	}

	if err := f.createMetadataFiles(ctx, rootDir); err != nil {
		return nil, err
	}

	return rootDir, nil
}
