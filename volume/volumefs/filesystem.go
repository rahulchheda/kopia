package volumefs

import (
	"fmt"
	"path"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/volume"
)

// This filesystem presents a representation of a volume's block address space and is
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

var log = logging.GetContextLoggerFunc("volume/filesystem")

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
func (a FilesystemArgs) Validate() error {
	if a.Repo == nil || a.VolumeManager == nil || a.VolumeID == "" || a.VolumeSnapshotID == "" || a.VolumeAccessProfile == nil {
		return fmt.Errorf("invalid FilesystemArgs")
	}
	return nil
}

// SourceInfo generates a snapshotSourceInfo
func (a FilesystemArgs) SourceInfo() snapshot.SourceInfo {
	return snapshot.SourceInfo{
		Path:     path.Join("/volumefs", a.VolumeManager.Type(), a.VolumeID),
		Host:     a.Repo.Hostname(),
		UserName: a.Repo.Username(),
	}
}

// Filesystem is a pseudo-filesystem mapping a provider volume block
// address space to the snapshot block address space.
type Filesystem struct {
	FilesystemArgs
	layoutProperties
	lastCompleteSnapshot *snapshot.Manifest
	blockReader          volume.BlockReader
	epoch                time.Time // all changes stamped with this time
	rootDir              *dirMeta
}

// New returns a new volume filesystem
func New(args FilesystemArgs) (*Filesystem, error) {
	if err := args.Validate(); err != nil {
		return nil, err
	}
	f := &Filesystem{}
	f.FilesystemArgs = args
	f.setDefaultLayoutProperties() // block size may be reset from previous repo snapshot
	return f, nil
}
