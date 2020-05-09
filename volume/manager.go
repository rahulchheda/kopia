// Package volume enables snapshots of volumes that support change-block-tracking.
package volume

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/kopia/kopia/repo/logging"
)

var log = logging.GetContextLoggerFunc("volume")

// Package errors.
var (
	ErrInvalidArgs  = fmt.Errorf("invalid arguments")
	ErrNotSupported = fmt.Errorf("not supported")
)

// Manager offers methods to operate on volumes of some type.
type Manager interface {
	// Type returns the volume type.
	Type() string

	// GetBlockReader returns a BlockReader for a particular volume.
	// Optional - will return ErrNotSupported if unavailable.
	GetBlockReader(args GetBlockReaderArgs) (BlockReader, error)

	// GetBlockWriter returns a BlockWriter for a particular volume.
	// Optional - will return ErrNotSupported if unavailable.
	GetBlockWriter(args GetBlockWriterArgs) (BlockWriter, error)
}

// GetBlockReaderArgs contains the arguments for GetBlockReader.
type GetBlockReaderArgs struct {
	// The ID of the volume concerned.
	VolumeID string
	// The ID of the volume snapshot being backed up.
	SnapshotID string
	// The previous snapshot ID for an incremental backup, if set.
	// If not set a complete backup is to be performed.
	PreviousSnapshotID string
	// The snapshot block size expected by the filesystem.
	BlockSizeBytes int64
	// Manager specific profile containing location and credential information.
	Profile interface{}
}

// Validate checks for required fields.
func (a GetBlockReaderArgs) Validate() error {
	if a.VolumeID == "" || a.SnapshotID == "" || a.BlockSizeBytes == 0 || a.SnapshotID == a.PreviousSnapshotID {
		return ErrInvalidArgs
	}

	return nil
}

// BlockReader is the interface used to get block related data and metadata from a volume manager.
type BlockReader interface {
	// GetBlockAddresses returns block addresses (in the snapshot address space) that need to be backed up.
	// It is assumed that the entire volume block map will fit into memory if necessary.
	GetBlockAddresses(ctx context.Context) ([]int64, error)
	// GetBlock returns a reader for the data in the specified snapshot block.
	GetBlock(ctx context.Context, blockAddr int64) (io.ReadCloser, error)
}

// GetBlockWriterArgs contains the arguments for GetBlockWriter.
type GetBlockWriterArgs struct {
	// The ID of the volume concerned.
	VolumeID string
	// The snapshot block size used by the filesystem.
	BlockSizeBytes int64
	// Manager specific profile containing location and credential information.
	Profile interface{}
}

// Validate checks for required fields.
func (a GetBlockWriterArgs) Validate() error {
	if a.VolumeID == "" || a.BlockSizeBytes < 0 {
		return ErrInvalidArgs
	}

	return nil
}

// BlockWriter is the interface used to write snapshot blocks to a volume manager.
type BlockWriter interface {
	// AllocateBuffer returns a buffer of volume specific size and alignment
	// to be used in the writer returned by PutBlock.
	AllocateBuffer() []byte
	// PutBlock returns a writer for the specified snapshot block.
	PutBlock(ctx context.Context, blockAddr int64) (io.WriteCloser, error)
}

// managerRegistry contains registered managers.
// Typically this is accomplished as the side-effect of importing their individual packages.
var managerRegistry = map[string]Manager{}

var managerRegistryMutex sync.Mutex

// RegisterManager registers a manager type.
func RegisterManager(volumeType string, p Manager) {
	managerRegistryMutex.Lock()
	defer managerRegistryMutex.Unlock()

	managerRegistry[volumeType] = p

	log(context.Background()).Debugf("registered volume type [%s]", volumeType)
}

// FindManager returns the manager for a manager type or nil.
func FindManager(volumeType string) Manager {
	managerRegistryMutex.Lock()
	defer managerRegistryMutex.Unlock()

	log(context.Background()).Debugf("lookup volume type [%s]", volumeType)

	return managerRegistry[volumeType]
}
