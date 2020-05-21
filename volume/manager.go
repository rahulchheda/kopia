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

// BlockReader is the interface used to get block related data and metadata from a volume manager.
type BlockReader interface {
	// GetBlocks returns an iterator to consume blocks from the volume.
	GetBlocks(ctx context.Context) (BlockIterator, error)
}

// BlockWriter is the interface used to write snapshot blocks to a volume manager.
type BlockWriter interface {
	// PutBlocks sends blocks to the volume via an iterator.
	PutBlocks(ctx context.Context, bi BlockIterator) error
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
	// Manager specific profile containing location and credential information.
	Profile interface{}
}

// Validate checks for required fields.
func (a GetBlockReaderArgs) Validate() error {
	if a.VolumeID == "" || a.SnapshotID == "" || a.SnapshotID == a.PreviousSnapshotID {
		return ErrInvalidArgs
	}

	return nil
}

// GetBlockWriterArgs contains the arguments for GetBlockWriter.
type GetBlockWriterArgs struct {
	// The ID of the volume concerned.
	VolumeID string
	// Manager specific profile containing location and credential information.
	Profile interface{}
}

// Validate checks for required fields.
func (a GetBlockWriterArgs) Validate() error {
	if a.VolumeID == "" {
		return ErrInvalidArgs
	}

	return nil
}

// Block is an abstraction of a volume data block.
type Block interface {
	// Address returns the block address.
	Address() int64
	// Size returns the size of the block in bytes.
	Size() int
	// Get returns a reader to read the block data.
	Get(ctx context.Context) (io.ReadCloser, error)
	// Release should be called on completion to free the resources associated with the block.
	Release()
}

// BlockIterator enumerates blocks.
type BlockIterator interface {
	// Next returns the next block. It may return nil without implying exhaustion - AtEnd() must be checked.
	// The lifecycle of the returned Block is independent of that of the iterator.
	Next(ctx context.Context) Block
	// AtEnd returns true if the iterator has encountered an error or is exhausted.
	AtEnd() bool
	// Close terminates the iterator and returns any error.
	Close() error
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
