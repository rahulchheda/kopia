// Package blockfile provides a writer to restore data to a block special file, or an ordinary file.
package blockfile

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/volume"
)

// Package constants.
const (
	VolumeType            = "blockfile"
	DefaultBlockSizeBytes = int64(16 * 1024) // nolint:gomnd // must be a power of 2

	modeRead  = "readMode"
	modeWrite = "writeMode"
)

var log = logging.GetContextLoggerFunc("volume/blockfile")

// Errors
var (
	ErrAlreadyInitialized = fmt.Errorf("already initialized")
	ErrCanceled           = fmt.Errorf("canceled")
	ErrCapacityExceeded   = fmt.Errorf("block capacity has been exceeded")
	ErrInvalidArgs        = fmt.Errorf("invalid blockwriter arguments")
	ErrInvalidSize        = fmt.Errorf("invalid size")
	ErrProfileMissing     = fmt.Errorf("profile missing")
)

// init registers the factory interface on import.
func init() {
	volume.RegisterManager(VolumeType, &blockfileFactory{})
}

// blockfileFactory is the external factory type
type blockfileFactory struct{}

var _ volume.Manager = (*blockfileFactory)(nil)

// Type returns the volume type
func (factory *blockfileFactory) Type() string {
	return VolumeType
}

// Profile contains directives for an instance of the manager.
type Profile struct {
	// Path name of the device special file or ordinary file.
	Name string
	// Create the file if it does not exist when writing.
	CreateIfMissing bool
	// The device block size. If unset a DefaultBlockSizeBytes will be used. Must be a power of 2.
	// Data written must be done in chunks of a multiple of this size.
	DeviceBlockSizeBytes int64
}

// Validate checks the Profile values for correctness.
func (p *Profile) Validate() error {
	// 0 is ok; see https://stackoverflow.com/questions/600293/how-to-check-if-a-number-is-a-power-of-2
	if p.Name == "" || p.DeviceBlockSizeBytes < 0 || ((p.DeviceBlockSizeBytes & (p.DeviceBlockSizeBytes - 1)) != 0) {
		return ErrInvalidArgs
	}

	return nil
}

type devFiler interface {
	Close() error
	ReadAt([]byte, int64) (int, error)
	Stat() (os.FileInfo, error)
	WriteAt([]byte, int64) (int, error)
}

// manager is the internal type that contains instance data
type manager struct {
	Profile

	logger logging.Logger

	fsBlockSizeBytes int64 // The snapshot block size used by the filesystem.
	mux              sync.Mutex
	file             devFiler // The file is shared by all writers.
	mode             string

	count  int           // The active count - on last close the file must be closed.
	ioChan chan struct{} // A counting semaphore to serialize concurrent write or read calls.
}

func (m *manager) applyProfileFromArgs(mode string, argsProfile interface{}, argsBlockSize int64) error {
	p, ok := argsProfile.(*Profile)
	if !ok {
		return ErrProfileMissing
	}

	if err := p.Validate(); err != nil {
		return err
	}

	bSz := DefaultBlockSizeBytes
	if p.DeviceBlockSizeBytes > 0 {
		bSz = p.DeviceBlockSizeBytes
	}

	m.Profile = *p
	m.Name = filepath.Clean(p.Name)
	m.DeviceBlockSizeBytes = bSz
	m.fsBlockSizeBytes = argsBlockSize
	m.mode = mode

	return nil
}

type devFileOpener func(string, int, os.FileMode) (devFiler, error)

var openFile devFileOpener = osOpenFile

func osOpenFile(name string, flags int, perms os.FileMode) (devFiler, error) {
	// Note: directio OpenFile does not work on files in linux docker containers
	return os.OpenFile(name, flags, perms)
}

func (m *manager) openFile(mustLock, forReading bool) (devFiler, error) {
	if mustLock {
		m.mux.Lock()
		defer m.mux.Unlock()
	}

	flags := os.O_WRONLY
	if forReading {
		flags = os.O_RDONLY
	} else if m.CreateIfMissing {
		flags |= os.O_CREATE
	}

	m.logger.Debugf("opening file %s %x", m.Name, flags)

	f, err := openFile(m.Name, flags, 0600) // nolint:gocritic
	if err != nil {
		m.logger.Errorf("open(%s): %v", m.Name, err)
		return nil, err
	}

	return f, nil
}
