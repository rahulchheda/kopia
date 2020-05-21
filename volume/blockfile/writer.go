package blockfile

import (
	"context"
	"io"

	"github.com/kopia/kopia/volume"
)

// GetBlockWriter returns a volume.BlockWriter.
func (factory *blockfileFactory) GetBlockWriter(args volume.GetBlockWriterArgs) (volume.BlockWriter, error) {
	if err := args.Validate(); err != nil {
		return nil, err
	}

	m := &manager{}

	if err := m.applyProfileFromArgs(modeWrite, args.Profile); err != nil {
		return nil, err
	}

	if m.ioChan == nil {
		m.ioChan = make(chan struct{}, 1)
	}

	return m, nil
}

var _ volume.BlockWriter = (*manager)(nil)

// PutBlocks reads blocks from the iterator to the file with the configured degree of concurrency.
func (m *manager) PutBlocks(ctx context.Context, bi volume.BlockIterator) error {
	if err := m.writerInit(ctx, bi); err != nil {
		return err
	}

	err := m.bp.Run(ctx) // will close the iter

	if closeErr := m.file.Close(); closeErr != nil {
		m.logger.Debugf("file [%s] Close(): %v", m.Name, closeErr)

		if err == nil {
			err = closeErr
		}
	}

	m.file = nil

	return err
}

// writerInit initializes the manager for writing.
func (m *manager) writerInit(ctx context.Context, bi volume.BlockIterator) error {
	m.mux.Lock()
	defer m.mux.Unlock()

	if m.file != nil {
		return ErrInUse
	}

	m.logger = log(ctx)

	f, err := m.openFile(false, false)
	if err != nil {
		return err
	}

	m.file = f
	m.bp = volume.BlockProcessor{
		Iter:       bi,
		Worker:     m.blockWriter,
		NumWorkers: m.Concurrency,
	}
	m.bufPool.New = func() interface{} {
		buf := make([]byte, m.DeviceBlockSizeBytes)

		return &buf
	}

	m.logger.Debugf("bSz:%d #w:%d", m.DeviceBlockSizeBytes, m.Concurrency)

	return nil
}

func (m *manager) getBuffer() *[]byte {
	return m.bufPool.Get().(*[]byte)
}

func (m *manager) releaseBuffer(bufPtr *[]byte) {
	m.bufPool.Put(bufPtr)
}

// blockWriter is invoked by the block processor to write a block.
// Potentially multiple calls can be made concurrently.
func (m *manager) blockWriter(ctx context.Context, block volume.Block) error {
	var (
		blockAddr = block.Address()
		blockSize = block.Size()
	)

	bufPtr := m.getBuffer()
	defer m.releaseBuffer(bufPtr)

	rc, err := block.Get(ctx)
	if err != nil {
		m.logger.Debugf("block[%x] get: %v", blockAddr, err)
		return err
	}

	// the helper synchronizes access to the file
	bwh := &blockWriterHelper{
		rwCommon: rwCommon{
			m:              m,
			currentOffset:  blockAddr * int64(blockSize),
			remainingBytes: blockSize,
		},
		blockAddr: blockAddr,
		rc:        rc,
		buf:       *bufPtr,
	}
	bwh.ctx, bwh.cancel = context.WithCancel(ctx)

	err = bwh.copyBlockToFile()

	if closeErr := rc.Close(); closeErr != nil {
		m.logger.Debugf("block[%x] close: %v", blockAddr, closeErr)

		if err == nil {
			err = closeErr // set return error
		}
	}

	return err
}

// blockWriterHelper is a helper for the blockWriterHelper method.
type blockWriterHelper struct {
	rwCommon
	blockAddr int64
	rc        io.ReadCloser
	buf       []byte
	err       error
}

// copyBlockToFile copies data from the block to the file until done
func (bwh *blockWriterHelper) copyBlockToFile() error {
	for bwh.err == nil {
		if err := bwh.do(bwh.doBody); err != nil && bwh.err == nil {
			bwh.err = err
		}
	}

	if bwh.err == io.EOF {
		bwh.err = nil
	}

	return bwh.err
}

// doBody reads a buffer from the block and writes it to the file.
// Simplifying assumption here: no short reads.
func (bwh *blockWriterHelper) doBody() {
	n, err := bwh.rc.Read(bwh.buf)

	if err != nil {
		bwh.m.logger.Debugf("block[%x] read: %v", bwh.blockAddr, err)
		bwh.err = err

		return
	}

	if n == 0 {
		return
	}

	if n > bwh.remainingBytes {
		bwh.err = ErrCapacityExceeded
		return
	}

	n, err = bwh.m.file.WriteAt(bwh.buf, bwh.currentOffset)
	if err != nil {
		bwh.m.logger.Debugf("block[%x] writeAt:(%d): %v", bwh.blockAddr, bwh.currentOffset, err)
		bwh.err = err

		return
	}

	bwh.currentOffset += int64(n)
	bwh.remainingBytes -= n
}
