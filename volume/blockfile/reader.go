package blockfile

import (
	"context"
	"io"
	"sync"

	"github.com/kopia/kopia/volume"

	"github.com/ncw/directio"
)

// GetBlockReader provides support for an "initial" snapshot of a file, for testing
// and verification purposes only. It cannot support change block tracking.
// The implementation is not very efficient.
func (factory *blockfileFactory) GetBlockReader(args volume.GetBlockReaderArgs) (volume.BlockReader, error) {
	if err := args.Validate(); err != nil {
		return nil, err
	}

	m := &manager{}

	if args.PreviousSnapshotID != "" {
		return nil, volume.ErrNotSupported
	}

	if err := m.applyProfileFromArgs(modeRead, args.Profile); err != nil {
		return nil, err
	}

	if m.ioChan == nil {
		m.ioChan = make(chan struct{}, 1)
	}

	return m, nil
}

var _ volume.BlockReader = (*manager)(nil)

// GetBlocks returns an iterator for the non-zero volume blocks.
func (m *manager) GetBlocks(ctx context.Context) (volume.BlockIterator, error) {
	ba, err := m.getBlockAddresses(ctx)
	if err != nil {
		return nil, err
	}

	return m.newBlockIterator(ba), nil
}

// getBlockAddresses returns block addresses for non-zero snapshot sized blocks.
// @TODO fold into GetBlocks() or make internal.
func (m *manager) getBlockAddresses(ctx context.Context) ([]int64, error) {
	m.logger = log(ctx)

	f, err := m.openFile(false, true)
	if err != nil {
		return nil, err
	}

	defer f.Close() // nolint:errcheck

	m.file = f

	fi, err := m.file.Stat()
	if err != nil {
		return nil, err
	}

	fSz := fi.Size()
	maxBlocks := fSz/m.DeviceBlockSizeBytes + (fSz+m.DeviceBlockSizeBytes-1)%m.DeviceBlockSizeBytes

	bmap := make([]int64, 0, maxBlocks)

	addBlock := func(offset int64) {
		ba := offset / m.DeviceBlockSizeBytes
		m.logger.Debugf("block 0x%014x non-zero", ba)
		bmap = append(bmap, ba)
	}

	// need to read the file to find out zero blocks
	buf := directio.AlignedBlock(int(m.DeviceBlockSizeBytes))
	pos := int64(0)

	for i := int64(0); i < maxBlocks; i++ {
		n, err := m.file.ReadAt(buf, pos)
		if err != nil {
			if err == io.EOF {
				if n > 0 {
					addBlock(pos)
				}

				break
			}

			return nil, err
		}

		if !m.isZeroBlock(buf, n) {
			addBlock(pos)
		}

		pos += int64(n)
	}

	return bmap, nil
}

func (m *manager) isZeroBlock(buf []byte, n int) bool {
	for i := 0; i < n; i++ {
		if buf[i] != 0 {
			return false
		}
	}

	return true
}

type blockIterator struct {
	m           *manager
	iterBlocks  []int64
	iterCurrent int
	iterError   error
	blockPool   sync.Pool
}

func (m *manager) newBlockIterator(blocks []int64) *blockIterator {
	bi := &blockIterator{m: m, iterBlocks: blocks}
	bi.blockPool.New = func() interface{} {
		b := new(block)
		b.addr = -1

		return b
	}

	return bi
}

// Next is from volume.BlockIterator
func (bi *blockIterator) Next(ctx context.Context) volume.Block {
	bi.m.mux.Lock()
	defer bi.m.mux.Unlock()

	if bi.iterError != nil {
		return nil
	}

	if bi.iterCurrent < len(bi.iterBlocks) {
		b := bi.blockPool.Get().(*block)
		b.bi = bi // will be unset if reused
		b.addr = bi.iterBlocks[bi.iterCurrent]
		bi.iterCurrent++

		return b
	}

	bi.iterError = io.EOF

	return nil
}

func (bi *blockIterator) AtEnd() bool {
	return bi.iterError != nil
}

// Close is from volume.BlockIterator
func (bi *blockIterator) Close() error {
	var err error

	bi.m.mux.Lock()
	defer bi.m.mux.Unlock()

	if bi.iterError != nil && bi.iterError != io.EOF {
		err = bi.iterError
	} else {
		bi.iterError = io.EOF // don't use the iterator again
	}

	return err
}

// block satisfies volume.Block
type block struct {
	bi   *blockIterator
	addr int64
}

func (b *block) Address() int64 {
	return b.addr
}

// Size is from volume.Block
func (b *block) Size() int {
	return int(b.bi.m.DeviceBlockSizeBytes)
}

func (b *block) Release() {
	bi := b.bi
	b.bi = nil // force failure if used
	bi.blockPool.Put(b)
}

func (b *block) Get(ctx context.Context) (io.ReadCloser, error) {
	return b.bi.m.getBlock(ctx, b.addr)
}

// getBlock returns a reader for the data in the specified snapshot block.
func (m *manager) getBlock(ctx context.Context, blockAddr int64) (io.ReadCloser, error) {
	m.mux.Lock()
	defer m.mux.Unlock()

	if m.count == 0 {
		m.logger = log(ctx)

		f, err := m.openFile(false, true)
		if err != nil {
			return nil, err
		}

		m.file = f
	}

	m.count++

	br := &Reader{
		rwCommon: rwCommon{
			m:              m,
			currentOffset:  blockAddr * m.DeviceBlockSizeBytes,
			remainingBytes: int(m.DeviceBlockSizeBytes),
		},
	}
	br.ctx, br.cancel = context.WithCancel(ctx)
	br.buf = directio.AlignedBlock(int(m.DeviceBlockSizeBytes))

	return br, nil
}

// Reader implements io.ReadCloser
type Reader struct {
	rwCommon
	buf []byte
}

var _ = io.ReadCloser(&Reader{})

// Read implements io.Reader.
// It synchronizes with concurrent readers.
func (r *Reader) Read(retBuf []byte) (n int, err error) {
	if r.remainingBytes == 0 {
		return 0, io.EOF
	}

	if len(retBuf) < len(r.buf) {
		return 0, ErrInvalidSize
	}

	doErr := r.do(func() {
		n, err = r.doBody(retBuf)
	})
	if doErr != nil {
		err = doErr
	}

	return
}

func (r *Reader) doBody(retBuf []byte) (n int, err error) {
	n, err = r.m.file.ReadAt(r.buf, r.currentOffset)
	if err == nil {
		r.currentOffset += int64(n)
		r.remainingBytes -= n
	} else if err == io.EOF {
		r.currentOffset += int64(n) // could have read some data
		r.remainingBytes = 0        // but nothing left
		err = nil
	}

	if err == nil {
		if n == len(r.buf) {
			copy(retBuf, r.buf)
		} else {
			for i := 0; i < n; i++ {
				retBuf[i] = r.buf[i]
			}
		}
	}

	return
}
