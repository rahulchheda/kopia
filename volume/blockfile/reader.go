package blockfile

import (
	"context"
	"io"

	"github.com/kopia/kopia/volume"

	"github.com/ncw/directio"
)

var _ = volume.BlockReader(&manager{})

// GetBlockReader provides support for an "initial" snapshot of a file, for testing
// and verification purposes only. It cannot support change block tracking.
// The implementation is not very efficient.
func (m *manager) GetBlockReader(args volume.GetBlockReaderArgs) (volume.BlockReader, error) {
	if err := args.Validate(); err != nil {
		return nil, err
	}

	if args.PreviousSnapshotID != "" {
		return nil, volume.ErrNotSupported
	}

	if err := m.applyProfileFromArgs(modeRead, args.Profile, args.BlockSizeBytes); err != nil {
		return nil, err
	}

	if m.ioChan == nil {
		m.ioChan = make(chan struct{}, 1)
	}

	return m, nil
}

// GetBlockAddresses returns block addresses for non-zero snapshot sized blocks.
func (m *manager) GetBlockAddresses(ctx context.Context) ([]int64, error) {
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
	maxBlocks := fSz/m.fsBlockSizeBytes + (fSz+m.fsBlockSizeBytes-1)%m.fsBlockSizeBytes

	bmap := make([]int64, 0, maxBlocks)

	addBlock := func(offset int64) {
		ba := offset / m.fsBlockSizeBytes
		m.logger.Debugf("block 0x%014x non-zero", ba)
		bmap = append(bmap, ba)
	}

	// need to read the file to find out zero blocks
	buf := directio.AlignedBlock(int(m.fsBlockSizeBytes))
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

// GetBlock returns a reader for the data in the specified snapshot block.
func (m *manager) GetBlock(ctx context.Context, blockAddr int64) (io.ReadCloser, error) {
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
			currentOffset:  blockAddr * m.fsBlockSizeBytes,
			remainingBytes: int(m.fsBlockSizeBytes),
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
