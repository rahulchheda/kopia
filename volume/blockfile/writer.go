package blockfile

import (
	"context"
	"io"

	"github.com/kopia/kopia/volume"

	"github.com/ncw/directio"
)

// GetBlockWriter returns a volume.BlockWriter.
func (factory *blockfileFactory) GetBlockWriter(args volume.GetBlockWriterArgs) (volume.BlockWriter, error) {
	if err := args.Validate(); err != nil {
		return nil, err
	}

	m := &manager{}

	if err := m.applyProfileFromArgs(modeWrite, args.Profile, args.BlockSizeBytes); err != nil {
		return nil, err
	}

	if m.ioChan == nil {
		m.ioChan = make(chan struct{}, 1)
	}

	return m, nil
}

var _ volume.BlockWriter = (*manager)(nil)

func (m *manager) AllocateBuffer() []byte {
	// AlignedBlock panics in degenerate cases
	return directio.AlignedBlock(int(m.DeviceBlockSizeBytes))
}

// PutBlock returns an io.WriteCloser to write a snapshot block to the file.
func (m *manager) PutBlock(ctx context.Context, blockAddr int64) (io.WriteCloser, error) {
	m.mux.Lock()
	defer m.mux.Unlock()

	if m.count == 0 {
		m.logger = log(ctx)

		f, err := m.openFile(false, false)
		if err != nil {
			return nil, err
		}

		m.file = f
	}

	m.count++

	bw := &Writer{
		rwCommon: rwCommon{
			m:              m,
			currentOffset:  blockAddr * m.fsBlockSizeBytes,
			remainingBytes: int(m.fsBlockSizeBytes),
		},
	}
	bw.ctx, bw.cancel = context.WithCancel(ctx)

	return bw, nil
}

// Writer implements io.WriteCloser.
type Writer struct {
	rwCommon
}

var _ = io.WriteCloser(&Writer{})

// Write implements io.Writer.
// It synchronizes with concurrent writers.
// It expects that the buffer was obtained from AllocateBuffer and
// so is properly sized and aligned.
func (w *Writer) Write(data []byte) (n int, err error) {
	if len(data) != int(w.m.DeviceBlockSizeBytes) {
		return 0, ErrInvalidSize
	}

	if len(data) > w.remainingBytes {
		return 0, ErrCapacityExceeded
	}

	doErr := w.do(func() {
		n, err = w.doBody(data)
	})

	if doErr != nil {
		err = doErr
	}

	return
}

func (w *Writer) doBody(data []byte) (n int, err error) {
	n, err = w.m.file.WriteAt(data, w.currentOffset)
	if err == nil {
		w.currentOffset += int64(n)
		w.remainingBytes -= n
	}

	return
}
