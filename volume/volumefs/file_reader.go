package volumefs

import (
	"context"
	"fmt"
	"io"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/logging"
)

// fileReader implements fs.Reader
type fileReader struct {
	fe        *fileEntry // Entry needs to return the value
	atEOF     bool
	cancel    context.CancelFunc
	rc        io.ReadCloser
	logger    logging.Logger
	bytesRead int
}

var _ fs.Reader = (*fileReader)(nil)

func newFileReader(ctx context.Context, fe *fileEntry) (fs.Reader, error) {
	if fe.f.blockReader == nil {
		return nil, fmt.Errorf("not initialized for reading")
	}
	r := &fileReader{fe: fe}
	r.logger = log(ctx)
	_, r.cancel = context.WithCancel(ctx)
	if fe.m.isMeta(fe.f) {
		r.atEOF = true
	} else {
		blockAddr := fe.m.blockAddr()
		log(ctx).Debugf("GetBlock(%0*x)", fe.f.fileHexFmtLen, blockAddr)
		rc, err := fe.f.blockReader.GetBlock(ctx, blockAddr)
		if err != nil {
			log(ctx).Debugf("GetBlock(%0*x): %s", fe.f.fileHexFmtLen, blockAddr, err.Error())
			return nil, err
		}
		r.rc = rc
	}
	return r, nil
}

func (r *fileReader) Entry() (fs.Entry, error) {
	r.logger.Debugf("Entry(%s)", r.fe.m.name)
	return r.fe, nil
}

func (r *fileReader) Seek(offset int64, whence int) (int64, error) {
	r.logger.Errorf("Seek(%s)", r.fe.m.name)
	return 0, fmt.Errorf("unsupported")
}

func (r *fileReader) Close() error {
	r.cancel()
	var err error
	if r.rc != nil {
		err = r.rc.Close()
		r.rc = nil
	}
	r.logger.Debugf("Close(%s): %d %v", r.fe.m.name, r.bytesRead, err)
	return err
}

func (r *fileReader) Read(p []byte) (n int, err error) {
	if r.atEOF {
		return 0, io.EOF
	}
	n, err = r.rc.Read(p)
	if err == io.EOF {
		r.atEOF = true
	}
	r.bytesRead += n
	return
}
