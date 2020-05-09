package blockfile

import "context"

type rwCommon struct {
	m              *manager
	currentOffset  int64
	remainingBytes int
	cancel         context.CancelFunc
	ctx            context.Context
	isClosed       bool
	numCalls       int // for UT
}

// Close closes the file on the last close
func (rw *rwCommon) Close() (err error) {
	if rw.isClosed {
		return
	}

	rw.cancel()

	rw.m.mux.Lock()
	defer rw.m.mux.Unlock()

	rw.m.count--
	if rw.m.count == 0 {
		err = rw.m.file.Close()
		rw.m.logger.Debugf("closed file: %v", err)
	}

	rw.isClosed = true

	return
}

// do is intended to be called from the IO handler
func (rw *rwCommon) do(action func()) error {
	// synchronize or handle cancellation
	rw.numCalls++
	select {
	case rw.m.ioChan <- struct{}{}:
		break
	case <-rw.ctx.Done():
		return ErrCanceled
	}

	// action
	action()

	// release the semaphore
	<-rw.m.ioChan

	return nil
}
