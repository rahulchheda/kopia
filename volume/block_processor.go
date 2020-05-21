package volume

import (
	"context"
	"sync"
)

// BlockProcessor related constants.
const (
	DefaultNumberBlockProcessorWorkers = 4
)

// BlockProcessor consumes Blocks provided by a BlockIterator and feeds them to one
// or more worker functions concurrently for processing.
// Processing stops on the first error encountered, including worker or iterator failure.
type BlockProcessor struct {
	// The iterator from which to read Block interfaces. It will be closed on completion.
	Iter BlockIterator
	// The worker function. It may return an error to terminate processing.
	// The block is released when the Worker function returns.
	Worker func(ctx context.Context, block Block) error
	// The number of workers. If 0 then DefaultNumberBlockProcessorWorkers is used.
	NumWorkers int

	// Counters set during Run.
	NumBlocks  int64
	MinAddress int64
	MaxAddress int64

	// internal fields
	ctx       context.Context
	cancel    context.CancelFunc
	blockChan chan Block
	stopChan  chan struct{}
	mux       sync.Mutex
	err       error
}

// Validate checks the initialization of the BlockProcessor.
func (bp *BlockProcessor) Validate() error {
	if bp.Iter == nil || bp.NumWorkers < 0 || bp.Worker == nil {
		return ErrInvalidArgs
	}

	return nil
}

// Run applies one or more Worker function concurrently to the Blocks returned by the iterator.
func (bp *BlockProcessor) Run(ctx context.Context) error {
	if err := bp.Validate(); err != nil {
		return err
	}

	if bp.NumWorkers == 0 {
		bp.NumWorkers = DefaultNumberBlockProcessorWorkers
	}

	bp.blockChan = make(chan Block, bp.NumWorkers)
	bp.stopChan = make(chan struct{})
	bp.ctx, bp.cancel = context.WithCancel(ctx)
	bp.NumBlocks = 0
	bp.MinAddress = int64((^uint64(0) >> 1))
	bp.MaxAddress = -1

	defer bp.terminate()

	wg := sync.WaitGroup{}

	for i := 0; i < bp.NumWorkers; i++ {
		wg.Add(1)

		go func() {
			bp.dequeueBlockAndCallWorker()
			wg.Done()
		}()
	}

	bp.dispatchBlocks()
	bp.stopWorkers()
	wg.Wait()
	bp.drain()

	return bp.err
}

// setError aborts the restore session on the first error posted
func (bp *BlockProcessor) setError(err error) {
	bp.mux.Lock()
	defer bp.mux.Unlock()

	if bp.err == nil {
		bp.err = err // first one wins
		bp.cancel()  // sent to the iterator and workers
		close(bp.stopChan)
	}
}

func (bp *BlockProcessor) terminate() {
	bp.mux.Lock()
	defer bp.mux.Unlock()

	bp.cancel()

	if bp.err == nil { // not aborted
		close(bp.stopChan)
	}
}

func (bp *BlockProcessor) stopWorkers() {
	close(bp.blockChan)
}

func (bp *BlockProcessor) drain() {
	for {
		b := <-bp.blockChan
		if b == nil {
			break
		}

		b.Release()
	}
}

func (bp *BlockProcessor) dequeueBlockAndCallWorker() {
	var (
		err error
		b   Block
	)

	for {
		if bp.err != nil { // check before blocking call
			return
		}

		select {
		case b = <-bp.blockChan:
			if b == nil {
				return // closed
			}
		case <-bp.stopChan:
			return // aborted
		}

		err = bp.Worker(bp.ctx, b)
		b.Release()

		if err != nil {
			bp.setError(err)
			return
		}
	}
}

func (bp *BlockProcessor) dispatchBlocks() {
	var (
		b    Block
		addr int64
	)

out:
	for {
		if bp.err != nil { // check before blocking call
			break out
		}

		b = bp.Iter.Next(bp.ctx)
		if b == nil {
			if bp.Iter.AtEnd() {
				break out
			}
			continue
		}

		bp.NumBlocks++
		addr = b.Address()

		if addr > bp.MaxAddress {
			bp.MaxAddress = addr
		}

		if addr < bp.MinAddress {
			bp.MinAddress = addr
		}

		if bp.err != nil { // check before blocking call
			b.Release()
			break out
		}

		select {
		case <-bp.stopChan:
			b.Release()
			break out
		case bp.blockChan <- b:
		}
	}

	if err := bp.Iter.Close(); err != nil {
		bp.setError(err)
	}
}
