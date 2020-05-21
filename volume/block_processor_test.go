package volume

import (
	"context"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic,goconst,gocyclo
func TestBlockProcessor(t *testing.T) {
	assert := assert.New(t)

	for _, tc := range []string{
		"invalid1", "invalid2", "invalid3",
		"no blocks", "some blocks default workers",
		"iter error", "worker error",
		"worker error while dispatch blocked in next",
		"aborted when dispatch blocked in queue",
		"error before calling next",
		"drain queued buffers",
	} {
		t.Logf("Case: %s", tc)

		ctx := context.Background()
		tbi := &testBlockIterator{}
		tbi.retErrOrEOF = true
		numWorkers := 1
		expError := ""
		expNumBlocks := int64(0)
		expMinAddr := int64((^uint64(0) >> 1))
		expMaxAddr := int64(-1)
		expNextCalls := 0
		expCloseCalls := 0
		expWorkerCalled := int32(0)
		blocks := []*testBlock{}
		expRelCnt := 0

		bp := BlockProcessor{
			Iter: tbi,
		}

		var workerCalled int32
		var workerErr error
		var workerBlockChan chan struct{}

		bp.Worker = func(ctx context.Context, b Block) error {
			atomic.AddInt32(&workerCalled, 1)
			if workerBlockChan != nil {
				<-workerBlockChan
			}

			return workerErr
		}

		switch tc {
		case "invalid1":
			bp.Iter = nil
			expError = "invalid arguments"
		case "invalid2":
			numWorkers = -1
			expError = "invalid arguments"
		case "invalid3":
			bp.Worker = nil
			expError = "invalid arguments"
		case "no blocks":
			expNextCalls, expCloseCalls, expWorkerCalled = 1, 1, 0
		case "some blocks default workers":
			numWorkers = 0 // use default
			blocks = append(blocks, &testBlock{addr: 10}, &testBlock{addr: 100}, &testBlock{addr: 0}, &testBlock{addr: 99}, &testBlock{addr: 2})
			tbi.retNextB = blocks
			expRelCnt = len(blocks)
			expNextCalls, expCloseCalls, expWorkerCalled = len(blocks)+1, 1, int32(len(blocks))
			expNumBlocks, expMinAddr, expMaxAddr = int64(len(blocks)), 0, 100
		case "iter error":
			numWorkers = 4
			blocks = append(blocks, &testBlock{addr: 10}, &testBlock{addr: 100}, &testBlock{addr: 0}, &testBlock{addr: 99}, &testBlock{addr: 2})
			tbi.retNextB = blocks
			tbi.nextFailOn = 3
			tbi.retCloseE = ErrNotSupported
			tbi.closeBlockChan = make(chan struct{})
			expError = tbi.retCloseE.Error()
			expRelCnt = tbi.nextFailOn - 1
			expNextCalls, expCloseCalls, expWorkerCalled = tbi.nextFailOn, 1, int32(tbi.nextFailOn)-1
			expNumBlocks, expMinAddr, expMaxAddr = int64(len(blocks)), 10, 100
		case "worker error":
			workerErr = ErrNotSupported
			expError = workerErr.Error()
			blocks = append(blocks, &testBlock{addr: 10})
			tbi.retNextB = blocks
			expRelCnt = len(blocks)
			expNextCalls, expCloseCalls, expWorkerCalled = 2, 1, 1
			expNumBlocks, expMinAddr, expMaxAddr = int64(len(blocks)), 10, 10
		case "worker error while dispatch blocked in next":
			tbi.nextBlockChan = make(chan struct{})
			tbi.nextBlockOn = 1
			blocks = append(blocks, &testBlock{addr: 10})
			tbi.retNextB = blocks
			expRelCnt = len(blocks)
			expNextCalls, expCloseCalls, expWorkerCalled = 1, 1, 0
			expNumBlocks, expMinAddr, expMaxAddr = 1, 10, 10
		case "aborted when dispatch blocked in queue":
			workerBlockChan = make(chan struct{})
			tbi.nextBlockChan = make(chan struct{})
			tbi.nextBlockOn = 2 // let one block get dispatched
			tbi.nextBlockChan = make(chan struct{})
			tbi.closeBlockChan = make(chan struct{})
			blocks = append(blocks, &testBlock{addr: 10}, &testBlock{addr: 1})
			tbi.retNextB = blocks
			expRelCnt = len(blocks)
			expNextCalls, expCloseCalls, expWorkerCalled = len(blocks), 1, 1
			expNumBlocks, expMinAddr, expMaxAddr = 1, 1, 10
		case "error before calling next":
			// Note: this cannot be tested without the existence of AtEnd!
			tbi.errOrEOFBlockChan = make(chan struct{})
			tbi.nextForceReturnNil = true
			tbi.retErrOrEOF = false
			expNextCalls, expCloseCalls, expWorkerCalled = 1, 1, 0
			expNumBlocks, expMinAddr, expMaxAddr = int64(len(blocks)-1), 1, 10
		case "drain queued buffers":
			blocks = append(blocks, &testBlock{addr: 10}, &testBlock{addr: 100}, &testBlock{addr: 0})
			tbi.retNextB = blocks
			numWorkers = len(blocks) - 1
			workerBlockChan = make(chan struct{})
			workerErr = ErrNotSupported
			expError = workerErr.Error()
			expRelCnt = len(blocks)
			expNextCalls, expCloseCalls, expWorkerCalled = len(blocks)+1, 1, int32(numWorkers)
			expNumBlocks, expMinAddr, expMaxAddr = int64(len(blocks)), 10, 100
		}

		bp.NumWorkers = numWorkers

		var err error
		wg := sync.WaitGroup{}
		wg.Add(1)

		go func() {
			err = bp.Run(ctx)
			wg.Done()
		}()

		// make racy situations deterministic
		switch tc {
		case "iter error":
			for atomic.AddInt32(&workerCalled, 0) != expWorkerCalled { // remove race
				time.Sleep(5 * time.Millisecond) // in UT worker count
			}
			assert.True(len(bp.blockChan) == 0)
			close(tbi.closeBlockChan) // resume dispatch
		case "worker error while dispatch blocked in next":
			for !tbi.nextIsBlocked {
				time.Sleep(5 * time.Millisecond)
			}
			bp.setError(ErrInvalidArgs)
			expError = ErrInvalidArgs.Error()
			close(tbi.nextBlockChan)
		case "aborted when dispatch blocked in queue":
			assert.True(len(blocks) > 1) // need at least 2
			assert.True(numWorkers == 1) // need exactly 1
			for !tbi.nextIsBlocked {
				time.Sleep(5 * time.Millisecond)
			}
			for atomic.AddInt32(&workerCalled, 0) == 0 {
				time.Sleep(5 * time.Millisecond)
			}
			// both worker and dispatch are now blocked under UT control
			close(bp.stopChan)           // close stop channel
			assert.NoError(bp.err)       // direct dispatch path to select block
			bp.blockChan <- &testBlock{} // insert a block to ensure correct select choice
			close(tbi.nextBlockChan)     // resume dispatch
			for tbi.closeCalls == 0 {    // wait until it blocks in close
				time.Sleep(5 * time.Millisecond)
			}
			assert.False(blocks[0].released)
			bp.err = ErrInvalidArgs   // do not close stopChan again
			<-bp.blockChan            // discard away the inserted block
			close(tbi.closeBlockChan) // resume dispatch
			close(workerBlockChan)    // resume workers
			expError = ErrInvalidArgs.Error()
		case "error before calling next":
			for !tbi.errOrEOFIsBlocked {
				time.Sleep(5 * time.Millisecond)
			}
			bp.setError(ErrInvalidArgs)
			expError = ErrInvalidArgs.Error()
			close(tbi.errOrEOFBlockChan)
		case "drain queued buffers":
			for atomic.AddInt32(&workerCalled, 0) != int32(numWorkers) {
				time.Sleep(5 * time.Millisecond)
			}
			for tbi.nextCalls < expNextCalls {
				time.Sleep(5 * time.Millisecond)
			}
			close(workerBlockChan)
		}

		wg.Wait()

		if expError == "" {
			assert.NoError(err)
			assert.NotNil(bp.blockChan)
			assert.NotNil(bp.stopChan)
			assert.Equal(expNumBlocks, bp.NumBlocks)
			assert.Equal(expMinAddr, bp.MinAddress)
			assert.Equal(expMaxAddr, bp.MaxAddress)
		} else {
			assert.Error(err, "returned error")
			assert.Regexp(expError, err, "returned error")
			if bp.err != nil {
				e := bp.err
				bp.setError(fmt.Errorf("new error"))
				assert.Equal(e, bp.err) // unchanged
			}
		}

		assert.Equal(expNextCalls, tbi.nextCalls, "nextCalls")
		assert.Equal(expCloseCalls, tbi.closeCalls, "closeCalls")
		assert.Equal(expWorkerCalled, atomic.AddInt32(&workerCalled, 0), "work called")
		relCnt := 0
		for _, tb := range blocks {
			if tb.released {
				relCnt++
			}
		}
		assert.Equal(expRelCnt, relCnt, "release count")
	}
}

type testBlockIterator struct {
	closeBlockChan     chan struct{}
	closeCalls         int
	errOrEOFBlockChan  chan struct{}
	errOrEOFIsBlocked  bool
	nextBlockChan      chan struct{}
	nextBlockOn        int // ordinal
	nextCalls          int
	nextFailOn         int // ordinal
	nextForceReturnNil bool
	nextIsBlocked      bool
	retCloseE          error
	retErrOrEOF        bool
	retNextB           []*testBlock
}

// nolint:wsl
func (tbi *testBlockIterator) Next(ctx context.Context) Block {
	tbi.nextCalls++
	if tbi.nextBlockChan != nil && (tbi.nextCalls >= tbi.nextBlockOn) {
		tbi.nextIsBlocked = true
		<-tbi.nextBlockChan
	}

	if tbi.nextForceReturnNil || len(tbi.retNextB) == 0 || (tbi.nextFailOn > 0 && tbi.nextCalls >= tbi.nextFailOn) {
		return nil
	}

	var b Block
	b, tbi.retNextB = tbi.retNextB[0], tbi.retNextB[1:]

	return b
}

func (tbi *testBlockIterator) AtEnd() bool {
	if tbi.errOrEOFBlockChan != nil {
		tbi.errOrEOFIsBlocked = true
		<-tbi.errOrEOFBlockChan
	}

	return tbi.retErrOrEOF
}

func (tbi *testBlockIterator) Close() error {
	tbi.closeCalls++
	if tbi.closeBlockChan != nil {
		<-tbi.closeBlockChan
	}

	return tbi.retCloseE
}

type testBlock struct {
	addr     int64
	released bool
}

func (tb *testBlock) Address() int64 {
	return tb.addr
}

func (tb *testBlock) Size() int {
	return 0
}

func (tb *testBlock) Get(ctx context.Context) (io.ReadCloser, error) {
	return nil, nil
}

func (tb *testBlock) Release() {
	tb.released = true
}
