package volumefs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestBlock(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	tRepo := &testRepo{}

	f := th.fs()
	assert.NotNil(f.blockPool.New)
	assert.Equal(f.Repo, f.repo)
	f.repo = tRepo
	f.logger = log(ctx)

	b := f.getBlock()
	assert.NotNil(b)
	assert.Equal(f, b.f)

	f.blockSzB = 1024
	b.BlockAddr = 1000
	b.Oid = "oid"

	assert.Equal(int64(1000), b.Address())
	assert.Equal(f.blockSzB, b.Size())

	expError := ErrInvalidArgs
	tRepo.retOoE = expError
	rc, err := b.Get(ctx)
	assert.Equal(expError, err)
	assert.Nil(rc)

	tRepo.retOoE = nil
	tRepo.retOoR = &testReader{}
	rc, err = b.Get(ctx)
	assert.Nil(err)
	assert.Equal(tRepo.retOoR, rc)

	b.Release()
}
