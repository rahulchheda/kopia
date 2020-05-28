package volumefs

import (
	"testing"

	"github.com/kopia/kopia/repo/object"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestBlockMapIter(t *testing.T) {
	assert := assert.New(t)

	bmi := &blockMapIter{}
	bmi.init()
	assert.NotNil(bmi.stopChan)
	assert.NotNil(bmi.mChan)

	sendRC := true
	go func() {
		for i := 0; i < 10000; i++ {
			bam := BlockAddressMapping{
				BlockAddr: int64(i),
				Oid:       "not-nil",
			}
			sendRC = bmi.trySend(bam)

			if !sendRC {
				break
			}
		}

		close(bmi.mChan)
	}()

	bam := bmi.Next()
	assert.Equal(int64(0), bam.BlockAddr)

	bam = bmi.Next()
	assert.Equal(int64(1), bam.BlockAddr)

	bmi.Close()

	assert.False(sendRC)
}

// This test assumes that the bTree works.
// nolint:wsl,gocritic
func TestBTreeMap(t *testing.T) {
	assert := assert.New(t)

	bmm := newBTreeMap(8)

	maxAddr := 10000
	oidVal := object.ID("not-nil")

	for i := 0; i < maxAddr; i++ {
		bam := BlockAddressMapping{
			BlockAddr: int64(i),
			Oid:       oidVal,
		}
		bmm.InsertOrReplace(bam)
	}

	oid := bmm.Find(0)
	assert.Equal(oidVal, oid)

	oid = bmm.Find(int64(maxAddr + 1))
	assert.Equal(object.ID(""), oid)

	bmi := bmm.Iterator()
	assert.NotNil(bmi)

	for i := 0; i < maxAddr; i++ {
		bam := bmi.Next()
		assert.Equal(int64(i), bam.BlockAddr) // retrieval is ordered
		assert.Equal(oidVal, bam.Oid)
	}

	bam := bmi.Next()
	assert.Equal(object.ID(""), bam.Oid)

	bmi.Close()
}
