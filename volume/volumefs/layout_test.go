package volumefs // nolint

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// nolint:gocritic
func TestLayout(t *testing.T) {
	assert := assert.New(t)

	_, th := newVolFsTestHarness(t)
	defer th.cleanup()

	// Test layout property computation
	Ki := int64(1024)
	Mi := 1024 * Ki
	Gi := 1024 * Mi
	Ti := 1024 * Gi
	Ei := 1024 * Ti

	layoutTCs := []layoutProperties{ // * indicates >16Ti size
		{blockSzB: 64 * Ki, dirSz: 32, depth: 3, maxVolSzB: 2 * Gi, dirSzLog2: 5, dirSzMask: 31, maxBlocks: 32 * Ki, dirHexFmtLen: 2},
		{blockSzB: 64 * Ki, dirSz: 64, depth: 3, maxVolSzB: 16 * Gi, dirSzLog2: 6, dirSzMask: 63, maxBlocks: 256 * Ki, dirHexFmtLen: 2},
		{blockSzB: 64 * Ki, dirSz: 128, depth: 3, maxVolSzB: 128 * Gi, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 2 * Mi, dirHexFmtLen: 2},
		{blockSzB: 64 * Ki, dirSz: 256, depth: 3, maxVolSzB: 1 * Ti, dirSzLog2: 8, dirSzMask: 255, maxBlocks: 16 * Mi, dirHexFmtLen: 2},
		{blockSzB: 64 * Ki, dirSz: 512, depth: 3, maxVolSzB: 8 * Ti, dirSzLog2: 9, dirSzMask: 511, maxBlocks: 128 * Mi, dirHexFmtLen: 3},
		{blockSzB: 64 * Ki, dirSz: 128, depth: 4, maxVolSzB: 16 * Ti, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 256 * Mi, dirHexFmtLen: 2}, // *
		{blockSzB: 64 * Ki, dirSz: 256, depth: 4, maxVolSzB: 256 * Ti, dirSzLog2: 8, dirSzMask: 255, maxBlocks: 4 * Gi, dirHexFmtLen: 2},  // *
		{blockSzB: 64 * Ki, dirSz: 32, depth: 5, maxVolSzB: 2 * Ti, dirSzLog2: 5, dirSzMask: 31, maxBlocks: 32 * Mi, dirHexFmtLen: 2},
		{blockSzB: 64 * Ki, dirSz: 64, depth: 5, maxVolSzB: 64 * Ti, dirSzLog2: 6, dirSzMask: 63, maxBlocks: 1 * Gi, dirHexFmtLen: 2},   // *
		{blockSzB: 64 * Ki, dirSz: 128, depth: 5, maxVolSzB: 2 * Ei, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 32 * Gi, dirHexFmtLen: 2}, // *

		{blockSzB: 256 * Ki, dirSz: 32, depth: 3, maxVolSzB: 8 * Gi, dirSzLog2: 5, dirSzMask: 31, maxBlocks: 32 * Ki, dirHexFmtLen: 2},
		{blockSzB: 256 * Ki, dirSz: 64, depth: 3, maxVolSzB: 64 * Gi, dirSzLog2: 6, dirSzMask: 63, maxBlocks: 256 * Ki, dirHexFmtLen: 2},
		{blockSzB: 256 * Ki, dirSz: 128, depth: 3, maxVolSzB: 512 * Gi, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 2 * Mi, dirHexFmtLen: 2},
		{blockSzB: 256 * Ki, dirSz: 256, depth: 3, maxVolSzB: 4 * Ti, dirSzLog2: 8, dirSzMask: 255, maxBlocks: 16 * Mi, dirHexFmtLen: 2},
		{blockSzB: 256 * Ki, dirSz: 512, depth: 3, maxVolSzB: 32 * Ti, dirSzLog2: 9, dirSzMask: 511, maxBlocks: 128 * Mi, dirHexFmtLen: 3}, // *
		{blockSzB: 256 * Ki, dirSz: 128, depth: 4, maxVolSzB: 64 * Ti, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 256 * Mi, dirHexFmtLen: 2}, // *
		{blockSzB: 256 * Ki, dirSz: 256, depth: 4, maxVolSzB: 1 * Ei, dirSzLog2: 8, dirSzMask: 255, maxBlocks: 4 * Gi, dirHexFmtLen: 2},    // *
		{blockSzB: 256 * Ki, dirSz: 32, depth: 5, maxVolSzB: 8 * Ti, dirSzLog2: 5, dirSzMask: 31, maxBlocks: 32 * Mi, dirHexFmtLen: 2},
		{blockSzB: 256 * Ki, dirSz: 64, depth: 5, maxVolSzB: 256 * Ti, dirSzLog2: 6, dirSzMask: 63, maxBlocks: 1 * Gi, dirHexFmtLen: 2}, // *
		{blockSzB: 256 * Ki, dirSz: 128, depth: 5, maxVolSzB: 8 * Ei, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 32 * Gi, dirHexFmtLen: 2},

		{blockSzB: 1 * Mi, dirSz: 32, depth: 3, maxVolSzB: 32 * Gi, dirSzLog2: 5, dirSzMask: 31, maxBlocks: 32 * Ki, dirHexFmtLen: 2},
		{blockSzB: 1 * Mi, dirSz: 64, depth: 3, maxVolSzB: 256 * Gi, dirSzLog2: 6, dirSzMask: 63, maxBlocks: 256 * Ki, dirHexFmtLen: 2},
		{blockSzB: 1 * Mi, dirSz: 128, depth: 3, maxVolSzB: 2 * Ti, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 2 * Mi, dirHexFmtLen: 2},
		{blockSzB: 1 * Mi, dirSz: 256, depth: 3, maxVolSzB: 16 * Ti, dirSzLog2: 8, dirSzMask: 255, maxBlocks: 16 * Mi, dirHexFmtLen: 2},   // *
		{blockSzB: 1 * Mi, dirSz: 512, depth: 3, maxVolSzB: 128 * Ti, dirSzLog2: 9, dirSzMask: 511, maxBlocks: 128 * Mi, dirHexFmtLen: 3}, // *
		{blockSzB: 1 * Mi, dirSz: 128, depth: 4, maxVolSzB: 256 * Ti, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 256 * Mi, dirHexFmtLen: 2}, // *
		{blockSzB: 1 * Mi, dirSz: 256, depth: 4, maxVolSzB: 4 * Ei, dirSzLog2: 8, dirSzMask: 255, maxBlocks: 4 * Gi, dirHexFmtLen: 2},     // *
		{blockSzB: 1 * Mi, dirSz: 32, depth: 5, maxVolSzB: 32 * Ti, dirSzLog2: 5, dirSzMask: 31, maxBlocks: 32 * Mi, dirHexFmtLen: 2},     // *
		{blockSzB: 1 * Mi, dirSz: 64, depth: 5, maxVolSzB: 1 * Ei, dirSzLog2: 6, dirSzMask: 63, maxBlocks: 1 * Gi, dirHexFmtLen: 2},       // *
		{blockSzB: 1 * Mi, dirSz: 128, depth: 5, maxVolSzB: 32 * Ei, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 32 * Gi, dirHexFmtLen: 2},   // *
	}
	for i, tc := range layoutTCs {
		tc.dirTopHexFmtLen = tc.dirHexFmtLen + extraTopHexFmtLen // fill in
		tc.fileHexFmtLen = fileHexFmtLen                         // 0 values
		l := &layoutProperties{}
		l.initLayoutProperties(tc.blockSzB, tc.dirSz, tc.depth)
		assert.Equal(tc, *l, "case %d", i)
	}

	// Test address to path
	f := th.filesystem(nil)
	assert.Equal(snapshotBlockSize, f.GetBlockSize())
	assert.Equal(snapshotBlockSize, f.blockSzB)
	assert.Equal(maxDirEntries, f.dirSz)
	assert.Equal(maxTreeDepth, f.depth)

	addrTCs := []struct {
		addr int64
		pp   parsedPath
	}{
		{0x00000000, parsedPath{"000", "00", "000000000000"}},
		{0x000000ff, parsedPath{"000", "00", "0000000000ff"}},
		{0x00000100, parsedPath{"000", "01", "000000000100"}},
		{0x00001000, parsedPath{"000", "10", "000000001000"}},
		{0x00010100, parsedPath{"001", "01", "000000010100"}},
		{0x000111ff, parsedPath{"001", "11", "0000000111ff"}},
		{0x001011ff, parsedPath{"010", "11", "0000001011ff"}},
		{0x00ffffff, parsedPath{"0ff", "ff", "000000ffffff"}},
	}
	for i, tc := range addrTCs {
		assert.EqualValues(3, f.depth)   // test case is set up
		assert.EqualValues(256, f.dirSz) // for default values
		pp, err := f.addrToPath(tc.addr)
		assert.NoError(err, "case %d", i)
		assert.Equal(tc.pp, pp, "case %d", i)

		fm := &fileMeta{
			name: pp[len(pp)-1],
		}
		assert.Equal(tc.addr, fm.blockAddr(), "case %d", i)

		addr := f.pathToAddr(tc.pp)
		assert.Equal(tc.addr, addr, "case %d %s=>%0x exp=%0x", i, pp.String(), addr, tc.addr)
	}

	_, err := f.addrToPath(f.maxBlocks)
	assert.Error(err)
	assert.Regexp("out of range", err.Error())
	_, err = f.addrToPath(-1)
	assert.Error(err)
	assert.Regexp("out of range", err.Error())
}
