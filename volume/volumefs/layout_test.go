package volumefs // nolint

import (
	"strconv"
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

	layoutTCs := []layoutProperties{ // * indicates >= 16Ti size
		{blockSzB: int(16 * Ki), dirSz: 512, depth: 3, maxVolSzB: 2 * Ti, dirSzLog2: 9, dirSzMask: 511, maxBlocks: 128 * Mi},
		{blockSzB: int(16 * Ki), dirSz: 1024, depth: 3, maxVolSzB: 16 * Ti, dirSzLog2: 10, dirSzMask: 1023, maxBlocks: 1 * Gi}, // *
		{blockSzB: int(16 * Ki), dirSz: 256, depth: 4, maxVolSzB: 64 * Ti, dirSzLog2: 8, dirSzMask: 255, maxBlocks: 4 * Gi},    // *
		{blockSzB: int(16 * Ki), dirSz: 256, depth: 5, maxVolSzB: 16 * Ei, dirSzLog2: 8, dirSzMask: 255, maxBlocks: 1 * Ti},    // *

		{blockSzB: int(64 * Ki), dirSz: 32, depth: 3, maxVolSzB: 2 * Gi, dirSzLog2: 5, dirSzMask: 31, maxBlocks: 32 * Ki},
		{blockSzB: int(64 * Ki), dirSz: 64, depth: 3, maxVolSzB: 16 * Gi, dirSzLog2: 6, dirSzMask: 63, maxBlocks: 256 * Ki},
		{blockSzB: int(64 * Ki), dirSz: 128, depth: 3, maxVolSzB: 128 * Gi, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 2 * Mi},
		{blockSzB: int(64 * Ki), dirSz: 256, depth: 3, maxVolSzB: 1 * Ti, dirSzLog2: 8, dirSzMask: 255, maxBlocks: 16 * Mi},
		{blockSzB: int(64 * Ki), dirSz: 512, depth: 3, maxVolSzB: 8 * Ti, dirSzLog2: 9, dirSzMask: 511, maxBlocks: 128 * Mi},
		{blockSzB: int(64 * Ki), dirSz: 128, depth: 4, maxVolSzB: 16 * Ti, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 256 * Mi}, // *
		{blockSzB: int(64 * Ki), dirSz: 256, depth: 4, maxVolSzB: 256 * Ti, dirSzLog2: 8, dirSzMask: 255, maxBlocks: 4 * Gi},  // *
		{blockSzB: int(64 * Ki), dirSz: 32, depth: 5, maxVolSzB: 2 * Ti, dirSzLog2: 5, dirSzMask: 31, maxBlocks: 32 * Mi},
		{blockSzB: int(64 * Ki), dirSz: 64, depth: 5, maxVolSzB: 64 * Ti, dirSzLog2: 6, dirSzMask: 63, maxBlocks: 1 * Gi},   // *
		{blockSzB: int(64 * Ki), dirSz: 128, depth: 5, maxVolSzB: 2 * Ei, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 32 * Gi}, // *

		{blockSzB: int(256 * Ki), dirSz: 32, depth: 3, maxVolSzB: 8 * Gi, dirSzLog2: 5, dirSzMask: 31, maxBlocks: 32 * Ki},
		{blockSzB: int(256 * Ki), dirSz: 64, depth: 3, maxVolSzB: 64 * Gi, dirSzLog2: 6, dirSzMask: 63, maxBlocks: 256 * Ki},
		{blockSzB: int(256 * Ki), dirSz: 128, depth: 3, maxVolSzB: 512 * Gi, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 2 * Mi},
		{blockSzB: int(256 * Ki), dirSz: 256, depth: 3, maxVolSzB: 4 * Ti, dirSzLog2: 8, dirSzMask: 255, maxBlocks: 16 * Mi},
		{blockSzB: int(256 * Ki), dirSz: 512, depth: 3, maxVolSzB: 32 * Ti, dirSzLog2: 9, dirSzMask: 511, maxBlocks: 128 * Mi}, // *
		{blockSzB: int(256 * Ki), dirSz: 128, depth: 4, maxVolSzB: 64 * Ti, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 256 * Mi}, // *
		{blockSzB: int(256 * Ki), dirSz: 256, depth: 4, maxVolSzB: 1 * Ei, dirSzLog2: 8, dirSzMask: 255, maxBlocks: 4 * Gi},    // *
		{blockSzB: int(256 * Ki), dirSz: 32, depth: 5, maxVolSzB: 8 * Ti, dirSzLog2: 5, dirSzMask: 31, maxBlocks: 32 * Mi},
		{blockSzB: int(256 * Ki), dirSz: 64, depth: 5, maxVolSzB: 256 * Ti, dirSzLog2: 6, dirSzMask: 63, maxBlocks: 1 * Gi}, // *
		{blockSzB: int(256 * Ki), dirSz: 128, depth: 5, maxVolSzB: 8 * Ei, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 32 * Gi},

		{blockSzB: int(1 * Mi), dirSz: 32, depth: 3, maxVolSzB: 32 * Gi, dirSzLog2: 5, dirSzMask: 31, maxBlocks: 32 * Ki},
		{blockSzB: int(1 * Mi), dirSz: 64, depth: 3, maxVolSzB: 256 * Gi, dirSzLog2: 6, dirSzMask: 63, maxBlocks: 256 * Ki},
		{blockSzB: int(1 * Mi), dirSz: 128, depth: 3, maxVolSzB: 2 * Ti, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 2 * Mi},
		{blockSzB: int(1 * Mi), dirSz: 256, depth: 3, maxVolSzB: 16 * Ti, dirSzLog2: 8, dirSzMask: 255, maxBlocks: 16 * Mi},   // *
		{blockSzB: int(1 * Mi), dirSz: 512, depth: 3, maxVolSzB: 128 * Ti, dirSzLog2: 9, dirSzMask: 511, maxBlocks: 128 * Mi}, // *
		{blockSzB: int(1 * Mi), dirSz: 128, depth: 4, maxVolSzB: 256 * Ti, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 256 * Mi}, // *
		{blockSzB: int(1 * Mi), dirSz: 256, depth: 4, maxVolSzB: 4 * Ei, dirSzLog2: 8, dirSzMask: 255, maxBlocks: 4 * Gi},     // *
		{blockSzB: int(1 * Mi), dirSz: 32, depth: 5, maxVolSzB: 32 * Ti, dirSzLog2: 5, dirSzMask: 31, maxBlocks: 32 * Mi},     // *
		{blockSzB: int(1 * Mi), dirSz: 64, depth: 5, maxVolSzB: 1 * Ei, dirSzLog2: 6, dirSzMask: 63, maxBlocks: 1 * Gi},       // *
		{blockSzB: int(1 * Mi), dirSz: 128, depth: 5, maxVolSzB: 32 * Ei, dirSzLog2: 7, dirSzMask: 127, maxBlocks: 32 * Gi},   // *
	}
	for i, tc := range layoutTCs {
		f := &Filesystem{}
		f.initLayoutProperties(tc.blockSzB, tc.dirSz, tc.depth)
		tc.baseEncoding = baseEncoding
		assert.Equal(tc, f.layoutProperties, "case %d", i)

		pp := f.newParsedPath()
		assert.True(cap(pp) == tc.depth)
		assert.True(len(pp) == 0)
		assert.Equal("", pp.Last())
	}

	// Test address to path
	f := th.fs()
	assert.Equal(snapshotBlockSize, f.GetBlockSize())
	assert.Equal(snapshotBlockSize, f.blockSzB)
	assert.Equal(maxDirEntries, f.dirSz)
	assert.Equal(maxTreeDepth, f.depth)
	_, err := f.addrToPath(f.maxBlocks)
	assert.Error(err)
	assert.Regexp("out of range", err.Error())
	_, err = f.addrToPath(-1)
	assert.Error(err)
	assert.Regexp("out of range", err.Error())

	type addrPP struct {
		addr int64
		pp   parsedPath
	}

	addrTCs := []struct {
		dirSz    int
		depth    int
		encoding int
		cases    []addrPP
	}{
		{2048, 3, 32, []addrPP{ // max 8 Gi
			{0x000000000000, parsedPath{"0", "0", "0"}},
			{0x0000000000ff, parsedPath{"0", "0", "7v"}},
			{0x000000000100, parsedPath{"0", "0", "80"}},
			{0x0000000003ff, parsedPath{"0", "0", "vv"}},
			{0x000000000400, parsedPath{"0", "0", "100"}},
			{0x0000000007ff, parsedPath{"0", "0", "1vv"}},
			{0x000000000800, parsedPath{"0", "1", "0"}},
			{0x0000000fffff, parsedPath{"0", "fv", "1vv"}},
			{0x0000007fffff, parsedPath{"1", "1vv", "1vv"}},
			{0x000000ffffff, parsedPath{"3", "1vv", "1vv"}},
			{0x0001ffffffff, parsedPath{"1vv", "1vv", "1vv"}},
		}},
		{1024, 3, 16, []addrPP{ // max 1 Gi
			{0x000000000000, parsedPath{"0", "0", "0"}},
			{0x0000000000ff, parsedPath{"0", "0", "ff"}},
			{0x000000000100, parsedPath{"0", "0", "100"}},
			{0x000000001000, parsedPath{"0", "4", "0"}},
			{0x000000010100, parsedPath{"0", "40", "100"}},
			{0x0000000111ff, parsedPath{"0", "44", "1ff"}},
			{0x0000001011ff, parsedPath{"1", "4", "1ff"}},
			{0x000000ffffff, parsedPath{"f", "3ff", "3ff"}},
			{0x00003fffffff, parsedPath{"3ff", "3ff", "3ff"}},
		}},
		{1024, 3, 32, []addrPP{ // max 1 Gi
			{0x000000000000, parsedPath{"0", "0", "0"}},
			{0x0000000000ff, parsedPath{"0", "0", "7v"}},
			{0x000000000100, parsedPath{"0", "0", "80"}},
			{0x000000001000, parsedPath{"0", "4", "0"}},
			{0x000000010100, parsedPath{"0", "20", "80"}},
			{0x0000000111ff, parsedPath{"0", "24", "fv"}},
			{0x0000001011ff, parsedPath{"1", "4", "fv"}},
			{0x000000ffffff, parsedPath{"f", "vv", "vv"}},
			{0x00003fffffff, parsedPath{"vv", "vv", "vv"}},
		}},
		{256, 4, 16, []addrPP{ // max 4Gi
			{0x000000000000, parsedPath{"0", "0", "0", "0"}},
			{0x0000000000ff, parsedPath{"0", "0", "0", "ff"}},
			{0x000000000100, parsedPath{"0", "0", "1", "0"}},
			{0x000000001000, parsedPath{"0", "0", "10", "0"}},
			{0x000000010100, parsedPath{"0", "1", "1", "0"}},
			{0x0000000111ff, parsedPath{"0", "1", "11", "ff"}},
			{0x0000001011ff, parsedPath{"0", "10", "11", "ff"}},
			{0x0000ffffffff, parsedPath{"ff", "ff", "ff", "ff"}},
		}},
		{256, 4, 32, []addrPP{ // max 4Gi
			{0x000000000000, parsedPath{"0", "0", "0", "0"}},
			{0x0000000000ff, parsedPath{"0", "0", "0", "7v"}},
			{0x000000000100, parsedPath{"0", "0", "1", "0"}},
			{0x000000001000, parsedPath{"0", "0", "g", "0"}},
			{0x000000010100, parsedPath{"0", "1", "1", "0"}},
			{0x0000000111ff, parsedPath{"0", "1", "h", "7v"}},
			{0x0000001011ff, parsedPath{"0", "g", "h", "7v"}},
			{0x0000ffffffff, parsedPath{"7v", "7v", "7v", "7v"}},
		}},
		{256, 4, 36, []addrPP{ // max 4Gi
			{0x000000000000, parsedPath{"0", "0", "0", "0"}},
			{0x0000000000ff, parsedPath{"0", "0", "0", "73"}},
			{0x000000000100, parsedPath{"0", "0", "1", "0"}},
			{0x000000001000, parsedPath{"0", "0", "g", "0"}},
			{0x000000010100, parsedPath{"0", "1", "1", "0"}},
			{0x0000000111ff, parsedPath{"0", "1", "h", "73"}},
			{0x0000001011ff, parsedPath{"0", "g", "h", "73"}},
			{0x0000ffffffff, parsedPath{"73", "73", "73", "73"}},
		}},
		{256, 5, 16, []addrPP{ // max 1Ti
			{0x000000000000, parsedPath{"0", "0", "0", "0", "0"}},
			{0x0000000000ff, parsedPath{"0", "0", "0", "0", "ff"}},
			{0x000000000100, parsedPath{"0", "0", "0", "1", "0"}},
			{0x000000001000, parsedPath{"0", "0", "0", "10", "0"}},
			{0x000000010100, parsedPath{"0", "0", "1", "1", "0"}},
			{0x0000000111ff, parsedPath{"0", "0", "1", "11", "ff"}},
			{0x0000001011ff, parsedPath{"0", "0", "10", "11", "ff"}},
			{0x0000ffffffff, parsedPath{"0", "ff", "ff", "ff", "ff"}},
			{0x000100000000, parsedPath{"1", "0", "0", "0", "0"}},
			{0x00ffffffffff, parsedPath{"ff", "ff", "ff", "ff", "ff"}},
		}},
	}
	for i, set := range addrTCs {
		f.initLayoutProperties(16384, set.dirSz, set.depth)
		f.baseEncoding = set.encoding

		for j, tc := range set.cases {
			t.Logf("*** [%d,%d] x%012x", i, j, tc.addr)
			pp, err := f.addrToPath(tc.addr)
			assert.NoError(err)
			assert.Equal(tc.pp, pp)
			assert.True(len(pp) == set.depth)
			assert.Equal(pp[len(pp)-1], pp.Last())

			cpp := pp.Child("child")
			assert.True(len(cpp) == len(pp)+1)
			assert.Equal("child", cpp.Last())
			assert.Equal(pp, cpp[0:len(pp)])

			addr := f.pathToAddr(tc.pp)
			assert.Equal(tc.addr, addr, "case %d.%d pathToAddr %s=>%0x exp=%0x", i, j, pp.String(), addr, tc.addr)
		}
	}
}

// this is not really a test but just a visualization tool
// nolint:wsl,gocritic
func TestEncoding(t *testing.T) {
	assert := assert.New(t)

	// base 32 is more efficient to encode than 36 (as per the library comments)
	for _, tc := range []struct {
		n int64
		s string
	}{
		{0, "0"},
		{10, "a"},
		{31, "v"},
		{32, "10"},
		{63, "1v"},
		{64, "20"},
		{96, "30"},
		{128, "40"},
		{255, "7v"},
		{256, "80"},
		{511, "fv"},
		{512, "g0"},
		{1023, "vv"},
		{1024, "100"}, // >= 3 chars
		{2047, "1vv"},
		{2048, "200"},
		{4096, "400"},
	} {
		s := strconv.FormatInt(tc.n, 32)
		t.Log(tc.n, tc.s, s)
		assert.Equal(tc.s, s)
		i, err := strconv.ParseInt(tc.s, 32, 64)
		assert.NoError(err)
		assert.Equal(tc.n, i)
	}

	// base 36
	for _, tc := range []struct {
		n int64
		s string
	}{
		{0, "0"},
		{10, "a"},
		{35, "z"},
		{36, "10"},
		{71, "1z"},
		{72, "20"},
		{108, "30"},
		{144, "40"},
		{255, "73"},
		{256, "74"},
		{511, "e7"},
		{512, "e8"},
		{1023, "sf"},
		{1024, "sg"},
		{1296, "100"}, // >= 3 chars
		{2048, "1kw"},
		{4096, "35s"},
	} {
		s := strconv.FormatInt(tc.n, 36)
		t.Log(tc.n, tc.s, s)
		assert.Equal(tc.s, s)
		i, err := strconv.ParseInt(tc.s, 36, 64)
		assert.NoError(err)
		assert.Equal(tc.n, i)
	}
}
