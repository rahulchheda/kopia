package volumefs // nolint

import (
	"fmt"
	"path"
	"testing"
	"time"

	"github.com/kopia/kopia/volume"
	"github.com/kopia/kopia/volume/fake"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestFilesystemArgs(t *testing.T) {
	assert := assert.New(t)

	_, th := newVolFsTestHarness(t)
	defer th.cleanup()

	tcs := []FilesystemArgs{
		{},
		{Repo: th.repo},
		{Repo: th.repo, VolumeID: "volid"},
	}
	for i, tc := range tcs {
		assert.Error(tc.Validate(), "case %d", i)
		f, err := New(&tc) // nolint:scopelint
		assert.Error(err)
		assert.Nil(f)
	}

	fa := FilesystemArgs{
		Repo:             th.repo,
		VolumeID:         "volID",
		VolumeSnapshotID: "volSnapID1",
	}
	t.Logf("%#v", fa)
	assert.NoError(fa.Validate())
	si := fa.SourceInfo()
	assert.Equal(th.repo.Hostname(), si.Host)
	assert.Equal(th.repo.Username(), si.UserName)
	assert.Equal(path.Join("/volume", fa.VolumeID), si.Path)
}

// nolint:wsl,gocritic,gocyclo
func TestE2E(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	mgr := volume.FindManager(fake.VolumeType)
	assert.NotNil(mgr)

	lastSnap := 4

	// block descriptors with history of when set
	type blockDesc struct {
		addr      int64
		setInSnap []int // ascending order
	}
	blockHistory := []*blockDesc{ // must be in ascending order of block address
		{0, []int{1, 2, 3, 4}},
		{1, []int{1}},
		{2, []int{2}},
		{3, []int{3}},
		{4, []int{4}},
		{1000, []int{2, 3}},
		{10000, []int{1, 2}},
		{100000, []int{1, 3}},
		{1000000, []int{1, 4}},
	}
	blockSetInSnap := func(bh *blockDesc, value int) bool {
		for _, v := range bh.setInSnap {
			if v == value {
				return true
			}
			if v > value {
				break
			}
		}
		return false
	}
	blockInSnap := func(bh *blockDesc, value int) bool {
		for _, v := range bh.setInSnap {
			if v <= value {
				return true
			}
		}
		return false
	}

	blocksSeen := 0
	for snapNum := 1; snapNum <= lastSnap; snapNum++ {
		snapLabel := fmt.Sprintf("%d", snapNum)
		prevLabel := ""
		if snapNum > 1 {
			prevLabel = fmt.Sprintf("%d", snapNum-1)
		}

		fa := &FilesystemArgs{
			Repo:             th.repo,
			VolumeID:         "volID",
			VolumeSnapshotID: snapLabel,
		}
		f, err := New(fa)
		assert.NoError(err)

		// build the changed block profile for this snapshot
		profile := &fake.ReaderProfile{}
		for _, bh := range blockHistory {
			if !blockSetInSnap(bh, snapNum) {
				continue
			}
			profile.Ranges = append(profile.Ranges, fake.BlockAddrRange{Start: bh.addr, Count: 1, Value: snapLabel})
		}

		ba := BackupArgs{
			PreviousVolumeSnapshotID: prevLabel,
			VolumeManager:            mgr,
			VolumeAccessProfile:      profile,
		}

		bRes, err := f.Backup(ctx, ba)
		assert.NoError(err)
		assert.NotNil(bRes)

		// validate that blocks are present
		bw := &testBW{}
		bw.waitChan = make(chan int)
		tvm := &testVM{}
		tvm.retGbwBW = []volume.BlockWriter{nil, bw}

		done := false
		var rRes *RestoreResult
		go func() {
			fr, fErr := New(fa)
			assert.NoError(fErr)

			ra := RestoreArgs{
				VolumeManager:       tvm,
				VolumeAccessProfile: tvm, // not needed
			}
			rRes, err = fr.Restore(ctx, ra)
			done = true
		}()

		for bw.inPutBlocksBI == nil {
			time.Sleep(20 * time.Millisecond)
		}

		bi := bw.inPutBlocksBI
		seenBlock := map[int64]struct{}{}
		for {
			b := bi.Next(ctx)
			if b == nil && bi.AtEnd() {
				cErr := bi.Close()
				assert.NoError(cErr)
				break
			}
			t.Logf("Snap#%d Block:%d", snapNum, b.Address())
			seenBlock[b.Address()] = struct{}{}
		}
		close(bw.waitChan)

		for !done {
			time.Sleep(20 * time.Millisecond)
		}
		assert.NoError(err)
		assert.NotNil(rRes)
		assert.Equal(rRes.NumBlocks, len(seenBlock))

		blocksSeen = 0
		for _, bh := range blockHistory {
			_, present := seenBlock[bh.addr]
			assert.Equal(blockInSnap(bh, snapNum), present, "Snap#: %d BA:%d P:%v", snapNum, bh.addr, present)
			if present {
				blocksSeen++
			}
		}

		// compact before the last snapshot
		if snapNum == lastSnap-1 {
			fc, fErr := New(fa)
			assert.NoError(fErr)

			ca := CompactArgs{}
			cRes, cErr := fc.Compact(ctx, ca)
			assert.NoError(cErr)
			assert.NotNil(cRes)
		}
	}
	assert.Equal(len(blockHistory), blocksSeen)
}
