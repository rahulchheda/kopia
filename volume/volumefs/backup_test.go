package volumefs // nolint

import (
	"fmt"
	"testing"
	"time"

	"github.com/kopia/kopia/snapshot"
	vmgr "github.com/kopia/kopia/volume/fake"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestInitializeForBackupNoPreviousSnapshot(t *testing.T) {
	assert := assert.New(t)

	var err error

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	profile := &vmgr.ReaderProfile{
		Ranges: []vmgr.BlockAddrRange{
			{Start: 0, Count: 1},
			{Start: 0x100, Count: 1},
			{Start: 0x111ff, Count: 1},
		},
	}
	expAddresses := []int64{0, 0x100, 0x111ff}

	f := th.filesystem(profile)
	f.blockSzB = 512 // HACK

	// Case: first backup
	tB := time.Now()
	root, err := f.InitializeForBackup(ctx, "", "")
	tA := time.Now()
	assert.NoError(err)
	assert.WithinDuration(tA, f.epoch, tA.Sub(tB))
	assert.NotNil(root)
	assert.NotNil(f.rootDir)
	de, ok := root.(*dirEntry)
	assert.True(ok)
	assert.Equal(f.rootDir, de.m)

	// check the hierarchy; all should have timestamp equal to f.epoch
	dirCnt := 0
	fileCnt := 0
	f.rootDir.descend(func(dm *dirMeta, pp parsedPath, arg interface{}) {
		assert.Equal(f.epoch, dm.mTime)
		switch x := arg.(type) {
		case bool:
			if x {
				dirCnt++
			}
		case *fileMeta:
			fileCnt++
			assert.Equal(f.epoch, x.mTime)
		}
	})
	assert.Equal(len(expAddresses)+3, fileCnt) // includes meta
	assert.Equal(6, dirCnt)

	for _, ba := range expAddresses {
		pp, err := f.addrToPath(ba)
		assert.NoError(err, "addr[%s] parsed", ba)

		fm := f.lookupFile(ctx, pp)
		assert.NotNil(fm, "file[%s] exists", pp)
	}

	fn := fmt.Sprintf(metaFmtX, metaBlockSzB, f.blockSzB)
	fm := f.lookupFile(ctx, parsedPath{fn})
	assert.NotNil(fm, "meta %s", fn)

	fn = fmt.Sprintf(metaFmtX, metaDirSz, f.dirSz)
	fm = f.lookupFile(ctx, parsedPath{fn})
	assert.NotNil(fm, "meta %s", fn)

	fn = fmt.Sprintf(metaFmtX, metaDepth, f.depth)
	fm = f.lookupFile(ctx, parsedPath{fn})
	assert.NotNil(fm, "meta %s", fn)

	th.compareInternalAndExternalTrees(ctx, f, root)

	// Case: failure
	fm = f.lookupFile(ctx, parsedPath{"000", "123", "foo"})
	assert.Nil(fm)
}

// adapted from snapshot_test.go
// nolint:gocritic
func TestScanGetSnapshotManifest(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	// t0: snapshot of source 1
	snap1_0 := th.addSnapshot(ctx, []int64{0, 1024, 2048})

	// t1: snapshot of source 2
	f := th.filesystem(&vmgr.ReaderProfile{})
	f.VolumeID = f.VolumeID + "FOOO"
	th.retFS = f
	snap2_0 := th.addSnapshot(ctx, []int64{999, 111111, 131313})
	th.retFS = nil

	// t1: snapshot of source 1
	snap1_1 := th.addSnapshot(ctx, []int64{0, 1024, 2048}) // no change

	// validate timestamps
	assert.True(snap1_0.StartTime.Before(snap2_0.StartTime))
	assert.True(snap2_0.StartTime.Before(snap1_1.StartTime))

	// validate sources
	assert.Equal(snap1_0.Source, snap1_1.Source)
	assert.NotEqual(snap1_0.Source, snap2_0.Source)

	// validate IDs
	assert.NotEqual(snap1_0.ID, snap1_1.ID)
	assert.NotEqual(snap1_0.ID, snap2_0.ID)
	assert.NotEqual(snap1_1.ID, snap2_0.ID)
	assert.Equal(snap1_0.RootObjectID(), snap1_1.RootObjectID()) // same root
	assert.NotEqual(snap1_0.RootObjectID(), snap2_0.RootObjectID())
	assert.NotEqual(snap1_1.RootObjectID(), snap2_0.RootObjectID())

	t.Logf("s1_0: [%s,%s,%s]", snap1_0.RootObjectID(), snap1_0.ID, snap1_0.StartTime)
	t.Logf("s1_1: [%s,%s,%s]", snap1_1.RootObjectID(), snap1_1.ID, snap1_1.StartTime)
	t.Logf("s2_0: [%s,%s,%s]", snap2_0.RootObjectID(), snap2_0.ID, snap2_0.StartTime)

	f = th.filesystem(&vmgr.ReaderProfile{}) // Reset
	items, err := snapshot.ListSnapshots(ctx, th.repo, f.SourceInfo())
	assert.Len(items, 2)
	assert.NoError(err)

	// Case: success
	man, err := f.findSnapshotManifest(ctx, snap1_0.RootObjectID())
	assert.NoError(err)
	assert.NotNil(man)
	assert.Equal(snap1_1.ID, man.ID)

	// Case: not found
	man, err = f.findSnapshotManifest(ctx, snap1_0.RootObjectID()+"foo")
	assert.Error(err)
	assert.Nil(man)
}

// nolint:wsl,gocritic
func TestInitializeForBackupWithPreviousSnapshot(t *testing.T) {
	assert := assert.New(t)

	var err error

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	profile := &vmgr.ReaderProfile{
		Ranges: []vmgr.BlockAddrRange{ // 3 changed blocks
			{Start: 0, Count: 1},
			{Start: 0x100, Count: 1},
			{Start: 0x111ff, Count: 1},
		},
	}
	changedAddresses := []int64{0, 0x100, 0x111ff}
	f := th.filesystem(profile)
	defaultBlockSize := f.GetBlockSize()
	f.blockSzB = defaultBlockSize * 2 // make a change that should be rolled back

	prevAddresses := []int64{0, 1, 2, 0x1000, 0x50000} // fake a previous snapshot with some partially overlapping addresses
	snap := th.addSnapshot(ctx, prevAddresses)
	t.Log(string(snap.RootObjectID()))

	// Case: trivial failure
	root, err := f.InitializeForBackup(ctx, string(snap.RootObjectID())+"foo", "volSnapID0")
	assert.Error(err)
	assert.Nil(root)

	// Case: subsequent backup
	tB := time.Now()
	root, err = f.InitializeForBackup(ctx, string(snap.RootObjectID()), "volSnapID0")
	tA := time.Now()

	assert.NoError(err)
	assert.WithinDuration(tA, f.epoch, tA.Sub(tB))
	assert.NotNil(root)
	assert.NotNil(f.rootDir)

	de, ok := root.(*dirEntry)
	assert.True(ok)
	assert.Equal(f.rootDir, de.m)

	// check that the default block size was restored by the snapshot
	assert.Equal(defaultBlockSize, f.GetBlockSize())

	// determine what should have changed
	prevUpdated := analyseAddresses(t, f, "prev", prevAddresses, nil)
	changedUpdated := analyseAddresses(t, f, "changed", changedAddresses, prevUpdated)

	// walk the internal tree and check the timestamps
	updateCnt := 0
	f.rootDir.descend(func(dm *dirMeta, pp parsedPath, arg interface{}) {
		switch x := arg.(type) {
		case bool:
			if x {
				if _, ok := changedUpdated[pp.String()]; ok {
					updateCnt++
					// t.Logf("Found updated dir [%s]", pp)
					assert.Equal(f.epoch, dm.mTime, "dir [%s]", pp)
				} else {
					// t.Logf("Found older dir [%s]", pp)
					assert.True(f.epoch.After(dm.mTime), "dir [%s]", pp)
				}
			}
		case *fileMeta:
			if _, ok := changedUpdated[pp.String()]; ok {
				updateCnt++
				// t.Logf("Found updated file [%s]", pp)
				assert.Equal(f.epoch, x.mTime, "file [%s]", pp)
			} else {
				// t.Logf("Found older file [%s]", pp)
				assert.True(f.epoch.After(x.mTime), "file [%s]", pp)
			}
		}
	})
	assert.Equal(len(changedUpdated), updateCnt)

	th.compareInternalAndExternalTrees(ctx, f, root)
}

// nolint:wsl,gocritic
func analyseAddresses(t *testing.T, f *Filesystem, prefix string, bal []int64, prev map[string]struct{}) map[string]struct{} { // nolint:unparam
	assert := assert.New(t)
	updated := map[string]struct{}{}

	for _, ba := range bal {
		pp, err := f.addrToPath(ba)
		assert.NoError(err)

		for i := 0; i < len(pp)-1; i++ { // don't consider the file in this loop
			dir := pp[0 : i+1].String()
			if _, ok := updated[dir]; ok {
				continue
			}

			if prev != nil { // don't count if present in prev
				if _, ok := prev[dir]; ok {
					// t.Logf("%s: Dir in previous [%s]", prefix, dir)
					continue
				}
				// if not in prev then parent must be modified too
				pDir := pp[0:i].String()
				// t.Logf("%s: dir pDir updated [%s]", prefix, pDir)
				updated[pDir] = struct{}{}
			}

			// t.Logf("%s: Dir updated [%s]", prefix, dir)
			updated[dir] = struct{}{}
		}

		fn := pp.String()
		if prev != nil { // if file not in prev then parent must be modified too
			if _, ok := prev[fn]; !ok {
				pDir := pp[0 : len(pp)-1].String()
				// t.Logf("%s: file pDir updated [%s]", prefix, pDir)
				updated[pDir] = struct{}{}
			}
		}

		// t.Logf("%s: File updated [%s]", prefix, fn)
		updated[fn] = struct{}{}
	}

	return updated
}
