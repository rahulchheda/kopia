package volumefs

import (
	"testing"

	"github.com/kopia/kopia/snapshot"
	vmgr "github.com/kopia/kopia/volume/fake"

	"github.com/stretchr/testify/assert"
)

// adapted from snapshot_test.go
// nolint:gocritic
func TestScanGetSnapshotManifest(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	// t0: snapshot of source 1
	snap1_0 := th.addSnapshot(ctx, []int64{0, 1024, 2048})

	// t1: snapshot of source 2
	f := th.fsForBackupTests(&vmgr.ReaderProfile{})
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

	f = th.fsForBackupTests(&vmgr.ReaderProfile{}) // Reset
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
