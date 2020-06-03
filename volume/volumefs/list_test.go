package volumefs

import (
	"testing"
	"time"

	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestListSnapshots(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	// External calls
	res, err := ListSnapshots(ctx, th.repo, "")
	assert.NoError(err)
	assert.NotNil(res)

	res, err = ListSnapshots(ctx, nil, "")
	assert.Equal(ErrInvalidArgs, err)
	assert.Nil(res)

	f1 := &Filesystem{FilesystemArgs: FilesystemArgs{VolumeID: "v1", VolumeSnapshotID: "v1s1"}}
	f2 := &Filesystem{FilesystemArgs: FilesystemArgs{VolumeID: "v2", VolumeSnapshotID: "v2s1"}}

	tNow := time.Now()
	manifests := []*snapshot.Manifest{
		{ID: "v1s1a", Description: f1.snapshotDescription(""), StartTime: tNow.Add(-1 * time.Hour)},
		{ID: "v1s1b", Description: f1.snapshotDescription(""), StartTime: tNow},
		{ID: "v1s2", Description: f1.snapshotDescription("v1s2")},
		{ID: "v2s1", Description: f2.snapshotDescription(""), StartTime: tNow},
	}
	manifestIDs := []manifest.ID{}
	manifestsV1 := []*snapshot.Manifest{}
	manifestIDsV1 := []manifest.ID{}
	expSnapV1 := []*Snapshot{}
	expSnapAll := []*Snapshot{}
	for _, m := range manifests {
		manifestIDs = append(manifestIDs, m.ID)
		v, vs := f1.parseSnapshotDescription(m)
		t.Logf("*** %s %s", v, vs)
		if v == "v1" {
			expSnapV1 = append(expSnapV1, &Snapshot{VolumeID: v, VolumeSnapshotID: vs, Manifest: m})
			manifestIDsV1 = append(manifestIDsV1, m.ID)
			manifestsV1 = append(manifestsV1, m)
		}
		expSnapAll = append(expSnapAll, &Snapshot{VolumeID: v, VolumeSnapshotID: vs, Manifest: m})
	}

	// Filesystem calls
	for _, tc := range []string{
		"lsm error", "ls error", "success all", "success v1",
	} {
		t.Logf("Case: %s", tc)

		tsp := &testSnapshotProcessor{}
		tsp.retLsmM = manifestIDs
		tsp.retLosM = manifests

		f := th.fs()
		assert.NotNil(f.sp)
		f.sp = tsp

		var expError error
		var expSnap []*Snapshot

		switch tc {
		case "lsm error":
			expError = ErrInternalError
			tsp.retLsmE = expError
		case "ls error":
			expError = ErrInternalError
			tsp.retLosE = expError
		case "success all":
			expSnap = expSnapAll
			f.VolumeID = ""
			tsp.retLosM = append(tsp.retLosM, &snapshot.Manifest{}) // add an invalid manifest
		case "success v1":
			f.VolumeID = "v1"
			expSnap = expSnapV1
			tsp.retLsmM = manifestIDsV1
			tsp.retLosM = manifestsV1
		}

		res, err := f.ListSnapshots(ctx)

		if expError == nil {
			assert.NoError(err)
			assert.NotNil(res)
			assert.Equal(expSnap, res)
			if tc == "success all" {
				assert.Nil(tsp.inLsmS)
			} else {
				assert.Equal(f.SourceInfo(), *tsp.inLsmS)
			}
		} else {
			assert.Error(err)
			assert.Regexp(expError.Error(), err.Error())
		}
	}

}
