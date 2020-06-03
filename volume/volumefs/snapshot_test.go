package volumefs

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/manifest"
	"github.com/kopia/kopia/snapshot"

	"github.com/stretchr/testify/assert"
)

// nolint:wsl,gocritic
func TestSnapshotAnalysis(t *testing.T) {
	assert := assert.New(t)

	expSA := SnapshotAnalysis{
		BlockSizeBytes:     1000,
		Bytes:              20000,
		NumBlocks:          1000,
		NumDirs:            100,
		ChainLength:        2,
		ChainedBytes:       80000,
		ChainedNumBlocks:   2000,
		ChainedNumDirs:     200,
		WeightedBlockCount: 1020402000,
	}

	curStats := snapshot.Stats{
		TotalDirectoryCount: expSA.NumDirs + expSA.ChainedNumDirs,
		TotalFileCount:      expSA.NumBlocks + expSA.ChainedNumBlocks,
		TotalFileSize:       expSA.Bytes + expSA.ChainedBytes,

		ExcludedFileCount:     expSA.ChainedNumBlocks,
		ExcludedTotalFileSize: expSA.ChainedBytes,
		ExcludedDirCount:      expSA.ChainedNumDirs,

		CachedFiles:    int32(expSA.BlockSizeBytes),
		NonCachedFiles: int32(expSA.ChainLength),

		ReadErrors: expSA.WeightedBlockCount,
	}
	sm := &snapshot.Manifest{
		Stats: curStats,
	}

	sa := SnapshotAnalysis{}
	sa.Analyze(sm)

	assert.Equal(expSA, sa)
}

// nolint:wsl,gocritic
func TestInitFromSnapshot(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	f := th.fs()
	f.logger = log(ctx)
	assert.NotNil(f.sp)

	expMD, _, expEntries := generateTestMetaData()
	manifests, tSnap := th.generateTestManifests(expMD)
	manifests = append(manifests, &snapshot.Manifest{
		Description: f.snapshotDescription("not-a-dir"),
		RootEntry:   &snapshot.DirEntry{ObjectID: "kf9fb0e450bba821a1c35585d48eaff04", Type: snapshot.EntryTypeFile},
	})

	for _, tc := range []string{
		"list error", "not found", "not-a-dir", "metadata error", "initialized",
	} {
		t.Logf("Case: %s", tc)

		tsp := &testSnapshotProcessor{}
		tsp.retLsM = manifests
		f.sp = tsp

		var expError error
		prevVolSnapshotID := expMD.VolSnapID

		switch tc {
		case "list error":
			expError = ErrInternalError
			tsp.retLsM = nil
			tsp.retLsE = expError
		case "not found":
			prevVolSnapshotID = "foo"
			expError = ErrSnapshotNotFound
		case "not-a-dir":
			prevVolSnapshotID = "not-a-dir"
			expError = ErrInvalidSnapshot
		case "metadata error":
			expError = ErrOutOfRange
			tsp.retSrEntry = &testDirEntry{retReadDirErr: expError}
		case "initialized":
			tsp.retSrEntry = &testDirEntry{retReadDirE: expEntries}
		}

		m, de, md, err := f.initFromSnapshot(ctx, prevVolSnapshotID)

		if expError == nil {
			assert.NoError(err)
			assert.NotNil(m)
			assert.NotNil(de)
			assert.Equal(tSnap, m.EndTime)

			if tc == "initialized" {
				assert.Equal(expMD, md)
			}
		} else {
			assert.Error(err)
			assert.Regexp(expError.Error(), err.Error())
			assert.Nil(m)
		}
	}
}

// nolint:gocritic
func TestCommitSnapshot(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	f := th.fs()
	f.logger = log(ctx)
	f.VolumeID = "VolumeID"
	f.VolumeSnapshotID = "VolumeSnapshotID"

	prevManifest := &snapshot.Manifest{
		RootEntry: &snapshot.DirEntry{
			ObjectID:   "k2313ef907f3b250b331aed988802e4c5",
			Type:       snapshot.EntryTypeDirectory,
			DirSummary: &fs.DirectorySummary{},
		},
		Stats: snapshot.Stats{
			TotalDirectoryCount: 1,
			TotalFileCount:      10, // previous added
			ExcludedFileCount:   2,  // 8 files
			TotalFileSize:       1,
			CachedFiles:         int32(f.blockSzB),
			NonCachedFiles:      2, // chain len
			ReadErrors:          1000,
		},
	}
	summary := &fs.DirectorySummary{
		TotalFileSize:  100,
		TotalFileCount: 19, // current adds 9 files
		TotalDirCount:  2,
	}

	for _, tc := range []string{
		"write root error", "save snapshot error", "repo flush error", "committed no prev", "committed with prev",
	} {
		t.Logf("Case: %s", tc)

		tU := &testUploader{}
		f.up = tU
		tSP := &testSnapshotProcessor{}
		f.sp = tSP
		tRepo := &testRepo{}
		f.repo = tRepo

		rootDir := &dirMeta{name: "/", summary: summary, oid: "root-dir-oid"}
		expMan := &snapshot.Manifest{
			Source:      f.SourceInfo(),
			Description: "volume:VolumeID:VolumeSnapshotID",
			StartTime:   f.epoch,
			Stats: snapshot.Stats{
				TotalDirectoryCount: 2,
				TotalFileCount:      int(summary.TotalFileCount),
				TotalFileSize:       100,
				CachedFiles:         int32(f.blockSzB),
			},
			RootEntry: &snapshot.DirEntry{
				Name:       "/",
				Type:       snapshot.EntryTypeDirectory,
				ObjectID:   rootDir.oid,
				DirSummary: summary,
			},
		}

		var expError error

		dm := rootDir
		psm := prevManifest

		switch tc {
		case "write root error":
			expError = ErrOutOfRange
			tU.retWriteDirE = expError
		case "save snapshot error":
			expError = ErrInvalidSnapshot
			tSP.retSsE = expError
		case "repo flush error":
			expError = ErrInternalError
			tRepo.retFE = expError
		case "committed no prev":
			psm = nil
		case "committed with prev":
			expMan.Stats.ExcludedDirCount = psm.Stats.TotalDirectoryCount
			expMan.Stats.ExcludedFileCount = psm.Stats.TotalFileCount
			expMan.Stats.ExcludedTotalFileSize = psm.Stats.TotalFileSize
			expMan.Stats.NonCachedFiles = psm.Stats.NonCachedFiles + 1
			expMan.Stats.ReadErrors = int(expMan.Stats.NonCachedFiles)*1000 + 8
		}

		tB := time.Now()
		man, err := f.commitSnapshot(ctx, dm, psm)
		tA := time.Now()

		if expError == nil {
			assert.NoError(err)
			assert.NotNil(man)
			assert.True(man.EndTime.After(tB))
			assert.True(man.EndTime.Before(tA))
			man.EndTime = expMan.EndTime
			assert.Equal(expMan, man)
		} else {
			assert.Error(err)
			assert.Regexp(expError.Error(), err.Error())
		}
	}
}

// nolint:wsl,gocritic
func TestLinkPreviousSnapshot(t *testing.T) {
	assert := assert.New(t)

	ctx, th := newVolFsTestHarness(t)
	defer th.cleanup()

	expMD, expPPs, expEntries := generateTestMetaData()
	manifests, tSnap := th.generateTestManifests(expMD)

	for _, tc := range []string{
		"previous snapshot not found", "volSnapID mismatch", "linked",
	} {
		t.Logf("Case: %s", tc)

		tsp := &testSnapshotProcessor{}
		tsp.retLsM = manifests
		tsp.retSrEntry = &testDirEntry{retReadDirE: expEntries}

		f := th.fs()
		f.logger = log(ctx)
		f.sp = tsp
		f.setMetadata(metadata{}) // clear all

		var expError error
		prevVolSnapshotID := expMD.VolSnapID

		switch tc {
		case "previous snapshot not found":
			prevVolSnapshotID = "foo"
			expError = ErrSnapshotNotFound
		case "volSnapID mismatch":
			entries := fs.Entries{}
			for _, pp := range expPPs {
				entries = append(entries, &testFileEntry{name: pp[0] + "x"})
			}
			tsp.retSrEntry = &testDirEntry{retReadDirE: entries}
			expError = ErrInvalidSnapshot
		}

		dm, man, err := f.linkPreviousSnapshot(ctx, prevVolSnapshotID)

		if expError == nil {
			assert.NoError(err)
			expMan := manifests[len(manifests)-1]
			assert.Equal(expMan, man)
			assert.Equal(tSnap, man.EndTime)
			expDM := &dirMeta{
				name:    previousSnapshotDirName,
				oid:     expMan.RootEntry.ObjectID,
				summary: expMan.RootEntry.DirSummary,
			}
			assert.Equal(expDM, dm)
			assert.Equal(prevVolSnapshotID, f.prevVolumeSnapshotID)
			assert.Equal(expMD.BlockSzB, f.blockSzB)
			assert.Equal(expMD.DirSz, f.dirSz)
			assert.Equal(expMD.Depth, f.depth)
		} else {
			assert.Error(err)
			assert.Regexp(expError.Error(), err.Error())
			assert.Nil(dm)
			assert.Nil(man)
		}
	}
}

// nolint:wsl,gocritic
func TestSnapshotDescription(t *testing.T) {
	assert := assert.New(t)

	f := &Filesystem{}
	f.VolumeID = "volumeID"
	f.VolumeSnapshotID = "snapshotID"

	for _, tc := range []struct{ vs, exp string }{
		{exp: fmt.Sprintf(descriptionFormat, descriptionSeparator, f.VolumeID, descriptionSeparator, f.VolumeSnapshotID)},
		{vs: f.VolumeSnapshotID + "foo", exp: fmt.Sprintf(descriptionFormat, descriptionSeparator, f.VolumeID, descriptionSeparator, f.VolumeSnapshotID+"foo")},
	} {
		d := f.snapshotDescription(tc.vs)
		assert.Equal(tc.exp, d)
		m := &snapshot.Manifest{Description: d}
		v, vs := f.parseSnapshotDescription(m)
		assert.Equal(f.VolumeID, v)
		if tc.vs != "" {
			assert.Equal(tc.vs, vs)
		} else {
			assert.Equal(f.VolumeSnapshotID, vs)
		}
	}

	// failure case
	m := &snapshot.Manifest{Description: "invalid description"}
	v, vs := f.parseSnapshotDescription(m)
	assert.Equal("", v)
	assert.Equal("", vs)
}

type testSnapshotProcessor struct {
	inLsmR  repo.Repository
	inLsmS  *snapshot.SourceInfo
	retLsmM []manifest.ID
	retLsmE error

	inLsR  repo.Repository
	inLsS  snapshot.SourceInfo
	retLsM []*snapshot.Manifest
	retLsE error

	inLosR  repo.Repository
	inLosM  []manifest.ID
	retLosM []*snapshot.Manifest
	retLosE error

	retSrEntry fs.Entry

	inSsR   repo.Repository
	inSsM   *snapshot.Manifest
	retSsID manifest.ID
	retSsE  error
}

var _ snapshotProcessor = (*testSnapshotProcessor)(nil)

// nolint:gocritic
func (tsp *testSnapshotProcessor) ListSnapshotManifests(ctx context.Context, repo repo.Repository, src *snapshot.SourceInfo) ([]manifest.ID, error) {
	tsp.inLsmR = repo
	tsp.inLsmS = src

	if tsp.retLsmE != nil {
		sh := &snapshotHelper{} // call the real thing to check that it works

		m, err := sh.ListSnapshotManifests(ctx, repo, src)

		if err != nil || len(m) == 0 { // returns empty list
			return nil, tsp.retLsmE
		}

		panic("failed to fail")
	}

	return tsp.retLsmM, tsp.retLsmE
}

// nolint:gocritic
func (tsp *testSnapshotProcessor) ListSnapshots(ctx context.Context, repo repo.Repository, si snapshot.SourceInfo) ([]*snapshot.Manifest, error) {
	tsp.inLsR = repo
	tsp.inLsS = si

	if tsp.retLsE != nil {
		sh := &snapshotHelper{} // call the real thing to check that it works

		m, err := sh.ListSnapshots(ctx, repo, snapshot.SourceInfo{})

		if err != nil || len(m) == 0 { // appears to never fail!
			return nil, tsp.retLsE
		}

		panic("failed to fail")
	}

	return tsp.retLsM, tsp.retLsE
}

// nolint:gocritic
func (tsp *testSnapshotProcessor) LoadSnapshots(ctx context.Context, repo repo.Repository, manifestIDs []manifest.ID) ([]*snapshot.Manifest, error) {
	tsp.inLosR = repo
	tsp.inLosM = manifestIDs

	if tsp.retLosE != nil {
		sh := &snapshotHelper{} // call the real thing to check that it works

		m, err := sh.LoadSnapshots(ctx, repo, manifestIDs)

		if err != nil || len(m) == 0 { // behavior?
			return nil, tsp.retLosE
		}

		panic("failed to fail")
	}

	return tsp.retLosM, tsp.retLosE
}

// nolint:gocritic
func (tsp *testSnapshotProcessor) SnapshotRoot(rep repo.Repository, man *snapshot.Manifest) (fs.Entry, error) {
	if tsp.retSrEntry != nil {
		return tsp.retSrEntry, nil
	}

	sh := &snapshotHelper{} // call the real thing

	return sh.SnapshotRoot(rep, man)
}

// nolint:gocritic
func (tsp *testSnapshotProcessor) SaveSnapshot(ctx context.Context, rep repo.Repository, man *snapshot.Manifest) (manifest.ID, error) {
	tsp.inSsR = rep
	tsp.inSsM = man

	if tsp.retSsE != nil {
		sh := &snapshotHelper{}        // call the real thing to check that it works
		sh.SaveSnapshot(ctx, rep, man) // does not fail

		return "", tsp.retSsE
	}

	return tsp.retSsID, tsp.retSsE
}
