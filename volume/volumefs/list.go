package volumefs

import (
	"context"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
)

// ListSnapshots returns a list of volume snapshots.
// volumeID is optional - if unspecified all volume snapshots are returned.
func ListSnapshots(ctx context.Context, rep repo.Repository, volumeID string) ([]*Snapshot, error) {
	if rep == nil {
		return nil, ErrInvalidArgs
	}

	f := &Filesystem{}
	f.logger = log(ctx)
	f.Repo = rep
	f.VolumeID = volumeID
	f.sp = &snapshotHelper{}
	f.repo = rep

	return f.ListSnapshots(ctx)
}

// ListSnapshots returns the snapshots of the volume associated with the filesystem.
func (f *Filesystem) ListSnapshots(ctx context.Context) ([]*Snapshot, error) {
	var src *snapshot.SourceInfo

	if f.VolumeID != "" { // possible if invoked via ListSnapshots only
		si := f.SourceInfo()
		src = &si
	}

	mi, err := f.sp.ListSnapshotManifests(ctx, f.Repo, src)
	if err != nil {
		return nil, err
	}

	sm, err := f.sp.LoadSnapshots(ctx, f.Repo, mi)
	if err != nil {
		return nil, err
	}

	res := make([]*Snapshot, 0, len(sm))

	for _, m := range sm {
		vID, vsID := f.parseSnapshotDescription(m)
		if vID == "" || vsID == "" {
			continue
		}

		s := &Snapshot{
			VolumeID:         vID,
			VolumeSnapshotID: vsID,
			Manifest:         m,
		}
		s.Analyze(m)
		res = append(res, s)
	}

	return res, nil
}
