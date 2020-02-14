package gc

import (
	"context"
	"time"

	"github.com/pkg/errors"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/snapshot"
)

// RepairAndDiscard ensures that any content referenced snapshots is preserved
// in case it had been marked as a candidate for deletion by a previous GC
// mark process. Contents that have not been reused become elegible for purging
func RepairAndDiscard(ctx context.Context, rep *repo.DirectRepository, minGCMarkAge time.Duration) error {
	ms, err := listMarkManifestsOlderThan(ctx, rep, minGCMarkAge)
	if err != nil {
		return err
	}

	snaps, err := snapshot.ListSnapshotManifests(ctx, rep, nil)
	if err != nil {
		return err
	}

	snapIDs := toManifestIDSet(snaps)
	checkedSnaps := manifestIDSet{}

	for _, m := range ms {
		details, err := getMarkDetails(ctx, rep.Content, m.DetailsID)
		if err != nil {
			return err
		}

		// check snaps that have not already been checked and that were not
		// observed by this GC mark process. That is,
		// `snapsToCheck = snapIDs - GC mark snaps - checkedSnaps`
		toCheck := snapIDs.diffS(details.Snapshots).subtract(checkedSnaps)
		if err := repairSnapshots(ctx, rep, toCheck); err != nil {
			return err
		}

		checkedSnaps.add(toCheck)
	}

	// Persist any undeletion index entries
	if err := rep.Content.Flush(ctx); err != nil {
		return err
	}

	// Collect contents ids to be discarded after all the mark manifests have
	// been checked, and the corresponding snapshots have been repaired
	discardIDs := content.IDSet{}

	for _, m := range ms {
		details, err := getMarkDetails(ctx, rep.Content, m.DetailsID)
		if err != nil {
			return err
		}

		// check content deleted by this mark phase and delete what has not been
		// reused
		for _, cid := range details.MarkedContent {
			info, err := rep.Content.ContentInfo(ctx, cid)
			if err != nil {
				return err
			}

			if !info.Deleted {
				log(ctx).Infof("found re-used content, not deleting: %v", cid)
				continue
			}

			log(ctx).Debugf("discarding content: %v", cid)
			discardIDs.Add(cid)
		}

		// Remove gc details and gc mark manifest
		if err := rep.Content.DeleteContent(ctx, m.DetailsID); err != nil {
			return err
		}

		if err := rep.Manifests.Delete(ctx, m.ID); err != nil {
			return err
		}
	}

	return rep.Content.DiscardIndexEntries(ctx, discardIDs, content.CompactOptions{})
}

func repairSnapshots(ctx context.Context, rep *repo.DirectRepository, snaps manifestIDSet) error {
	return walkSnapshots(ctx, rep, snaps.slice(), func(entry fs.Entry) error {
		oid := oidOf(entry)
		contentIDs, err := rep.Objects.VerifyObject(ctx, oid)

		if err != nil {
			return errors.Wrapf(err, "error verifying %v", oid)
		}

		for _, cid := range contentIDs {
			info, err := rep.Content.ContentInfo(ctx, cid)
			if err != nil {
				return err
			}

			if info.Deleted {
				if err := rep.Content.UndeleteContent(ctx, cid); err != nil {
					return errors.Wrapf(err, "could not undelete content %v", cid)
				}

				log(ctx).Infof("un-deleted content: %v", cid)
			}
		}

		return nil
	})
}
