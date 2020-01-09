package gc

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/repo/content"
	"github.com/kopia/kopia/repo/manifest"
)

// ContentPrefix is the prefix used for GC related content
const ContentPrefix content.ID = "g"

const markManifestType = "gc-mark"

// MarkManifest represents information about a single GC mark run
type MarkManifest struct {
	ID manifest.ID `json:"-"`

	StartTime time.Time `json:"startTime"`
	EndTime   time.Time `json:"endTime"`

	DetailsID content.ID `json:"detailsID"`
}

// MarkDetails contains metadata about a GC mark phase, which is later used
// by the GC delete phase
type MarkDetails struct {
	// Snapshots that were visible to a GC run.
	Snapshots []manifest.ID `json:"liveSnapshots,omitempty"`
	// Set of contents marked for deletion by the GC mark phase.
	MarkedContent []content.ID `json:"markedContent,omitempty"`
}

func markContentsDeleted(ctx context.Context, rep *repo.DirectRepository, snaps []manifest.ID, toDelete []content.ID) error {
	if err := markContentAndCreateManifest(ctx, rep, snaps, toDelete); err != nil {
		return err
	}

	return rep.Content.Flush(ctx)
}

func markContentAndCreateManifest(ctx context.Context, rep *repo.DirectRepository, snaps []manifest.ID, toDelete []content.ID) error {
	// create mark details manifest, get back an id
	m := MarkManifest{
		StartTime: rep.Time().UTC(),
	}

	did, err := writeMarkDetails(ctx, rep, snaps, toDelete)
	if err != nil {
		return err
	}

	m.DetailsID = did

	// disable flushing to ensure that the mark manifest and the deleted entries
	// are in a single index pack. May want to do this earlier to include the
	// details manifest
	rep.Content.DisableIndexFlush(ctx)
	defer rep.Content.EnableIndexFlush(ctx)

	// may want to have a batch call for this.
	for _, cid := range toDelete {
		if err2 := rep.Content.DeleteContent(ctx, cid); err2 != nil {
			return err2
		}
	}

	m.EndTime = rep.Time().UTC()

	if _, err = rep.Manifests.Put(ctx, markManifestLabels(), m); err != nil {
		return err
	}

	return rep.Manifests.Flush(ctx)
}

func writeMarkDetails(ctx context.Context, rep *repo.DirectRepository, snaps []manifest.ID, toDelete []content.ID) (content.ID, error) {
	content.SortIDs(toDelete)

	details := MarkDetails{
		Snapshots:     snaps,
		MarkedContent: toDelete,
	}

	b, err := json.Marshal(details)
	if err != nil {
		return "", nil
	}

	return rep.Content.WriteContent(ctx, b, ContentPrefix)
}

func markManifestLabels() map[string]string {
	return map[string]string{
		manifest.TypeLabelKey: markManifestType,
	}
}

func listMarkManifestsMeta(ctx context.Context, mgr *manifest.Manager) ([]*manifest.EntryMetadata, error) {
	return mgr.Find(ctx, markManifestLabels())
}

func listMarkManifestsOlderThan(ctx context.Context, rep *repo.DirectRepository, minAge time.Duration) ([]MarkManifest, error) {
	mgr := rep.Manifests
	ageDate := rep.Time().Add(-minAge)

	meta, err := listMarkManifestsMeta(ctx, mgr)
	if err != nil {
		return nil, err
	}

	var mms []MarkManifest

	for _, m := range meta {
		if m.ModTime.After(ageDate) {
			continue // ignore recent entries
		}

		var man MarkManifest
		if _, err := mgr.Get(ctx, m.ID, &man); err != nil {
			if err == manifest.ErrNotFound {
				continue
			}

			return nil, err
		}

		if man.EndTime.After(ageDate) {
			continue
		}

		man.ID = m.ID
		mms = append(mms, man)
	}

	return mms, nil
}

func getMarkDetails(ctx context.Context, cm *content.Manager, detailsID content.ID) (MarkDetails, error) {
	var details MarkDetails

	b, err := cm.GetContent(ctx, detailsID)
	if err != nil {
		return details, err
	}

	err = json.Unmarshal(b, &details)

	return details, err
}

type empty struct{}

type manifestIDSet map[manifest.ID]empty

func toManifestIDSet(ids []manifest.ID) manifestIDSet {
	s := make(manifestIDSet, len(ids))

	return s.addAll(ids...)
}

func (s manifestIDSet) addAll(ids ...manifest.ID) manifestIDSet {
	for _, id := range ids {
		s.put(id)
	}

	return s
}

func (s manifestIDSet) put(id manifest.ID) {
	s[id] = empty{}
}

func (s manifestIDSet) has(id manifest.ID) bool {
	_, ok := s[id]

	return ok
}

// add adds the elements in b to s. It is equivalent to `s = s U b`
func (s manifestIDSet) add(b manifestIDSet) manifestIDSet {
	for id := range b {
		s.put(id)
	}

	return s
}

// subtract removes the elements in b from s. It is equivalent to `s = s - b`
func (s manifestIDSet) subtract(b manifestIDSet) manifestIDSet {
	for id := range b {
		delete(s, id)
	}

	return s
}

// diffS is a convenience helper that is the "slice-version" of diff.
func (s manifestIDSet) diffS(ids []manifest.ID) manifestIDSet {
	return s.diff(toManifestIDSet(ids))
}

// diff returns a new set `d = s - b` without modifying `s`.
func (s manifestIDSet) diff(b manifestIDSet) manifestIDSet {
	d := manifestIDSet{}

	for id := range s {
		if !b.has(id) {
			d.put(id)
		}
	}

	return d
}

// return a slice with the contents of s
func (s manifestIDSet) slice() []manifest.ID {
	ids := make([]manifest.ID, 0, len(s))

	for id := range s {
		ids = append(ids, id)
	}

	return ids
}
