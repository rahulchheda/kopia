package volumefs

import (
	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

// directory metadata is saved in the following type
type dirMeta struct {
	name    string
	oid     object.ID
	subdirs []*dirMeta
	files   []*fileMeta
	summary *fs.DirectorySummary
}

func (dm *dirMeta) findSubdir(name string) *dirMeta {
	for _, sdm := range dm.subdirs {
		if sdm.name == name {
			return sdm
		}
	}

	return nil
}

func (dm *dirMeta) insertSubdir(sdm *dirMeta) {
	if dm.subdirs == nil {
		dm.subdirs = make([]*dirMeta, 0)
	}

	dm.subdirs = append(dm.subdirs, sdm)
}

func (dm *dirMeta) findFile(name string) *fileMeta {
	for _, fm := range dm.files {
		if fm.name == name {
			return fm
		}
	}

	return nil
}

func (dm *dirMeta) insertFile(fm *fileMeta) {
	if dm.files == nil {
		dm.files = make([]*fileMeta, 0)
	}

	dm.files = append(dm.files, fm)
}

func (dm *dirMeta) snapshotDirEntry() *snapshot.DirEntry {
	return &snapshot.DirEntry{
		Name:       dm.name,
		Type:       snapshot.EntryTypeDirectory,
		ObjectID:   dm.oid,
		DirSummary: dm.summary,
	}
}
