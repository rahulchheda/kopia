package volumefs

import (
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

// file metadata is saved in the following type
type fileMeta struct {
	name string
	oid  object.ID
}

func (fm *fileMeta) isMeta() bool {
	var md metadata

	isMeta, _, _ := md.isMetaFile(fm.name)

	return isMeta
}

func (fm *fileMeta) snapshotDirEntry() *snapshot.DirEntry {
	return &snapshot.DirEntry{
		Name:     fm.name,
		Type:     snapshot.EntryTypeFile,
		ObjectID: fm.oid,
	}
}
