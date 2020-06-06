package volumefs

import (
	"context"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"
)

// createRoot creates the root directory with references to current, previous and meta.
func (f *Filesystem) createRoot(ctx context.Context, curDm, prevRootDm *dirMeta) (*dirMeta, error) {
	rootDir := &dirMeta{
		name: "/",
	}

	rootDir.insertSubdir(curDm)

	if prevRootDm != nil {
		rootDir.insertSubdir(prevRootDm)
	}

	if err := f.createMetadataFiles(ctx, rootDir); err != nil {
		return nil, err
	}

	return rootDir, nil
}

// createTreeFromBlockMap creates an in-memory hierarchy from a block map and returns its root.
// Directories are not written to the repo.
func (f *Filesystem) createTreeFromBlockMap(bm BlockMap) (*dirMeta, BlockIterStats, error) {
	bis := BlockIterStats{}
	bis.initStats()

	bi := bm.Iterator()
	defer bi.Close()

	mapRootDm := &dirMeta{
		name: currentSnapshotDirName,
	}
	emptyBam := BlockAddressMapping{}

	for bam := bi.Next(); bam != emptyBam; bam = bi.Next() {
		pp, err := f.addrToPath(bam.BlockAddr)
		if err != nil {
			return nil, bis, err
		}

		bis.NumBlocks++
		if bis.MaxBlockAddr < bam.BlockAddr {
			bis.MaxBlockAddr = bam.BlockAddr
		}

		if bis.MinBlockAddr > bam.BlockAddr {
			bis.MinBlockAddr = bam.BlockAddr
		}

		fm := f.ensureFileInTree(mapRootDm, pp)
		fm.oid = bam.Oid
	}

	return mapRootDm, bis, nil
}

// lookupDir searches for a directory in a directory hierarchy
func (f *Filesystem) lookupDir(pDir *dirMeta, pp parsedPath) *dirMeta {
	for i := 0; i < len(pp); i++ {
		sdm := pDir.findSubdir(pp[i])
		if sdm == nil {
			f.logger.Debugf("dir not found [%s]", pp[:i])
			return nil
		}

		pDir = sdm
	}

	return pDir
}

// lookupFile searches for a file in the directory hierarchy
func (f *Filesystem) lookupFile(pDir *dirMeta, pp parsedPath) *fileMeta {
	fileIdx := len(pp) - 1
	if pDir = f.lookupDir(pDir, pp[:fileIdx]); pDir != nil {
		if fm := pDir.findFile(pp[fileIdx]); fm != nil {
			return fm
		}

		f.logger.Debugf("file not found [%s]", pp)
	}

	return nil
}

// ensureFile adds a new file to the specified directory hierarchy if it does not exist.
// It returns the fileMeta.
func (f *Filesystem) ensureFileInTree(pDir *dirMeta, pp parsedPath) *fileMeta {
	fileIdx := len(pp) - 1

	for i := 0; i < fileIdx; i++ {
		sdm := pDir.findSubdir(pp[i])
		if sdm == nil {
			sdm = &dirMeta{
				name: pp[i],
			}

			pDir.insertSubdir(sdm)
		}

		pDir = sdm
	}

	fm := pDir.findFile(pp[fileIdx])
	if fm == nil {
		fm = &fileMeta{
			name: pp[fileIdx],
		}

		pDir.insertFile(fm)

		return fm
	}

	return fm
}

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
