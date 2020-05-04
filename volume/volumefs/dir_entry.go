package volumefs

import (
	"context"
	"os"
	"time"

	"github.com/kopia/kopia/fs"
)

// directory metadata is saved in the following type
type dirMeta struct {
	name    string
	mTime   time.Time
	subdirs []*dirMeta
	files   []*fileMeta
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
	dm.mTime = sdm.mTime
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
	dm.mTime = fm.mTime
}

// TBD move to test
func (dm *dirMeta) descend(cb func(*dirMeta, parsedPath, interface{})) {
	dm.postOrderWalk(nil, cb)
}

func (dm *dirMeta) postOrderWalk(ppp parsedPath, cb func(*dirMeta, parsedPath, interface{})) {
	pp := parsedPath{}
	if ppp != nil {
		pp = append(ppp, dm.name)
	}
	cb(dm, pp, true)
	for _, sdm := range dm.subdirs {
		sdm.postOrderWalk(pp, cb)
	}
	for _, fm := range dm.files {
		pp = append(pp, fm.name)
		cb(dm, pp, fm)
		pp = pp[:len(pp)-1]
	}
	cb(dm, pp, false)
}

func (dm *dirMeta) fsEntry(f *Filesystem) fs.Directory {
	return &dirEntry{
		f: f,
		m: dm,
	}
}

// dirEntry implements fs.Directory
type dirEntry struct {
	f *Filesystem
	m *dirMeta
}

var _ fs.Directory = (*dirEntry)(nil)

func (e *dirEntry) IsDir() bool {
	return true
}

func (e *dirEntry) Name() string {
	return e.m.name
}

func (e *dirEntry) Mode() os.FileMode {
	return 0555 | os.ModeDir
}

func (e *dirEntry) Size() int64 {
	// TBD: not sure if this is the correct thing to return
	return int64(len(e.m.subdirs) + len(e.m.files))
}

func (e *dirEntry) Sys() interface{} {
	return nil
}

func (e *dirEntry) ModTime() time.Time {
	return e.m.mTime
}

func (e *dirEntry) Owner() fs.OwnerInfo {
	return fs.OwnerInfo{}
}

func (e *dirEntry) Summary() *fs.DirectorySummary {
	return nil
}

func (e *dirEntry) Child(ctx context.Context, name string) (fs.Entry, error) {
	return fs.ReadDirAndFindChild(ctx, e, name)
}

func (e *dirEntry) Readdir(ctx context.Context) (fs.Entries, error) {
	// Mixed entries only found in the top directory where meta files exist; no name conflicts.
	ret := make(fs.Entries, 0, int(len(e.m.subdirs)+len(e.m.files)))
	for _, dm := range e.m.subdirs {
		ret = append(ret, dm.fsEntry(e.f))
	}
	for _, fm := range e.m.files {
		ret = append(ret, fm.fsEntry(e.f))
	}
	ret.Sort()
	log(ctx).Debugf("Readdir(%s): %dd+%df=%d", e.m.name, len(e.m.subdirs), len(e.m.files), len(ret))
	return ret, nil
}
