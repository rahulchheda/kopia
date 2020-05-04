package volumefs

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/kopia/kopia/fs"
)

// file metadata is saved in the following type
type fileMeta struct {
	name  string
	mTime time.Time
}

func (fm *fileMeta) isMeta(f *Filesystem) bool {
	isMeta, _, _ := f.isMetaFile(fm.name)
	return isMeta
}

func (fm *fileMeta) fsEntry(f *Filesystem) fs.File {
	return &fileEntry{
		f: f,
		m: fm,
	}
}

func (fm *fileMeta) blockAddr() int64 {
	var addr int64
	fmt.Sscanf(fm.name, "%x", &addr)
	return addr
}

// fileEntry implements fs.File
type fileEntry struct {
	f *Filesystem
	m *fileMeta
}

var _ fs.File = (*fileEntry)(nil)

func (e *fileEntry) IsDir() bool {
	return false
}

func (e *fileEntry) Name() string {
	return e.m.name
}

func (e *fileEntry) Mode() os.FileMode {
	return 0555
}

func (e *fileEntry) Size() int64 {
	if e.m.isMeta(e.f) {
		return 0
	}
	return e.f.blockSzB
}

func (e *fileEntry) Sys() interface{} {
	return nil
}

func (e *fileEntry) ModTime() time.Time {
	return e.m.mTime
}

func (e *fileEntry) Owner() fs.OwnerInfo {
	return fs.OwnerInfo{}
}

func (e *fileEntry) Open(ctx context.Context) (fs.Reader, error) {
	log(ctx).Debugf("Open(%s)", e.m.name)
	return newFileReader(ctx, e)
}
