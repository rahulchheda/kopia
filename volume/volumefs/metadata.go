package volumefs

import (
	"context"
	"fmt"
	"reflect"
	"regexp"
	"strconv"

	"github.com/kopia/kopia/fs"
)

// metadata file names match the following patterns
var reMetaFile = regexp.MustCompile(`meta:(\w+):(.*)`)

const (
	metaFmtX = "meta:%s:%x"
	metaFmtS = "meta:%s:%s"
)

// metadata collates the filesystem metadata.
func (f *Filesystem) metadata() metadata {
	return metadata{
		BlockSzB:      f.blockSzB,
		DirSz:         f.dirSz,
		Depth:         f.depth,
		VolSnapID:     f.VolumeSnapshotID,
		VolPrevSnapID: f.prevVolumeSnapshotID,
	}
}

func (f *Filesystem) setMetadata(md metadata) {
	f.blockSzB = md.BlockSzB
	f.dirSz = md.DirSz
	f.depth = md.Depth
	f.VolumeSnapshotID = md.VolSnapID
	f.prevVolumeSnapshotID = md.VolPrevSnapID
}

// recoverMetadataFromDirEntry scans a directory and parses metadata file names.
// It does not modify the filesystem.
func (f *Filesystem) recoverMetadataFromDirEntry(ctx context.Context, dir fs.Directory) (metadata, error) {
	var md metadata

	entries, err := dir.Readdir(ctx)
	if err != nil {
		f.logger.Debugf("failed to recover metadata: %v", err)
		return md, err
	}

	md.recoverMetadataFromEntries(entries)

	return md, nil
}

// createMetadataFiles creates files for the filesystem metadata in the specified directory.
func (f *Filesystem) createMetadataFiles(ctx context.Context, dir *dirMeta) error {
	md := f.metadata()
	for _, pp := range md.metadataFiles() {
		f.ensureFileInTree(dir, pp)

		oid, _, err := f.up.writeFileToRepo(ctx, pp, nil, nil)
		if err != nil {
			return err
		}

		fm := f.lookupFile(dir, pp)
		fm.oid = oid
	}

	return nil
}

// metadata contains values that are recorded in the names of special files in a directory.
type metadata struct {
	BlockSzB      int
	DirSz         int
	Depth         int
	VolSnapID     string
	VolPrevSnapID string
}

func (md *metadata) isMetaFile(name string) (isMeta bool, metaName, metaValue string) {
	matches := reMetaFile.FindStringSubmatch(name)
	if len(matches) != 3 { // nolint:gomnd
		return false, "", ""
	}

	return true, matches[1], matches[2]
}

func (md *metadata) recoverMetadataFromFilename(name string) {
	isMeta, name, value := md.isMetaFile(name)
	if !isMeta {
		return
	}

	vV := reflect.ValueOf(md)
	vV = vV.Elem()
	vT := vV.Type()

	for i := 0; i < vT.NumField(); i++ {
		if vT.Field(i).Name == name {
			vFV := vV.Field(i)
			switch vFV.Kind() {
			case reflect.Int:
				fallthrough // nolint:gocritic
			case reflect.Int64:
				var i64 int64
				i64, _ = strconv.ParseInt(value, 16, 64)
				vFV.SetInt(i64)
			default:
				vFV.SetString(value)
			}

			return
		}
	}
}

func (md *metadata) recoverMetadataFromEntries(entries fs.Entries) {
	for _, entry := range entries {
		if fe, ok := entry.(fs.File); ok {
			md.recoverMetadataFromFilename(fe.Name())
		}
	}
}

// metadataFiles returns path names for all non-zero metadata values.
func (md *metadata) metadataFiles() []parsedPath {
	ret := []parsedPath{}

	vV := reflect.ValueOf(md)
	vV = vV.Elem()
	vT := vV.Type()

	for i := 0; i < vT.NumField(); i++ {
		vFV := vV.Field(i)
		switch vFV.Kind() {
		case reflect.Int:
			fallthrough // nolint:gocritic
		case reflect.Int64:
			if vFV.Int() != 0 {
				s := fmt.Sprintf(metaFmtX, vV.Type().Field(i).Name, vFV.Int())
				ret = append(ret, parsedPath([]string{s}))
			}
		case reflect.String:
			if vFV.String() != "" {
				s := fmt.Sprintf(metaFmtS, vV.Type().Field(i).Name, vFV.String())
				ret = append(ret, parsedPath([]string{s}))
			}
		}
	}

	return ret
}
