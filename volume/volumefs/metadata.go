package volumefs

import (
	"context"
	"fmt"
	"regexp"

	"github.com/kopia/kopia/fs"
)

// metadata file names match the following patterns
var reMetaFile = regexp.MustCompile(`meta:(\w+):(.*)`)
var metaFmtX = "meta:%s:%x"

const (
	metaBlockSzB = "blockSzB"
	metaDirSz    = "dirSz"
	metaDepth    = "depth"
)

func (f *Filesystem) isMetaFile(name string) (isMeta bool, metaName, metaValue string) {
	matches := reMetaFile.FindStringSubmatch(name)
	if len(matches) != 3 { // nolint:gomnd
		return false, "", ""
	}

	return true, matches[1], matches[2]
}

func (f *Filesystem) recoverMetadataFromFilename(name string) {
	isMeta, name, value := f.isMetaFile(name)
	if !isMeta {
		return
	}

	i64 := int64(0)
	fmt.Sscanf(value, "%x", &i64)

	switch name {
	case metaBlockSzB:
		if i64 > 0 {
			f.logger.Debugf("recovered blockSzB=0x%x", i64)
			f.blockSzB = i64
		}
	case metaDirSz:
		if i64 > 0 {
			f.logger.Debugf("recovered dirSz=%d", i64)
			f.dirSz = i64
		}
	case metaDepth:
		if i64 > 0 {
			f.logger.Debugf("recovered metadata depth=%d", i64)
			f.depth = int(i64)
		}
	}
}

func (f *Filesystem) recoverMetadataFromRootEntry(ctx context.Context, rootEntry fs.Directory) error {
	entries, err := rootEntry.Readdir(ctx)
	if err != nil {
		f.logger.Errorf("failed to recover metadata: %v", err)
		return err
	}

	for _, entry := range entries {
		if fe, ok := entry.(fs.File); ok {
			f.recoverMetadataFromFilename(fe.Name())
		}
	}

	// recompute
	f.layoutProperties.initLayoutProperties(f.blockSzB, f.dirSz, f.depth)

	return nil
}

// setMetadata sets one-time filesystem metadata
func (f *Filesystem) setMetadata(ctx context.Context) {
	fn := fmt.Sprintf(metaFmtX, metaBlockSzB, f.blockSzB)
	pp := parsedPath([]string{fn})
	f.ensureFile(ctx, pp)

	fn = fmt.Sprintf(metaFmtX, metaDirSz, f.dirSz)
	pp = parsedPath([]string{fn})
	f.ensureFile(ctx, pp)

	fn = fmt.Sprintf(metaFmtX, metaDepth, f.depth)
	pp = parsedPath([]string{fn})
	f.ensureFile(ctx, pp)
}
