package volumefs

import (
	"context"
	"encoding/json"
	"io"
	"sort"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo/object"
	"github.com/kopia/kopia/snapshot"

	"github.com/pkg/errors"
)

// uploader aids in unit testing
type uploader interface {
	writeDirToRepo(ctx context.Context, pp parsedPath, dir *dirMeta, writeSubTree bool) error
	writeFileToRepo(ctx context.Context, pp parsedPath, src io.Reader, buf []byte) (object.ID, int64, error)
}

// writeFileToRepo writes a file to the repo.
// The src io.Reader is optional if the file is empty.
// The pp argument is for the description and diagnostic purposes.
func (f *Filesystem) writeFileToRepo(ctx context.Context, pp parsedPath, src io.Reader, buf []byte) (object.ID, int64, error) {
	writer := f.repo.NewObjectWriter(ctx, object.WriterOptions{
		Description: "FILE:" + pp.Last(),
		Compressor:  "", // @TODO Determine file compression policy
		AsyncWrites: 1,  // No sub-block concurrency
	})

	defer writer.Close() //nolint:errcheck // no error returned

	var (
		sz  int64
		err error
	)

	if src != nil {
		sz, err = io.CopyBuffer(writer, src, buf)
		if err != nil {
			f.logger.Debugf("[%s] io.CopyBuffer: %v", pp.String(), err)

			return "", 0, err
		}
	}

	oid, err := writer.Result()
	if err != nil {
		f.logger.Debugf("[%s] writer close: %v", pp.String(), err)
		return "", 0, err
	}

	f.logger.Debugf("uploaded file [%s] oid:%s size:%d", pp.String(), oid.String(), sz)

	return oid, sz, nil
}

// writeDirToRepo writes the directories of the subtree to the repo.
// The specified dir will be updated with the oid and summary.
// The pp argument is for debug only.
// The method will descend the subtree if writeSubTree is true.
func (f *Filesystem) writeDirToRepo(ctx context.Context, pp parsedPath, dir *dirMeta, writeSubTree bool) error {
	f.logger.Debugf("uploading dir [%s]", pp.String())

	// assemble a snapshot.DirManifest and validate that all entries have object ids
	dirManifest := &snapshot.DirManifest{
		StreamType: directoryStreamType,
		Summary: &fs.DirectorySummary{
			TotalDirCount: 1,
		},
	}
	summary := dirManifest.Summary
	summary.TotalDirCount = int64(len(dir.subdirs))

	dirManifest.Entries = make([]*snapshot.DirEntry, 0, len(dir.subdirs)+len(dir.files))

	// As per snapshot.upload, directories are first and then files, ordered by name.
	// Note: this is not the numerical sorting order which is not relevant with chain-of-trees.
	sort.Slice(dir.subdirs, func(i, j int) bool {
		return dir.subdirs[i].name < dir.subdirs[j].name
	})
	sort.Slice(dir.files, func(i, j int) bool {
		return dir.files[i].name < dir.files[j].name
	})

	for _, dm := range dir.subdirs {
		dpp := append(pp, dm.name) // nolint:gocritic

		if writeSubTree {
			err := f.writeDirToRepo(ctx, dpp, dm, writeSubTree)
			if err != nil {
				f.logger.Debugf("failed to upload dir [%s]: %v", dpp.String(), err)

				return err
			}
		} else if dm.oid == "" || dm.summary == nil {
			f.logger.Debugf("subdir [%s] missing oid or summary", dpp.String())

			return ErrInternalError
		}

		summary.TotalFileCount += dm.summary.TotalFileCount
		summary.TotalFileSize += dm.summary.TotalFileSize
		summary.TotalDirCount += dm.summary.TotalDirCount
		dirManifest.Entries = append(dirManifest.Entries, dm.snapshotDirEntry())
	}

	for _, fm := range dir.files {
		if fm.oid == "" {
			fpp := append(pp, fm.name) // nolint:gocritic

			f.logger.Debugf("file [%s] has no oid", fpp.String())

			return ErrInternalError
		}

		dirManifest.Entries = append(dirManifest.Entries, fm.snapshotDirEntry())

		if !fm.isMeta() {
			summary.TotalFileSize += int64(f.blockSzB) // all files have the same size
			summary.TotalFileCount++
		}
	}

	writer := f.repo.NewObjectWriter(ctx, object.WriterOptions{
		Description: "DIR:" + dir.name,
		Prefix:      "k",
	})

	defer writer.Close() //nolint:errcheck

	if err := json.NewEncoder(writer).Encode(dirManifest); err != nil {
		f.logger.Debugf("dir [%s] encode error %v", pp.String(), err)

		return errors.Wrap(err, "unable to encode directory JSON")
	}

	oid, err := writer.Result()
	if err != nil {
		f.logger.Debugf("dir [%s] write error %v", pp.String(), err)

		return err
	}

	dir.summary = summary
	dir.oid = oid
	f.logger.Debugf("uploaded dir [%s] oid:%s %#v", pp.String(), oid, *summary)

	return nil
}
