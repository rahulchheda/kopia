// Package fswalker provides the checker.Comparer interface using FSWalker
// walker and reporter.
package fswalker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/golang/protobuf/proto"
	"github.com/google/fswalker"
	fspb "github.com/google/fswalker/proto/fswalker"

	"github.com/kopia/kopia/tests/robustness/checker"
	"github.com/kopia/kopia/tests/tools/fswalker/reporter"
	"github.com/kopia/kopia/tests/tools/fswalker/walker"
)

var _ checker.Comparer = &WalkCompare{}

// WalkCompare is a checker.Comparer that utilizes the fswalker
// libraries to perform the data consistency check.
type WalkCompare struct {
	GlobalFilterFuncs []func(string, fswalker.ActionData) bool
}

// NewWalkCompare instantiates a new WalkCompare and returns its pointer
func NewWalkCompare() *WalkCompare {
	return &WalkCompare{
		GlobalFilterFuncs: []func(string, fswalker.ActionData) bool{
			filterFileTimeDiffs,
			isRootDirectoryRename,
			dirSizeMightBeOffByBlockSizeMultiple,
		},
	}
}

// Gather meets the checker.Comparer interface. It performs a fswalker Walk
// and returns the resulting Walk as a protobuf Marshaled buffer.
func (chk *WalkCompare) Gather(ctx context.Context, path string) ([]byte, error) {
	walkData, err := walker.WalkPathHash(ctx, path)
	if err != nil {
		return nil, err
	}

	err = rerootWalkDataPaths(walkData, path)
	if err != nil {
		return nil, err
	}

	// Store the walk data along with the snapshot ID
	b, err := proto.Marshal(walkData)
	if err != nil {
		return nil, err
	}

	return b, nil
}

// Compare meets the checker.Comparer interface. It performs a fswalker Walk
// on the provided file path, unmarshals the comparison data as a fswalker Walk,
// and generates a fswalker report comparing the two Walks. If there are any differences
// an error is returned, and the full report will be written to the provided writer
// as JSON.
func (chk *WalkCompare) Compare(ctx context.Context, path string, data []byte, reportOut io.Writer) error {
	beforeWalk := &fspb.Walk{}

	err := proto.Unmarshal(data, beforeWalk)
	if err != nil {
		return err
	}

	afterWalk, err := walker.WalkPathHash(ctx, path)
	if err != nil {
		return err
	}

	err = rerootWalkDataPaths(afterWalk, path)
	if err != nil {
		return err
	}

	report, err := reporter.Report(ctx, &fspb.ReportConfig{}, beforeWalk, afterWalk)
	if err != nil {
		return err
	}

	chk.filterReportDiffs(report)

	err = validateReport(report)
	if err != nil && reportOut != nil {
		printReportSummary(report, reportOut)

		b, marshalErr := json.MarshalIndent(report, "", "   ")
		if marshalErr != nil {
			_, reportErr := reportOut.Write([]byte(marshalErr.Error()))
			if reportErr != nil {
				return fmt.Errorf("error while writing marshal error (%v): %v", marshalErr.Error(), reportErr.Error())
			}

			return marshalErr
		}

		if _, wrErr := reportOut.Write(b); wrErr != nil {
			return err
		}

		return err
	}

	return nil
}

func printReportSummary(report *fswalker.Report, reportOut io.Writer) {
	rptr := &fswalker.Reporter{}
	rptr.PrintDiffSummary(reportOut, report)
	rptr.PrintReportSummary(reportOut, report)
	rptr.PrintRuleSummary(reportOut, report)
}

func (chk *WalkCompare) filterReportDiffs(report *fswalker.Report) {
	var newModList []fswalker.ActionData

	for _, mod := range report.Modified {
		var newDiffItemList []string

		diffItems := strings.Split(mod.Diff, "\n")

	DiffItemLoop:
		for _, diffItem := range diffItems {
			for _, filterFunc := range chk.GlobalFilterFuncs {
				if filterFunc(diffItem, mod) {
					continue DiffItemLoop
				}
			}

			newDiffItemList = append(newDiffItemList, diffItem)
		}

		if len(newDiffItemList) > 0 {
			fmt.Println("Not Filtering", newDiffItemList)
			mod.Diff = strings.Join(newDiffItemList, "\n")
			newModList = append(newModList, mod)
		}
	}

	report.Modified = newModList
}

func isRootDirectoryRename(diffItem string, mod fswalker.ActionData) bool {
	if !strings.HasPrefix(diffItem, "name: ") {
		return false
	}

	return mod.Before.Info.IsDir && filepath.Dir(mod.Before.Path) == "."
}

func dirSizeMightBeOffByBlockSizeMultiple(str string, mod fswalker.ActionData) bool {
	if !mod.Before.Info.IsDir {
		return false
	}

	if !strings.Contains(str, "size: ") {
		return false
	}

	const blockSize = 4096

	return (mod.Before.Stat.Size-mod.After.Stat.Size)%blockSize == 0
}

func filterFileTimeDiffs(str string, mod fswalker.ActionData) bool {
	return strings.Contains(str, "ctime:") || strings.Contains(str, "atime:") || strings.Contains(str, "mtime:")
}

func validateReport(report *fswalker.Report) error {
	if len(report.Modified) > 0 {
		return errors.New("files were modified")
	}

	if len(report.Added) > 0 {
		return errors.New("files were added")
	}

	if len(report.Deleted) > 0 {
		return errors.New("files were deleted")
	}

	if len(report.Errors) > 0 {
		return errors.New("errors were thrown in the walk")
	}

	return nil
}

func rerootWalkDataPaths(walk *fspb.Walk, newRoot string) error {
	for _, f := range walk.File {
		var err error

		f.Path, err = filepath.Rel(newRoot, f.Path)
		if err != nil {
			return err
		}
	}

	return nil
}
