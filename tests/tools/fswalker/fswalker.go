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

var _ checker.Comparer = &WalkChecker{}

// WalkChecker is a checker.Comparer that utilizes the fswalker
// libraries to perform the data consistency check.
type WalkChecker struct {
	GlobalFilterMatchers []string
}

// NewChecker instantiates a new WalkChecker and returns its pointer
func NewChecker() *WalkChecker {
	return &WalkChecker{
		GlobalFilterMatchers: []string{
			"ctime:",
			"atime:",
			"mtime:",
		},
	}
}

// Gather meets the checker.Comparer interface. It performs a fswalker Walk
// and returns the resulting Walk as a protobuf Marshalled buffer.
func (chk *WalkChecker) Gather(ctx context.Context, path string) ([]byte, error) {
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
func (chk *WalkChecker) Compare(ctx context.Context, path string, data []byte, reportOut io.Writer) error {
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

	// rptr := &fswalker.Reporter{}
	// rptr.PrintDiffSummary(reportOut, report)
	// rptr.PrintReportSummary(reportOut, report)
	// rptr.PrintRuleSummary(reportOut, report)

	chk.filterReportDiffs(report)

	// rptr.PrintDiffSummary(reportOut, report)
	// rptr.PrintReportSummary(reportOut, report)
	// rptr.PrintRuleSummary(reportOut, report)

	err = chk.validateReport(report)
	if err != nil {
		b, marshalErr := json.MarshalIndent(report, "", "   ")
		if marshalErr != nil {
			_, err := reportOut.Write([]byte(marshalErr.Error()))
			if err != nil {
				return fmt.Errorf("Error while writing marshal error %v", err.Error())
			}
			return marshalErr
		}
		reportOut.Write(b)

		return err
	}

	return nil
}

func (chk *WalkChecker) filterReportDiffs(report *fswalker.Report) {
	var newModList []fswalker.ActionData

	for _, mod := range report.Modified {
		var newDiffItemList []string

		diffItems := strings.Split(mod.Diff, "\n")

	DiffItemLoop:
		for _, diffItem := range diffItems {
			for _, filterStr := range chk.GlobalFilterMatchers {
				if strings.Contains(diffItem, filterStr) {
					fmt.Printf("FILTERING %s due to filtered prefix %q\n", diffItem, filterStr)
					continue DiffItemLoop
				}
			}

			// Filter the rename of the root directory
			if isRootDirectoryRename(diffItem, mod) {
				fmt.Println("Filtering", diffItem, "due to root directory rename")
				continue DiffItemLoop
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

func (chk *WalkChecker) validateReport(report *fswalker.Report) error {
	if len(report.Modified) > 0 {
		return errors.New("Files were modified")
	}

	if len(report.Added) > 0 {
		return errors.New("Files were added")
	}

	if len(report.Deleted) > 0 {
		return errors.New("Files were deleted")
	}

	if len(report.Errors) > 0 {
		return errors.New("Errors were thrown in the walk")
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
