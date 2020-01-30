package fswalker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"

	"github.com/google/fswalker"
	fspb "github.com/google/fswalker/proto/fswalker"
	"github.com/kopia/kopia/tests/robustness/snapif"
	"github.com/kopia/kopia/tests/tools/fswalker/reporter"
	"github.com/kopia/kopia/tests/tools/fswalker/walker"
)

type Checker struct {
	RestoreDir string
	snap       snapif.Snapshotter

	GlobalFilterMatchers []string
	snapStore            map[string]*fspb.Walk
}

func NewChecker(snap snapif.Snapshotter) (*Checker, error) {
	restoreDir, err := ioutil.TempDir("", "restore-data-")
	if err != nil {
		return nil, err
	}

	return &Checker{
		RestoreDir: restoreDir,
		snap:       snap,
		GlobalFilterMatchers: []string{
			"ctime:",
			"atime:",
			"mtime:",
		},
		snapStore: make(map[string]*fspb.Walk),
	}, nil
}

func (chk *Checker) Cleanup() {
	if chk.RestoreDir != "" {
		os.RemoveAll(chk.RestoreDir)
	}
}

func (chk *Checker) TakeSnapshot(ctx context.Context, sourceDir string) (snapID string, err error) {
	walkData, err := walker.WalkPathHash(ctx, sourceDir)
	if err != nil {
		return "", err
	}

	err = rerootWalkDataPaths(walkData, sourceDir)
	if err != nil {
		return "", err
	}

	snapID, err = chk.snap.TakeSnapshot(sourceDir)
	if err != nil {
		return snapID, err
	}

	// Store the walk data along with the snapshot ID
	// TODO
	chk.snapStore[snapID] = walkData

	return snapID, nil
}

func (chk *Checker) RestoreSnapshot(ctx context.Context, snapID string, reportOut io.Writer) error {
	// Lookup walk data by snapshot ID
	// TODO
	beforeWalk, ok := chk.snapStore[snapID]
	if !ok {
		return fmt.Errorf("Could not find snapshot with ID %v", snapID)
	}

	// Make an independent directory for the restore
	restoreSubDir, err := ioutil.TempDir(chk.RestoreDir, fmt.Sprintf("restore-snap-%v", snapID))
	if err != nil {
		return err
	}

	defer os.RemoveAll(restoreSubDir) //nolint:errcheck

	err = chk.snap.RestoreSnapshot(snapID, restoreSubDir)
	if err != nil {
		return err
	}

	afterWalk, err := walker.WalkPathHash(ctx, restoreSubDir)
	if err != nil {
		return err
	}

	err = rerootWalkDataPaths(afterWalk, restoreSubDir)
	if err != nil {
		return err
	}

	report, err := reporter.Report(ctx, &fspb.ReportConfig{}, beforeWalk, afterWalk)
	if err != nil {
		return err
	}

	rptr := &fswalker.Reporter{}
	rptr.PrintDiffSummary(os.Stdout, report)
	rptr.PrintReportSummary(os.Stdout, report)
	rptr.PrintRuleSummary(os.Stdout, report)

	chk.filterReportDiffs(report)

	rptr.PrintDiffSummary(os.Stdout, report)
	rptr.PrintReportSummary(os.Stdout, report)
	rptr.PrintRuleSummary(os.Stdout, report)

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

func (chk *Checker) filterReportDiffs(report *fswalker.Report) {
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
				fmt.Println("FILTERING", diffItem, "due to root directory rename")
				continue DiffItemLoop
			}

			newDiffItemList = append(newDiffItemList, diffItem)
		}

		if len(newDiffItemList) > 0 {
			fmt.Println("NOT FILTERING", newDiffItemList)
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

func (chk *Checker) validateReport(report *fswalker.Report) error {
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
