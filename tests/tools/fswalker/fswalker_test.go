package fswalker

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/google/fswalker"
	fspb "github.com/google/fswalker/proto/fswalker"
)

func TestWalkChecker_GatherCompare(t *testing.T) {
	type fields struct {
		GlobalFilterMatchers []string
	}

	for _, tt := range []struct {
		name             string
		fields           fields
		fileTreeMaker    func(root string) error
		fileTreeModifier func(root string) error
		wantErr          bool
	}{
		{
			name: "empty directory, unchanged",
			fields: fields{
				GlobalFilterMatchers: nil,
			},
			fileTreeMaker:    func(rootDir string) error { return nil },
			fileTreeModifier: func(rootDir string) error { return nil },
			wantErr:          false,
		},
		{
			name: "subdirectory tree, unchanged",
			fields: fields{
				GlobalFilterMatchers: nil,
			},
			fileTreeMaker: func(rootDir string) error {
				return os.MkdirAll(filepath.Join(rootDir, "some", "path"), 0700)
			},
			fileTreeModifier: func(rootDir string) error { return nil },
			wantErr:          false,
		},
		{
			name: "file in root, unchanged",
			fields: fields{
				GlobalFilterMatchers: nil,
			},
			fileTreeMaker: func(rootDir string) error {
				return ioutil.WriteFile(filepath.Join(rootDir, "test-file"), []byte("some data"), 0700)
			},
			fileTreeModifier: func(rootDir string) error { return nil },
			wantErr:          false,
		},
		{
			name: "file in root, contents modified",
			fields: fields{
				GlobalFilterMatchers: nil,
			},
			fileTreeMaker: func(rootDir string) error {
				return ioutil.WriteFile(filepath.Join(rootDir, "test-file"), []byte("some data"), 0700)
			},
			fileTreeModifier: func(rootDir string) error {
				ioutil.WriteFile(filepath.Join(rootDir, "test-file"), []byte("some different data"), 0700)
				return nil
			},
			wantErr: true,
		},
		{
			name: "file in deep subdirectory, contents modified",
			fields: fields{
				GlobalFilterMatchers: nil,
			},
			fileTreeMaker: func(rootDir string) error {
				subdir := filepath.Join(rootDir, "some", "really", "really", "very", "substantially", "deep", "directory", "tree")

				err := os.MkdirAll(subdir, 0700)
				if err != nil {
					return err
				}

				return ioutil.WriteFile(filepath.Join(subdir, "test-file"), []byte("some data"), 0700)
			},
			fileTreeModifier: func(rootDir string) error {
				subdir := filepath.Join(rootDir, "some", "really", "really", "very", "substantially", "deep", "directory", "tree")
				return ioutil.WriteFile(filepath.Join(subdir, "test-file"), []byte("some different data"), 0700)
			},
			wantErr: true,
		},
		{
			name: "add a file",
			fields: fields{
				GlobalFilterMatchers: nil,
			},
			fileTreeMaker: func(rootDir string) error {
				return nil
			},
			fileTreeModifier: func(rootDir string) error {
				return ioutil.WriteFile(filepath.Join(rootDir, "test-file"), []byte("some data"), 0700)
			},
			wantErr: true,
		},
		{
			name: "delete a file",
			fields: fields{
				GlobalFilterMatchers: nil,
			},
			fileTreeMaker: func(rootDir string) error {
				return ioutil.WriteFile(filepath.Join(rootDir, "test-file"), []byte("some data"), 0700)
			},
			fileTreeModifier: func(rootDir string) error {
				return os.Remove(filepath.Join(rootDir, "test-file"))
			},
			wantErr: true,
		},
		{
			name: "get an error when walking",
			fields: fields{
				GlobalFilterMatchers: nil,
			},
			fileTreeMaker: func(rootDir string) error {
				return ioutil.WriteFile(filepath.Join(rootDir, "test-file"), []byte("some data"), 0700)
			},
			fileTreeModifier: func(rootDir string) error {
				return os.Chmod(filepath.Join(rootDir, "test-file"), 0000)
			},
			wantErr: true,
		},
		{
			name: "add a directory",
			fields: fields{
				GlobalFilterMatchers: nil,
			},
			fileTreeMaker: func(rootDir string) error {
				subdir := filepath.Join(rootDir, "some", "really", "really", "very", "substantially", "deep", "directory", "tree")
				return os.MkdirAll(subdir, 0700)
			},
			fileTreeModifier: func(rootDir string) error {
				subdir := filepath.Join(rootDir, "some", "other", "path")
				return os.MkdirAll(subdir, 0700)
			},
			wantErr: true,
		},
		{
			name: "file in root, contents modified, filter just fingerprint (not size or mtime)",
			fields: fields{
				GlobalFilterMatchers: []string{
					"fingerprint:",
				},
			},
			fileTreeMaker: func(rootDir string) error {
				return ioutil.WriteFile(filepath.Join(rootDir, "test-file"), []byte("some data"), 0700)
			},
			fileTreeModifier: func(rootDir string) error {
				ioutil.WriteFile(filepath.Join(rootDir, "test-file"), []byte("some different data"), 0700)
				return nil
			},
			wantErr: true,
		},
		{
			name: "file in root, contents modified, filter fingerprint, size, and mtime",
			fields: fields{
				GlobalFilterMatchers: []string{
					"fingerprint:",
					"mtime:",
					"size",
				},
			},
			fileTreeMaker: func(rootDir string) error {
				return ioutil.WriteFile(filepath.Join(rootDir, "test-file"), []byte("some data"), 0700)
			},
			fileTreeModifier: func(rootDir string) error {
				ioutil.WriteFile(filepath.Join(rootDir, "test-file"), []byte("some different data"), 0700)
				return nil
			},
			wantErr: false,
		},
	} {
		t.Log(tt.name)

		matchers := tt.fields.GlobalFilterMatchers

		chk := &WalkCompare{
			GlobalFilterFuncs: []func(string, fswalker.ActionData) bool{
				func(inputStr string, _ fswalker.ActionData) bool {
					for _, filterStr := range matchers {
						if strings.Contains(inputStr, filterStr) {
							return true
						}
					}
					return false
				},
			},
		}

		tmpDir, err := ioutil.TempDir("", "")
		if err != nil {
			t.Fatal(err)
		}

		defer os.RemoveAll(tmpDir) //nolint:errcheck

		err = tt.fileTreeMaker(tmpDir)
		if err != nil {
			t.Fatal(err)
		}

		ctx := context.TODO()

		walk, err := chk.Gather(ctx, tmpDir)
		if err != nil {
			t.Fatal(err)
		}

		tt.fileTreeModifier(tmpDir)

		reportOut := &bytes.Buffer{}
		if err := chk.Compare(ctx, tmpDir, walk, reportOut); (err != nil) != tt.wantErr {
			t.Errorf("Compare error = %v, wantErr %v", err, tt.wantErr)
			return
		}

		// If an error was thrown, expect a report to be written to the provided writer
		if (reportOut.Len() > 0) != tt.wantErr {
			t.Errorf("report length unexpected len = %v, expReport %v", reportOut.Len(), tt.wantErr)
		}
	}
}

func TestWalkChecker_filterReportDiffs(t *testing.T) {
	type fields struct {
		GlobalFilterMatchers []string
	}

	for _, tt := range []struct {
		name        string
		fields      fields
		inputReport *fswalker.Report
		expModCount int
	}{
		{
			name: "No filters",
			fields: fields{
				GlobalFilterMatchers: nil,
			},
			inputReport: &fswalker.Report{
				Modified: []fswalker.ActionData{
					{
						Diff: "some difference",
					},
				},
			},
			expModCount: 1,
		},
		{
			name: "filter the only diff as prefix",
			fields: fields{
				GlobalFilterMatchers: []string{
					"some",
				},
			},
			inputReport: &fswalker.Report{
				Modified: []fswalker.ActionData{
					{
						Diff: "some difference",
					},
				},
			},
			expModCount: 0,
		},
		{
			name: "filter the only diff as substring",
			fields: fields{
				GlobalFilterMatchers: []string{
					"iff",
				},
			},
			inputReport: &fswalker.Report{
				Modified: []fswalker.ActionData{
					{
						Diff: "some difference",
					},
				},
			},
			expModCount: 0,
		},
		{
			name: "filter some but not all diffs",
			fields: fields{
				GlobalFilterMatchers: []string{
					"definitely",
				},
			},
			inputReport: &fswalker.Report{
				Modified: []fswalker.ActionData{
					{
						Diff: "this will not be filtered",
					},
					{
						Diff: "this will definitely be filtered",
					},
				},
			},
			expModCount: 1,
		},
		{
			name: "filter multiple diffs",
			fields: fields{
				GlobalFilterMatchers: []string{
					"definitely",
				},
			},
			inputReport: &fswalker.Report{
				Modified: []fswalker.ActionData{
					{
						Diff: "this will not be filtered",
					},
					{
						Diff: "this will definitely be filtered",
					},
					{
						Diff: "this will also definitely be filtered",
					},
				},
			},
			expModCount: 1,
		},
	} {
		t.Log(tt.name)

		matchers := tt.fields.GlobalFilterMatchers

		chk := &WalkCompare{
			GlobalFilterFuncs: []func(string, fswalker.ActionData) bool{
				func(inputStr string, _ fswalker.ActionData) bool {
					for _, filterStr := range matchers {
						if strings.Contains(inputStr, filterStr) {
							return true
						}
					}
					return false
				},
			},
		}

		chk.filterReportDiffs(tt.inputReport)

		if want, got := tt.expModCount, len(tt.inputReport.Modified); want != got {
			t.Errorf("Expected %v modifications after filter but got %v (%v)", want, got, tt.inputReport.Modified)
		}
	}
}

func Test_isRootDirectoryRename(t *testing.T) {
	type args struct {
		diffItem string
		mod      fswalker.ActionData
	}

	for _, tt := range []struct {
		name string
		args args
		want bool
	}{
		{
			name: "Check a root name change",
			args: args{
				diffItem: "name: \"fio-data-902268402\" => \"restore-snap-43720e98eaa9b40ec0be735e347bb853964221402\"",
				mod: fswalker.ActionData{
					Before: &fspb.File{
						Path: "this_is_restore_directory_root",
						Info: &fspb.FileInfo{
							IsDir: true,
						},
					},
				},
			},
			want: true,
		},
		{
			name: "Check a non-root name change",
			args: args{
				diffItem: "name: \"fio-data-902268402\" => \"restore-snap-43720e98eaa9b40ec0be735e347bb853964221402\"",
				mod: fswalker.ActionData{
					Before: &fspb.File{
						Path: filepath.Join("this_is_restore_directory_root", "with", "more", "subdirectories"),
						Info: &fspb.FileInfo{
							IsDir: true,
						},
					},
				},
			},
			want: false,
		},
		{
			name: "Check empty path",
			args: args{
				diffItem: "name: \"fio-data-902268402\" => \"restore-snap-43720e98eaa9b40ec0be735e347bb853964221402\"",
				mod: fswalker.ActionData{
					Before: &fspb.File{
						Path: "",
						Info: &fspb.FileInfo{
							IsDir: true,
						},
					},
				},
			},
			want: true,
		},
		{
			name: "Check a non-name change diff item on root",
			args: args{
				diffItem: "ctime: 2020-02-06 00:30:41 UTC => 2020-02-06 00:30:47 UTC",
				mod: fswalker.ActionData{
					Before: &fspb.File{
						Path: "",
						Info: &fspb.FileInfo{
							IsDir: true,
						},
					},
				},
			},
			want: false,
		},
		{
			name: "Check a non-name change diff item in another directory",
			args: args{
				diffItem: "ctime: 2020-02-06 00:30:41 UTC => 2020-02-06 00:30:47 UTC",
				mod: fswalker.ActionData{
					Before: &fspb.File{
						Path: filepath.Join("some", "path"),
						Info: &fspb.FileInfo{
							IsDir: true,
						},
					},
				},
			},
			want: false,
		},
		{
			name: "Check not a directory",
			args: args{
				diffItem: "name: \"fio-data-902268402\" => \"restore-snap-43720e98eaa9b40ec0be735e347bb853964221402\"",
				mod: fswalker.ActionData{
					Before: &fspb.File{
						Path: "",
						Info: &fspb.FileInfo{
							IsDir: false,
						},
					},
				},
			},
			want: false,
		},
	} {
		t.Log(tt.name)

		if got := isRootDirectoryRename(tt.args.diffItem, tt.args.mod); got != tt.want {
			t.Errorf("isRootDirectoryRename() = %v, want %v", got, tt.want)
		}
	}
}

func Test_validateReport(t *testing.T) {
	type args struct {
		report *fswalker.Report
	}

	for _, tc := range []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "no entries in the report",
			args: args{
				report: &fswalker.Report{
					Added:    nil,
					Deleted:  nil,
					Modified: nil,
					Errors:   nil,
				},
			},
			wantErr: false,
		},
		{
			name: "something was added",
			args: args{
				report: &fswalker.Report{
					Added:    []fswalker.ActionData{{}},
					Deleted:  nil,
					Modified: nil,
					Errors:   nil,
				},
			},
			wantErr: true,
		},
		{
			name: "something was deleted",
			args: args{
				report: &fswalker.Report{
					Added:    nil,
					Deleted:  []fswalker.ActionData{{}},
					Modified: nil,
					Errors:   nil,
				},
			},
			wantErr: true,
		},
		{
			name: "something was modified",
			args: args{
				report: &fswalker.Report{
					Added:    nil,
					Deleted:  nil,
					Modified: []fswalker.ActionData{{}},
					Errors:   nil,
				},
			},
			wantErr: true,
		},
		{
			name: "something hit an error",
			args: args{
				report: &fswalker.Report{
					Added:    nil,
					Deleted:  nil,
					Modified: nil,
					Errors:   []fswalker.ActionData{{}},
				},
			},
			wantErr: true,
		},
		{
			name: "multiple issues in report",
			args: args{
				report: &fswalker.Report{
					Added:    []fswalker.ActionData{{}},
					Deleted:  []fswalker.ActionData{{}},
					Modified: []fswalker.ActionData{{}},
					Errors:   []fswalker.ActionData{{}},
				},
			},
			wantErr: true,
		},
	} {
		t.Log(tc.name)

		if err := validateReport(tc.args.report); (err != nil) != tc.wantErr {
			t.Errorf("validateReport() error = %v, wantErr %v", err, tc.wantErr)
		}
	}
}
