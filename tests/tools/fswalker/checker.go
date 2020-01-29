package fswalker

import "github.com/kopia/kopia/tests/robustness/snapif"

type Checker struct {
	RestoreDir string
	snap       snapif.Snapshotter
}

func (chk *Checker) TakeSnapshot(sourceDir string) (snapID string, err error) {
	return "", nil
}

func (chk *Checker) RestoreSnapshot(snapID string) error {
	return nil
}
