package fswalker

type Checker struct {
	RestoreDir string
}

func (chk *Checker) TakeSnapshot(sourceDir string) (snapID string, err error) {
	return "", nil
}

func (chk *Checker) RestoreSnapshot(snapID string) error {
	return nil
}
