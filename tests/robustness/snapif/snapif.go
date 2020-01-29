package snapif

type Snapshotter interface {
	TakeSnapshot(sourceDir string) (snapID string, err error)
	RestoreSnapshot(snapID string, restoreDir string) error
}
