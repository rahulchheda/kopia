package snapif

type Snapshotter interface {
	CreateRepo(args ...string) error
	TakeSnapshot(sourceDir string) (snapID string, err error)
	RestoreSnapshot(snapID string, restoreDir string) error
}
