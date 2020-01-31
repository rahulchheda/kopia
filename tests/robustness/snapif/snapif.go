package snapif

type Snapshotter interface {
	CreateRepo(args ...string) error
	ConnectRepo(args ...string) error
	ConnectOrCreateRepo(args ...string) error
	TakeSnapshot(sourceDir string) (snapID string, err error)
	RestoreSnapshot(snapID string, restoreDir string) error
}
