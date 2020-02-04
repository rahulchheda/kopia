package snapif

type Snapshotter interface {
	RepoManager
	TakeSnapshot(sourceDir string) (snapID string, err error)
	RestoreSnapshot(snapID string, restoreDir string) error
}

type RepoManager interface {
	ConnectOrCreateS3(bucketName, pathPrefix string) error
	ConnectOrCreateFilesystem(path string) error
}
