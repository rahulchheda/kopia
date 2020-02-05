package snapif

// Snapshotter is an interface that describes methods
// for taking, restoring, deleting snapshots, and
// tracking them by a string snapshot ID.
type Snapshotter interface {
	RepoManager
	TakeSnapshot(sourceDir string) (snapID string, err error)
	RestoreSnapshot(snapID string, restoreDir string) error
	DeleteSnapshot(snapID string) error
}

// RepoManager is an interface that describes connecting to
// a repository
type RepoManager interface {
	ConnectOrCreateS3(bucketName, pathPrefix string) error
	ConnectOrCreateFilesystem(path string) error
}
