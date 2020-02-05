package checker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"time"

	"github.com/kopia/kopia/tests/robustness/snapif"
	"github.com/kopia/kopia/tests/robustness/snapstore"
)

type CheckerIF interface {
	Gather(ctx context.Context, path string) ([]byte, error)
	Compare(ctx context.Context, path string, data []byte, reportOut io.Writer) error
}

type Checker struct {
	RestoreDir string
	snap       snapif.Snapshotter
	snapStore  snapstore.Storer
	validator  CheckerIF
}

func NewChecker(snap snapif.Snapshotter, snapStore snapstore.Storer, validator CheckerIF) (*Checker, error) {
	restoreDir, err := ioutil.TempDir("", "restore-data-")
	if err != nil {
		return nil, err
	}

	return &Checker{
		RestoreDir: restoreDir,
		snap:       snap,
		snapStore:  snapStore,
		validator:  validator,
	}, nil
}

func (chk *Checker) Cleanup() {
	if chk.RestoreDir != "" {
		os.RemoveAll(chk.RestoreDir)
	}
}

func (chk *Checker) GetSnapIDs() []string {
	return chk.snapStore.GetKeys()
}

type SnapshotMetadata struct {
	SnapID         string
	SnapStartTime  time.Time
	SnapEndTime    time.Time
	DeletionTime   time.Time
	ValidationData []byte
}

func (chk *Checker) TakeSnapshot(ctx context.Context, sourceDir string) (snapID string, err error) {
	b, err := chk.validator.Gather(ctx, sourceDir)
	if err != nil {
		return "", err
	}

	ssStart := time.Now()
	snapID, err = chk.snap.TakeSnapshot(sourceDir)
	if err != nil {
		return snapID, err
	}
	ssEnd := time.Now()

	ssMeta := &SnapshotMetadata{
		SnapID:         snapID,
		SnapStartTime:  ssStart,
		SnapEndTime:    ssEnd,
		ValidationData: b,
	}

	ssMetaRaw, err := json.Marshal(ssMeta)
	if err != nil {
		return snapID, err
	}

	err = chk.snapStore.Store(snapID, ssMetaRaw)
	if err != nil {
		return snapID, err
	}

	return snapID, nil
}

func (chk *Checker) RestoreSnapshot(ctx context.Context, snapID string, reportOut io.Writer) error {

	// Make an independent directory for the restore
	restoreSubDir, err := ioutil.TempDir(chk.RestoreDir, fmt.Sprintf("restore-snap-%v", snapID))
	if err != nil {
		return err
	}

	defer os.RemoveAll(restoreSubDir) //nolint:errcheck

	return chk.RestoreSnapshotToPath(ctx, snapID, restoreSubDir, reportOut)
}

func (chk *Checker) RestoreSnapshotToPath(ctx context.Context, snapID, destPath string, reportOut io.Writer) error {
	// Lookup walk data by snapshot ID
	b, err := chk.snapStore.Load(snapID)
	if err != nil {
		return err
	}
	if b == nil {
		return fmt.Errorf("could not find snapID %v", snapID)
	}

	ssMeta := &SnapshotMetadata{}
	err = json.Unmarshal(b, ssMeta)
	if err != nil {
		return err
	}

	err = chk.snap.RestoreSnapshot(snapID, destPath)
	if err != nil {
		return err
	}

	err = chk.validator.Compare(ctx, destPath, ssMeta.ValidationData, reportOut)
	if err != nil {
		return err
	}

	return nil
}

func (chk *Checker) DeleteSnapshot(ctx context.Context, snapID string) error {
	err := chk.snap.DeleteSnapshot(snapID)
	if err != nil {
		return err
	}

	return nil
}
