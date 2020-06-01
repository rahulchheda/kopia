package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/volume"
	"github.com/kopia/kopia/volume/blockfile"
	fmgr "github.com/kopia/kopia/volume/fake" // register the fake manager
	"github.com/kopia/kopia/volume/volumefs"
)

var (
	volBackupCommand              = volumeCommands.Command("backup", "Backup a provider volume snapshot")
	volBackupCommandType          = volBackupCommand.Flag("vol-type", "Volume type").Required().Short('T').String()
	volBackupCommandVolID         = volBackupCommand.Flag("vol-id", "Volume identifier").Required().Short('V').String()
	volBackupCommandVolSnapID     = volBackupCommand.Flag("vol-snapshot-id", "Volume snapshot identifier").Required().Short('i').String()
	volBackupCommandPrevVolSnapID = volBackupCommand.Flag("vol-previous-snapshot-id", "Previous volume snapshot identifier. Use with '-S'.").Short('I').String()
	volBackupCommandPrevSnapID    = volBackupCommand.Flag("previous-snapshot-id", "Previous repository snapshot identifier. Use with '-I'.").Short('S').String()
	volBackupCommandFakeProfile   = volBackupCommand.Flag("fake-profile", "Path to the volume manager profile if -T=fake.").ExistingFile()
	volBackupCommandBlockfile     = volBackupCommand.Flag("block-file", "Path to a file if -T=blockfile.").ExistingFile()
	volBackupCommandConcurrency   = volBackupCommand.Flag("parallel", "Backup N blocks in parallel").PlaceHolder("N").Default("0").Int()
)

func init() {
	volBackupCommand.Action(repositoryAction(runVolBackupCommand))
}

// runVolBackupCommand is a simplified adaptation of runSnapshotCommand
func runVolBackupCommand(ctx context.Context, rep repo.Repository) error {
	if (*volBackupCommandPrevVolSnapID != "" && *volBackupCommandPrevSnapID == "") ||
		(*volBackupCommandPrevVolSnapID == "" && *volBackupCommandPrevSnapID != "") {
		return fmt.Errorf("previous values for the volume and repository must be specified together")
	}

	fsArgs := &volumefs.FilesystemArgs{
		Repo:             rep,
		VolumeID:         *volBackupCommandVolID,
		VolumeSnapshotID: *volBackupCommandVolSnapID,
	}

	backupArgs := volumefs.BackupArgs{
		PreviousSnapshotID:       *volBackupCommandPrevSnapID,
		PreviousVolumeSnapshotID: *volBackupCommandPrevVolSnapID,
		BackupConcurrency:        *volBackupCommandConcurrency,
	}

	backupArgs.VolumeManager = volume.FindManager(*volBackupCommandType)
	if backupArgs.VolumeManager == nil {
		return fmt.Errorf("volume type not supported")
	}

	switch backupArgs.VolumeManager.Type() { // setup manager type specific profiles
	case fmgr.VolumeType:
		backupArgs.VolumeAccessProfile = *volBackupCommandFakeProfile
	case blockfile.VolumeType:
		bfp := &blockfile.Profile{}
		bfp.Name = *volBackupCommandBlockfile
		backupArgs.VolumeAccessProfile = bfp
	default:
		break
	}

	f, err := volumefs.New(fsArgs)
	if err != nil {
		return err
	}

	result, err := f.Backup(ctx, backupArgs)
	if err != nil {
		return err
	}

	dur := result.Current.EndTime.Sub(result.Current.StartTime)
	printStderr("\nCreated snapshot with root %v and ID %v in %v\n", result.Current.RootObjectID(), result.Current.ID, dur.Truncate(time.Second))

	var (
		buf bytes.Buffer
		enc = json.NewEncoder(&buf)
	)

	_ = enc.Encode(result.Analyze())

	printStderr("%s\n", buf.String())

	return nil
}
