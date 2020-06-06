package cli

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/volume"
	"github.com/kopia/kopia/volume/blockfile"
	"github.com/kopia/kopia/volume/volumefs"
)

var (
	volRestoreCommand            = volumeCommands.Command("restore", "Restore a volume snapshot to a device or file")
	volRestoreCommandVolID       = volRestoreCommand.Flag("vol-id", "Volume identifier").Required().Short('V').String()
	volRestoreCommandSnapID      = volRestoreCommand.Flag("vol-snapshot-id", "Volume snapshot identifier").Required().Short('I').String()
	volRestoreCommandDeviceFile  = volRestoreCommand.Flag("device-file", "Name of the device file").Required().Short('d').String()
	volRestoreCommandCreate      = volRestoreCommand.Flag("create", "Create the file if no present").Short('c').Bool()
	volRestoreCommandBlockSize   = volRestoreCommand.Flag("device-block-size", "Device block size").Short('n').Int64()
	volRestoreCommandConcurrency = volRestoreCommand.Flag("parallel", "Restore N blocks in parallel").Short('N').Default("0").Int()
)

func init() {
	volRestoreCommand.Action(repositoryAction(runVolRestoreCommand))
}

func runVolRestoreCommand(ctx context.Context, rep repo.Repository) error {
	fsArgs := &volumefs.FilesystemArgs{
		Repo:             rep,
		VolumeID:         *volRestoreCommandVolID,
		VolumeSnapshotID: *volRestoreCommandSnapID,
	}

	restoreArgs := volumefs.RestoreArgs{
		Concurrency: *volRestoreCommandConcurrency,
	}

	restoreArgs.VolumeManager = volume.FindManager(blockfile.VolumeType)
	if restoreArgs.VolumeManager == nil {
		return fmt.Errorf("blockfile not found")
	}

	wp := &blockfile.Profile{
		Name:                 *volRestoreCommandDeviceFile,
		CreateIfMissing:      *volRestoreCommandCreate,
		DeviceBlockSizeBytes: *volRestoreCommandBlockSize,
	}
	restoreArgs.VolumeAccessProfile = wp

	f, err := volumefs.New(fsArgs)
	if err != nil {
		return err
	}

	res, err := f.Restore(ctx, restoreArgs)

	if err == nil {
		fmt.Printf("Restored %s\n%s\n", res.Snapshot.Manifest.RootObjectID(), res.BlockIterStats.String())
	}

	return err
}
