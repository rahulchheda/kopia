package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
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
	volBackupCommandVolSnapID     = volBackupCommand.Flag("vol-snapshot-id", "Volume snapshot identifier").Required().Short('I').String()
	volBackupCommandPrevVolSnapID = volBackupCommand.Flag("vol-previous-snapshot-id", "Previous volume snapshot identifier.").Short('P').String()
	volBackupCommandFakeProfile   = volBackupCommand.Flag("fake-profile", "Path to the volume manager profile if -T=fake.").ExistingFile()
	volBackupCommandBlockfile     = volBackupCommand.Flag("block-file", "Path to a file if -T=blockfile.").ExistingFile()
	volBackupCommandConcurrency   = volBackupCommand.Flag("parallel", "Backup N blocks in parallel").Short('N').Default("0").Int()
	volBackupMaxChainLen          = volBackupCommand.Flag("max-chain-length", "Maximum chain length before automatic compaction").Short('M').Default("0").Int()
)

func init() {
	volBackupCommand.Action(repositoryAction(runVolBackupCommand))
}

// runVolBackupCommand is a simplified adaptation of runSnapshotCommand
func runVolBackupCommand(ctx context.Context, rep repo.Repository) error {
	fsArgs := &volumefs.FilesystemArgs{
		Repo:             rep,
		VolumeID:         *volBackupCommandVolID,
		VolumeSnapshotID: *volBackupCommandVolSnapID,
	}

	backupArgs := volumefs.BackupArgs{
		PreviousVolumeSnapshotID: *volBackupCommandPrevVolSnapID,
		Concurrency:              *volBackupCommandConcurrency,
		MaxChainLength:           *volBackupMaxChainLen,
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

	res, err := f.Backup(ctx, backupArgs)
	if err != nil {
		return err
	}

	var (
		dur = res.Snapshot.Manifest.EndTime.Sub(res.Snapshot.Manifest.StartTime)
		buf bytes.Buffer
		enc = json.NewEncoder(&buf)
	)

	_ = enc.Encode(res.Snapshot.SnapshotAnalysis)

	fmt.Printf("Created %v and ID %v in %v\n", res.Snapshot.Manifest.RootObjectID(), res.Snapshot.Manifest.ID, dur.Truncate(time.Second))
	fmt.Printf("Block Stats: %s\n", res.BlockIterStats.String())

	if res.CompactIterStats.NumBlocks > 0 {
		fmt.Printf("Compaction Stats: %s\n", res.CompactIterStats.String())
	}

	fmt.Printf("%s", strings.ReplaceAll(buf.String(), "\"", ""))

	return nil
}
