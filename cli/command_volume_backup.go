package cli

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/kopia/kopia/fs"
	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/snapshot"
	"github.com/kopia/kopia/snapshot/policy"
	"github.com/kopia/kopia/snapshot/snapshotfs"
	"github.com/kopia/kopia/volume"
	"github.com/kopia/kopia/volume/blockfile"
	fmgr "github.com/kopia/kopia/volume/fake" // register the fake manager
	"github.com/kopia/kopia/volume/volumefs"

	"github.com/pkg/errors"
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

	fsArgs.VolumeManager = volume.FindManager(*volBackupCommandType)
	if fsArgs.VolumeManager == nil {
		return fmt.Errorf("volume type not supported")
	}

	switch fsArgs.VolumeManager.Type() { // setup manager type specific profiles
	case fmgr.VolumeType:
		fsArgs.VolumeAccessProfile = *volBackupCommandFakeProfile
	case blockfile.VolumeType:
		bfp := &blockfile.Profile{}
		bfp.Name = *volBackupCommandBlockfile
		fsArgs.VolumeAccessProfile = bfp
	default:
		break
	}

	f, err := volumefs.New(fsArgs)
	if err != nil {
		return err
	}

	root, err := f.InitializeForBackup(ctx, *volBackupCommandPrevSnapID, *volBackupCommandPrevVolSnapID)
	if err != nil {
		return err
	}

	u := setupUploader(rep) // defined in command_snapshot_create

	sourceInfo := f.SourceInfo()

	return snapshotSingleVolumeSource(ctx, rep, u, sourceInfo, root)
}

// snapshotSingleVolumeSource is identical to snapshotSingleSource with the exception of:
// - removed getLocalFSEntry
// - added source info and rootDir
// Note there are still references to snapshot command variables that are unused here.
func snapshotSingleVolumeSource(ctx context.Context, rep repo.Repository, u *snapshotfs.Uploader, sourceInfo snapshot.SourceInfo, rootDir fs.Entry) error {
	printStderr("Snapshotting %v ...\n", sourceInfo)

	t0 := time.Now()

	previous, err := findPreviousSnapshotManifest(ctx, rep, sourceInfo, nil)
	if err != nil {
		return err
	}

	policyTree, err := policy.TreeForSource(ctx, rep, sourceInfo)
	if err != nil {
		return errors.Wrap(err, "unable to get policy tree")
	}

	log(ctx).Debugf("uploading %v using %v previous manifests", sourceInfo, len(previous))

	manifest, err := u.Upload(ctx, rootDir, policyTree, sourceInfo, previous...)
	if err != nil {
		return err
	}

	manifest.Description = *snapshotCreateDescription
	startTimeOverride, _ := parseTimestamp(*snapshotCreateStartTime)
	endTimeOverride, _ := parseTimestamp(*snapshotCreateEndTime)

	if !startTimeOverride.IsZero() {
		if endTimeOverride.IsZero() {
			// Calculate the correct end time based on current duration if they're not specified
			duration := manifest.EndTime.Sub(manifest.StartTime)
			manifest.EndTime = startTimeOverride.Add(duration)
		}

		manifest.StartTime = startTimeOverride
	}

	if !endTimeOverride.IsZero() {
		if startTimeOverride.IsZero() {
			inverseDuration := manifest.StartTime.Sub(manifest.EndTime)
			manifest.StartTime = endTimeOverride.Add(inverseDuration)
		}

		manifest.EndTime = endTimeOverride
	}

	snapID, err := snapshot.SaveSnapshot(ctx, rep, manifest)
	if err != nil {
		return errors.Wrap(err, "cannot save manifest")
	}

	if _, err = policy.ApplyRetentionPolicy(ctx, rep, sourceInfo, true); err != nil {
		return errors.Wrap(err, "unable to apply retention policy")
	}

	if ferr := rep.Flush(ctx); ferr != nil {
		return errors.Wrap(ferr, "flush error")
	}

	progress.Finish()

	var maybePartial string
	if manifest.IncompleteReason != "" {
		maybePartial = " partial"
	}

	if ds := manifest.RootEntry.DirSummary; ds != nil {
		if ds.NumFailed > 0 {
			errorColor.Fprintf(os.Stderr, "\nIgnored %v errors while snapshotting.", ds.NumFailed) //nolint:errcheck
		}
	}

	printStderr("\nCreated%v snapshot with root %v and ID %v in %v\n", maybePartial, manifest.RootObjectID(), snapID, time.Since(t0).Truncate(time.Second))

	return err
}
