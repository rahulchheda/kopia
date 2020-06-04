package cli

import (
	"context"
	"fmt"

	"github.com/kopia/kopia/repo"
	"github.com/kopia/kopia/volume/volumefs"
)

var (
	volCompactCommand            = volumeCommands.Command("compact", "Compact a volume snapshot")
	volCompactCommandVolID       = volCompactCommand.Flag("vol-id", "Volume identifier").Required().Short('V').String()
	volCompactCommandSnapID      = volCompactCommand.Flag("vol-snapshot-id", "Volume snapshot identifier").Required().Short('I').String()
	volCompactCommandConcurrency = volCompactCommand.Flag("parallel", "Concurrency to use while compacting").PlaceHolder("N").Default("0").Int()
)

func init() {
	volCompactCommand.Action(repositoryAction(runVolCompactCommand))
}

func runVolCompactCommand(ctx context.Context, rep repo.Repository) error {
	fsArgs := &volumefs.FilesystemArgs{
		Repo:             rep,
		VolumeID:         *volCompactCommandVolID,
		VolumeSnapshotID: *volCompactCommandSnapID,
	}

	compactArgs := volumefs.CompactArgs{
		Concurrency: *volCompactCommandConcurrency,
	}

	f, err := volumefs.New(fsArgs)
	if err != nil {
		return err
	}

	cur, prev, err := f.Compact(ctx, compactArgs)

	if err == nil {
		deltaBlocks := cur.CurrentNumBlocks - (prev.CurrentNumBlocks + prev.ChainedNumBlocks)
		deltaDirs := cur.CurrentNumDirs - (prev.CurrentNumDirs + prev.ChainedNumDirs)
		fmt.Printf("%s compacted to %s Δ[%d blocks, %d dirs]\n", prev.Manifest.RootObjectID(), cur.Manifest.RootObjectID(), deltaBlocks, deltaDirs)
	}

	return err
}
