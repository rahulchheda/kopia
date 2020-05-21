package volume

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// nolint:gocritic
func TestGBRArgs(t *testing.T) {
	assert := assert.New(t)

	badTCs := []GetBlockReaderArgs{
		{},
		{VolumeID: "volumeID"},
		{VolumeID: "volumeID", SnapshotID: "snapshotID", PreviousSnapshotID: "snapshotID"},
	}
	for i, tc := range badTCs {
		err := tc.Validate()
		assert.Error(err, "case %d", i)
	}

	goodTCs := []GetBlockReaderArgs{
		{VolumeID: "volumeID", SnapshotID: "snapshotID"},
		{VolumeID: "volumeID", SnapshotID: "snapshotID", PreviousSnapshotID: "previousSnapshotID"},
	}
	for i, tc := range goodTCs {
		err := tc.Validate()
		assert.NoError(err, "case %d", i)
	}
}

// nolint:gocritic
func TestGBWArgs(t *testing.T) {
	assert := assert.New(t)

	badTCs := []GetBlockWriterArgs{
		{},
	}
	for i, tc := range badTCs {
		err := tc.Validate()
		assert.Error(err, "case %d", i)
	}

	goodTCs := []GetBlockWriterArgs{
		{VolumeID: "volumeID"},
	}
	for i, tc := range goodTCs {
		err := tc.Validate()
		assert.NoError(err, "case %d", i)
	}
}

// nolint:gocritic
func TestRegistry(t *testing.T) {
	assert := assert.New(t)

	expTm := &testManager{}
	RegisterManager(expTm.Type(), expTm)

	m := FindManager(expTm.Type())
	assert.NotNil(m)

	tm, ok := m.(*testManager)
	assert.True(ok)
	assert.Equal(expTm, tm)

	assert.Nil(FindManager(expTm.Type() + "foo"))
}

type testManager struct{}

func (tm *testManager) Type() string {
	return "testManager"
}

func (tm *testManager) GetBlockReader(args GetBlockReaderArgs) (BlockReader, error) {
	return nil, nil
}

func (tm *testManager) GetBlockWriter(args GetBlockWriterArgs) (BlockWriter, error) {
	return nil, nil
}
