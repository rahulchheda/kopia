package fake

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"io"
	"io/ioutil"
	"text/template"

	"github.com/kopia/kopia/repo/logging"
	"github.com/kopia/kopia/volume"
)

// Package constants
const (
	VolumeType = "fake"
)

var log = logging.GetContextLoggerFunc("volume/fakemanager")

// init registers the manager on loading
func init() {
	volume.RegisterManager(VolumeType, &manager{})
}

// ReaderProfile contains the directives for a fake manager session
// It can be passed in directly to GetBlockReader
type ReaderProfile struct {
	Ranges    []BlockAddrRange
	numBlocks int
	blockSize int64
}

// BlockAddrRange represents an address range
type BlockAddrRange struct {
	Start    int64
	Count    int64
	Value    string
	template *template.Template
}

type manager struct{}

var _ = volume.Manager(&manager{})

func (p *manager) Type() string {
	return VolumeType
}

// GetBlockReader is a factory method for a volume.BlockReader
// The Profile may be a string with a pathname containing a JSON profile or a Profile object
func (p *manager) GetBlockReader(args volume.GetBlockReaderArgs) (volume.BlockReader, error) {
	ctx := context.Background()
	if err := args.Validate(); err != nil {
		return nil, err
	}
	var rp *ReaderProfile
	switch x := args.Profile.(type) {
	case string:
		log(ctx).Debugf("opening profile file %s", x)
		data, err := ioutil.ReadFile(x)
		if err == nil {
			rp = &ReaderProfile{}
			log(ctx).Debugf("unmarshal %s", x)
			err = json.Unmarshal(data, rp)
		}
		if err != nil {
			return nil, err
		}
	case *ReaderProfile:
		rp = x
	default:
		return nil, fmt.Errorf("invalid GetBlockReaderArgs.Profile: expected a file path or a ReaderProfile")
	}
	if err := rp.Validate(); err != nil {
		return nil, err
	}
	log(ctx).Debugf("rp: %#v", rp)
	rp.blockSize = args.BlockSizeBytes
	return rp, nil
}

// Validate verifies the ReaderProfile
func (rp *ReaderProfile) Validate() error {
	if len(rp.Ranges) == 0 {
		return nil // required to support empty snapshots
	}
	ea := int64(-1)
	cnt := int64(0)
	for i, r := range rp.Ranges {
		if ea > r.Start {
			return fmt.Errorf("range[%d] start overlaps previous range", i)
		}
		if r.Count == 0 {
			return fmt.Errorf("range[%d] count is 0", i)
		}
		ea = r.Start + r.Count
		cnt += r.Count
	}
	rp.numBlocks = int(cnt)
	return nil
}

// GetBlockAddresses fakes its namesake
func (rp *ReaderProfile) GetBlockAddresses(ctx context.Context) ([]int64, error) {
	ba := make([]int64, rp.numBlocks)
	i := 0
	for _, r := range rp.Ranges {
		for sa, k := r.Start, int64(0); k < r.Count; sa, k, i = sa+1, k+1, i+1 {
			ba[i] = sa
		}
	}
	if len(ba) > 0 {
		log(ctx).Debugf("get block addresses: %d [%012x, %012x] #r=%d", len(ba), ba[0], ba[len(ba)-1], len(rp.Ranges))
	} else {
		log(ctx).Debugf("get block addresses: no change")
	}
	return ba, nil
}

// BlockTemplateArgs are used for template substitution
type BlockTemplateArgs struct {
	Start   string
	Count   string
	Address string
}

// GetBlock fakes its namesake
func (rp *ReaderProfile) GetBlock(ctx context.Context, blockAddr int64) (io.ReadCloser, error) {
	var rangeTemplate *template.Template
	var err error
	var start, count int64
	for i, r := range rp.Ranges {
		if r.Start+r.Count < blockAddr {
			continue
		}
		if r.template == nil {
			rangeTemplate, err = template.New("").Parse(r.Value)
			if err != nil {
				return nil, fmt.Errorf("invalid template in range %#v", r)
			}
			rp.Ranges[i].template = rangeTemplate
		} else {
			rangeTemplate = r.template
		}
		start = r.Start
		count = r.Count
		break
	}
	bta := BlockTemplateArgs{
		Start:   fmt.Sprintf("%012x", start),
		Count:   fmt.Sprintf("%d", count),
		Address: fmt.Sprintf("%012x", blockAddr),
	}
	var buf bytes.Buffer
	if rangeTemplate != nil { // nil if out of range
		rangeTemplate.Execute(&buf, bta)
	}
	if buf.Len() > 0 {
		copies := 256 / buf.Len()
		fill := bytes.Repeat(buf.Bytes(), copies+1)
		fill = fill[:256]
		nFill := int(rp.blockSize) / 256
		fBuf := bytes.NewBuffer(bytes.Repeat(fill, nFill))
		ckSum3 := crc32.ChecksumIEEE(fBuf.Bytes())
		log(ctx).Debugf("get block %012x: filled [%d] cksum-o3:%d", blockAddr, nFill*len(fill), ckSum3)
		return ioutil.NopCloser(fBuf), nil
	}
	log(ctx).Debugf("get block %012x: empty [0]", blockAddr)
	return ioutil.NopCloser(&buf), nil
}
