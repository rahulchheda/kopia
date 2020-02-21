package fio

import (
	"fmt"
	"strconv"
)

// Options are flags to be set when running fio
type Options map[string]string

// List of FIO argument strings
const (
	BlockSizeFioArg        = "blocksize"
	DedupePercentageFioArg = "dedupe_percentage"
	FileSizeFioArg         = "filesize"
	IOLimitFioArg          = "io_limit"
	IOSizeFioArg           = "io_size"
	NumFilesFioArg         = "nrfiles"
	RandRepeatFioArg       = "randrepeat"
	SizeFioArg             = "size"
)

// List of FIO specific fields and delimiters
const (
	RandWriteFio  = "randwrite"
	RangeDelimFio = "-"
)

// Merge will merge two Options, overwriting common option keys
// with the incoming option values. Returns the merged result
func (o Options) Merge(other Options) Options {
	out := make(map[string]string, len(o)+len(other))

	for k, v := range o {
		out[k] = v
	}

	for k, v := range other {
		out[k] = v
	}

	return out
}

func (o Options) WithSize(sizeB int64) Options {
	return o.Merge(Options{
		SizeFioArg: strconv.Itoa(int(sizeB)),
	})
}

func (o Options) WithSizeRange(sizeMinB, sizeMaxB int64) Options {
	return o.Merge(rangeOpt(SizeFioArg, int(sizeMinB), int(sizeMaxB)))
}

func (o Options) WithIOLimit(ioSizeB int64) Options {
	return o.Merge(Options{
		IOLimitFioArg: strconv.Itoa(int(ioSizeB)),
	})
}

func (o Options) WithIOSize(sizeB int64) Options {
	return o.Merge(Options{
		IOSizeFioArg: strconv.Itoa(int(sizeB)),
	})
}

func (o Options) WithNumFiles(numFiles int) Options {
	return o.Merge(Options{
		NumFilesFioArg: strconv.Itoa(numFiles),
	})
}

func (o Options) WithFileSize(fileSizeB int64) Options {
	return o.Merge(Options{
		FileSizeFioArg: strconv.Itoa(int(fileSizeB)),
	})
}

func (o Options) WithFileSizeRange(fileSizeMinB, fileSizeMaxB int64) Options {
	return o.Merge(rangeOpt(FileSizeFioArg, int(fileSizeMinB), int(fileSizeMaxB)))
}

func (o Options) WithDedupePercentage(dPcnt int) Options {
	return o.Merge(Options{
		DedupePercentageFioArg: strconv.Itoa(dPcnt),
	})
}

func (o Options) WithBlockSize(blockSizeB int64) Options {
	return o.Merge(Options{
		BlockSizeFioArg: strconv.Itoa(int(blockSizeB)),
	})
}

func (o Options) WithRandRepeat(set bool) Options {
	return o.Merge(boolOpt(RandRepeatFioArg, set))
}

func boolOpt(key string, val bool) Options {
	if val {
		return Options{key: strconv.Itoa(1)}
	}

	return Options{key: strconv.Itoa(0)}
}

func rangeOpt(key string, min, max int) Options {
	if min > max {
		min, max = max, min
	}

	return Options{
		key: fmt.Sprintf("%d%s%d", min, RangeDelimFio, max),
	}
}
