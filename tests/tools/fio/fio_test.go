package fio

import (
	"fmt"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestFIORun(t *testing.T) {
	r, err := NewRunner()
	testenv.AssertNoError(t, err)
	defer r.Cleanup()

	stdout, stderr, err := r.Run()
	testenv.AssertNoError(t, err)

	fmt.Println(stdout)
	fmt.Println("ERR", stderr)
}

func TestFIORunConfig(t *testing.T) {
	r, err := NewRunner()
	testenv.AssertNoError(t, err)
	defer r.Cleanup()

	cfgs := []Config{
		{
			{
				Name: "global",
				Options: map[string]string{
					"openfiles":         "10",
					"create_fsync":      "0",
					"create_serialize":  "1",
					"file_service_type": "sequential",
					"ioengine":          "libaio",
					"direct":            "1",
					"iodepth":           "32",
					"blocksize":         "1m",
					"refill_buffers":    "",
					"rw":                "write",
					"unique_filename":   "1",
				},
			},
			{
				Name: "write-10g",
				Options: map[string]string{
					"size":    "1g",
					"nrfiles": "10",
				},
			},
		},
	}
	stdout, stderr, err := r.RunConfigs(cfgs...)
	testenv.AssertNoError(t, err)

	fmt.Println(stdout)
	fmt.Println("STDERR", stderr)
}
