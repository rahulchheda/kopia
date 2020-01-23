package fio

import (
	"fmt"
	"strings"
	"testing"

	"github.com/kopia/kopia/tests/testenv"
)

func TestFIORun(t *testing.T) {
	r, err := NewRunner()
	testenv.AssertNoError(t, err)
	defer r.Cleanup()

	stdout, stderr, err := r.Run()
	fmt.Println(stdout)
	fmt.Println(stderr)
	fmt.Println(err)
}

func TestFIORunConfig(t *testing.T) {
	r, err := NewRunner()
	testenv.AssertNoError(t, err)
	defer r.Cleanup()

	cfg := Config{
		{
			Name: "write-10g",
			Options: map[string]string{
				"size":    "1g",
				"nrfiles": "10",
			},
		},
	}
	stdout, stderr, err := r.RunConfigs(cfg)
	testenv.AssertNoError(t, err)

	fmt.Println(stdout)
	fmt.Println("STDERR", stderr)
}

func TestFIOGlobalConfigOverride(t *testing.T) {
	r, err := NewRunner()
	testenv.AssertNoError(t, err)
	defer r.Cleanup()

	cfgs := []Config{
		{
			{
				Name: "global",
				Options: map[string]string{
					"rw": "read",
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
	stdout, _, err := r.RunConfigs(cfgs...)
	testenv.AssertNoError(t, err)

	if !strings.Contains(stdout, "rw=read") {
		t.Fatal("Expected the global config 'rw' flag to be overwritten by the passed config")
	}
}
