// Package walker wraps calls to the fswalker tool.
// It assumes the tool is executable by "walker", but
// gives the option to specify another executable
// path by setting environment variable FSWALKER_EXE.
package walker

import (
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"strings"
	"sync"
)

// List of walker flags
const (
	PolicyFileFlag       = "-policy-file"
	VerboseFlag          = "-verbose"
	OutputFilePrefixFlag = "-output-file-pfx"
)

// Runner is a helper for running fswalker walker commands
type Runner struct {
	Exe       string
	PolicyDir string
}

// NewRunner creates a new walker runner
func NewRunner() (*Runner, error) {
	Exe := os.Getenv("WALKER_EXE")
	if Exe == "" {
		Exe = "walker"
	}

	polDir, err := ioutil.TempDir("", "fswalker-policy-data")
	if err != nil {
		return nil, err
	}

	return &Runner{
		Exe:       Exe,
		PolicyDir: polDir,
	}, nil
}

// Cleanup cleans up the data directory
func (wr *Runner) Cleanup() {
	if wr.PolicyDir != "" {
		os.RemoveAll(wr.PolicyDir) //nolint:errcheck
	}
}

// RunPolicy runs the provided policy, temporarily writing it to a file for
// consumption by the walker tool. The operation output is written to a file at the
// output file prefix provided, and the stdout result is returned.
func (wr *Runner) RunPolicy(policy Policy, outputFilePfx string, verbose bool) (stdout string, err error) {
	f, err := ioutil.TempFile(wr.PolicyDir, policy.Name)
	if err != nil {
		return "", err
	}
	policyPath := f.Name()

	defer func() {
		f.Close()                //enolint:errcheck
		os.RemoveAll(policyPath) //enolint:errcheck
	}()

	_, err = policy.Write(f)
	if err != nil {
		return "", err
	}

	log.Printf("Running policy:\n%s\n", policy.String())

	return wr.RunPolicyFile(policyPath, outputFilePfx, verbose)
}

// RunPolicyFile takes the path to an existing policy file and runs the walker on it,
// outputting the result to the provided output file prefix
func (wr *Runner) RunPolicyFile(policyPath string, outputFilePfx string, verbose bool) (stdout string, err error) {
	args := []string{
		PolicyFileFlag, policyPath,
		OutputFilePrefixFlag, outputFilePfx,
	}

	if verbose {
		args = append(args, VerboseFlag)
	}

	stdout, _, err = wr.Run(args...)

	return stdout, err
}

// Run will execute the walker command with the given args
func (wr *Runner) Run(args ...string) (stdout, stderr string, err error) {
	argsStr := strings.Join(args, " ")
	log.Printf("running '%s %v'", wr.Exe, argsStr)
	// nolint:gosec
	c := exec.Command(wr.Exe, args...)

	stderrPipe, err := c.StderrPipe()
	if err != nil {
		return stdout, stderr, err
	}

	var errOut []byte

	var wg sync.WaitGroup

	wg.Add(1)

	go func() {
		defer wg.Done()

		errOut, err = ioutil.ReadAll(stderrPipe)
	}()

	o, err := c.Output()

	wg.Wait()

	log.Printf("finished '%s %v' with err=%v and output:\nSTDOUT:\n%v\nSTDERR:\n%v\n", wr.Exe, argsStr, err, string(o), string(errOut))

	return string(o), string(errOut), err
}
