// Package kopiarunner wraps the execution of the kopia binary.
package kopiarunner

import (
	"bufio"
	"bytes"
	"errors"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
)

const (
	repoPassword = "qWQPJ2hiiLgWRRCr" // nolint:gosec
)

// Runner is a helper for running kopia commands
type Runner struct {
	Exe         string
	ConfigDir   string
	fixedArgs   []string
	environment []string
}

// ErrExeVariableNotSet is an exported error
var ErrExeVariableNotSet = errors.New("KOPIA_EXE variable has not been set")

// NewRunner initializes a new kopia runner and returns its pointer
func NewRunner() (*Runner, error) {
	exe := os.Getenv("KOPIA_EXE")
	if exe == "" {
		return nil, ErrExeVariableNotSet
	}

	configDir, err := ioutil.TempDir("", "kopia-config")
	if err != nil {
		return nil, err
	}

	fixedArgs := []string{
		// use per-test config file, to avoid clobbering current user's setup.
		"--config-file", filepath.Join(configDir, ".kopia.config"),
	}

	return &Runner{
		Exe:         exe,
		ConfigDir:   configDir,
		fixedArgs:   fixedArgs,
		environment: []string{"KOPIA_PASSWORD=" + repoPassword},
	}, nil
}

// Cleanup cleans up the directories managed by the kopia Runner
func (kr *Runner) Cleanup() {
	if kr.ConfigDir != "" {
		os.RemoveAll(kr.ConfigDir) //nolint:errcheck
	}
}

// Run will execute the kopia command with the given args
func (kr *Runner) Run(args ...string) (stdout, stderr string, err error) {
	argsStr := strings.Join(args, " ")
	log.Printf("running '%s %v'", kr.Exe, argsStr)
	// nolint:gosec
	cmdArgs := append(append([]string(nil), kr.fixedArgs...), args...)

	// nolint:gosec
	c := exec.Command(kr.Exe, cmdArgs...)
	c.Env = append(os.Environ(), kr.environment...)

	stderrPipe, err := c.StderrPipe()
	if err != nil {
		return stdout, stderr, err
	}

	var wg sync.WaitGroup

	wg.Add(1)

	errOut := bytes.Buffer{}

	go func() {
		defer wg.Done()

		scanner := bufio.NewScanner(stderrPipe)
		for scanner.Scan() {
			log.Println(scanner.Text())
			errOut.Write(scanner.Bytes())
			errOut.WriteByte('\n')
		}
	}()

	stdoutPipe, err := c.StdoutPipe()
	if err != nil {
		return stdout, stderr, err
	}

	wg.Add(1)

	o := bytes.Buffer{}

	go func() {
		defer wg.Done()

		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			log.Println(scanner.Text())
			o.Write(scanner.Bytes())
			o.WriteByte('\n')
		}
	}()

	err = c.Run()

	wg.Wait()

	log.Printf("finished '%s %v' with err=%v and output:\nSTDOUT:\n%v\nSTDERR:\n%v", kr.Exe, argsStr, err, o.String(), errOut.String())

	return o.String(), errOut.String(), err
}
