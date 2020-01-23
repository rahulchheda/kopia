package walker

import (
	"fmt"
	"io"
	"strings"
)

// Policy represents the a walker policy
type Policy struct {
	Name            string
	Options         map[string]string
	Include         []string
	ExcludePrefixes []string
}

const (
	includeKey    = "include"
	excludePfxKey = "exclude_pfx"
)

// Write writes a policy to the provided writer as a well-formatted policy file
func (policy *Policy) Write(w io.Writer) error {
	for _, includePath := range policy.Include {
		fmt.Fprintf(w, "%v: %q", includeKey, includePath)
	}
	for _, excludePfx := range policy.ExcludePrefixes {
		fmt.Fprintf(w, "%s: %q", excludePfxKey, excludePfx)
	}
	return nil
}

func (policy *Policy) String() string {
	w := &strings.Builder{}
	policy.Write(w)
	return w.String()
}
