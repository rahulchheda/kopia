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
func (policy *Policy) Write(w io.Writer) (tot int, err error) {
	for _, includePath := range policy.Include {
		n, err := fmt.Fprintf(w, "%v: %q", includeKey, includePath)
		tot += n

		if err != nil {
			return tot, err
		}
	}

	for _, excludePfx := range policy.ExcludePrefixes {
		n, err := fmt.Fprintf(w, "%s: %q", excludePfxKey, excludePfx)
		tot += n

		if err != nil {
			return tot, err
		}
	}

	return tot, nil
}

func (policy *Policy) String() string {
	w := &strings.Builder{}

	_, err := policy.Write(w)
	if err != nil {
		return err.Error()
	}

	return w.String()
}
