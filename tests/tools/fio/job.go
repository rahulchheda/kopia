package fio

import (
	"fmt"
	"strings"
)

// Job represents the configuration for running a FIO job
type Job struct {
	Name    string
	Options Options
}

func (job Job) String() string {
	ret := []string{fmt.Sprintf("[%s]", job.Name)}
	for k, v := range job.Options {
		if v == "" {
			ret = append(ret, k)
			continue
		}
		ret = append(ret, fmt.Sprintf("%s=%s", k, v))
	}

	return strings.Join(ret, "\n")
}
