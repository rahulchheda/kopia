package fio

// Options are flags to be set when running fio
type Options map[string]string

// Merge will merge two Options, overwriting common option keys
// with the incoming option values. Returns the merged result
func (opt Options) Merge(otherOpt Options) map[string]string {
	for k, v := range otherOpt {
		opt[k] = v
	}

	return opt
}
