package fio

// Config structures the fields of a FIO job run configuration
type Config []Job

func (cfg Config) String() string {
	ret := ""
	for _, job := range cfg {
		ret += job.String()
	}
	return ret
}
