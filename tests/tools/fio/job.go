package fio

// Job represents the configuration for running a FIO job
type Job struct {
	Name    string
	Options map[string]string
}
