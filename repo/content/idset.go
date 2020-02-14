package content

// IDSet is a set of IDs.
type IDSet map[ID]struct{}

// Add adds id to s.
func (s IDSet) Add(id ID) {
	s[id] = struct{}{}
}

// Contains returns true if s has id in it.
func (s IDSet) Contains(id ID) bool {
	_, ok := s[id]

	return ok
}
