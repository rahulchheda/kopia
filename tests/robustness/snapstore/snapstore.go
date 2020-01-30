package snapstore

type Storer interface {
	Store(key string, value []byte) error
	Load(key string) ([]byte, error)
}
