package snapstore

var _ Storer = &KopiaMetadata{}

type KopiaMetadata struct {
	MetadataRepo string
}

func NewKopiaMetadata() (*KopiaMetadata, error) {
	return &KopiaMetadata{}, nil
}

func (store *KopiaMetadata) Store(key string, val []byte) error {
	return nil
}

func (store *KopiaMetadata) Load(key string) ([]byte, error) {
	return nil, nil
}
