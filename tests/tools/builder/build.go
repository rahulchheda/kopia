package builder

import "fmt"

// Builder is the builder object for command
type Builder struct {
	builder *builder
	error   error
	stdout  string
	stderr  string
}

// NewBuilder returns a new instance of builder
func NewBuilder() *Builder {
	return &Builder{
		builder: &builder{
			command: []string{
				"kopia",
			},
		},
	}
}

// Add repo
func (b *Builder) Repo() {
	b.builder.command = append(b.builder.command, "repository")
}

func (b *Builder) Connect() {
	b.builder.command = append(b.builder.command, "connect")
}

func (b *Builder) S3() {
	b.builder.command = append(b.builder.command, "s3")
}

func (b *Builder) S3BucketArgs(bucketName string) {
	b.builder.command = append(b.builder.command, fmt.Sprintf("--bucket=%v", bucketName))

}

func (b *Builder) PrefixArgs(prefix string) {
	b.builder.command = append(b.builder.command, fmt.Sprintf("--prefix=%v", prefix))
}

func (b *Builder) ContentCacheSizeMbArgs(contentCacheSizeMb string) {
	b.builder.command = append(b.builder.command, fmt.Sprintf("--content-cache-size-mb%v", contentCacheSizeMb))
}
