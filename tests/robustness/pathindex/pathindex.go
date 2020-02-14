package pathindex

import (
	"os"
	"path/filepath"
)

type FileNameList []string

type PathIndex struct {
	m map[string]FileNameList
}

func newPathIndex() *PathIndex {
	idx := new(PathIndex)
	idx.m = make(map[string]FileNameList)

	return idx
}

func BuildPathIndex(rootDir string) (*PathIndex, error) {
	idx := newPathIndex()

	filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			idx.m[info.Name()] = FileNameList{}
		}
	})

	return idx, nil
}
