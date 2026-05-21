package storage

import (
	"errors"
	"os"
	"path/filepath"
)

var (
	ErrInvalidFilePath = errors.New("Invalid file path")
)

func ReadFile(path string) ([]byte, error) {
	if !filepath.IsAbs(path) {
		var err error
		path, err = filepath.Abs(path)
		if err != nil {
			return nil, ErrInvalidFilePath
		}
	}

	return os.ReadFile(path)
}
