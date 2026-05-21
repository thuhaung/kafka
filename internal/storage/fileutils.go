package storage

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

var (
	ErrInvalidFilePath = errors.New("Invalid file path")
)

func ResolveFilePath(path string) (string, error) {
	if !filepath.IsAbs(path) {
		var err error
		path, err = filepath.Abs(path)
		if err != nil {
			return path, fmt.Errorf("%w: %v", ErrInvalidFilePath, err)
		}
	}
	return path, nil
}

func ReadFile(path string) ([]byte, error) {
	path, err := ResolveFilePath(path)
	if err != nil {
		return nil, err
	}

	return os.ReadFile(path)
}
