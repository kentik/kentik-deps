package bloom

import (
	"io"
	"os"
)

// So we can mock the filesystem in tests.
var (
	createFile = osCreate
	openFile   = osOpen
	existsFile = osStatOK
)

func osCreate(filename string) (io.WriteCloser, error) { return os.Create(filename) }
func osOpen(filename string) (readerAtCloser, error)   { return os.Open(filename) }
func osStatOK(filename string) bool {
	_, err := os.Stat(filename)
	return err == nil
}

type readerAtCloser interface {
	io.ReaderAt
	io.Reader
	io.Closer
}
