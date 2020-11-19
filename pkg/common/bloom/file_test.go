package bloom

import (
	"bytes"
	"fmt"
	"io"
)

type mapFS struct {
	m map[string][]byte
}

func (fs mapFS) Open(filename string) (readerAtCloser, error) {
	bs, ok := fs.m[filename]
	if !ok {
		return nil, fmt.Errorf("file not found: %s", filename)
	}
	return &bytesReaderWithClose{bytes.NewReader(bs)}, nil
}

func (fs mapFS) Exists(filename string) bool {
	_, ok := fs.m[filename]
	return ok
}

type bytesReaderWithClose struct{ *bytes.Reader }

func (*bytesReaderWithClose) Close() error { return nil }

func (fs mapFS) Create(filename string) (io.WriteCloser, error) {
	return &mapFSWriter{m: fs.m, k: filename}, nil
}

type mapFSWriter struct {
	m map[string][]byte
	k string
}

func (w *mapFSWriter) Write(p []byte) (n int, err error) {
	w.m[w.k] = append(w.m[w.k], p...)
	return len(p), nil
}
func (w *mapFSWriter) Close() error { return nil }
