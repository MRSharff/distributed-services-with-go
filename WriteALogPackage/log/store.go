package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

// enc defines the encoding that we persist record sizes and index entries in
var enc = binary.BigEndian // todo: find a better name

// lenWidth defines the number of bytes used to store the records length
const lenWidth = 8 // todo: find a better name

// store is a simple wrapper around a file with two APIs to append and read
// bytes to and from the file
type store struct {
	*os.File
	mu sync.Mutex

	// buf is used to improve performance by reducing the number of system calls.
	// We can make many small writes to the buffer and then write the entire
	// buffer to the file in one system call.
	buf *bufio.Writer // todo: consider renaming to "writeBuffer", longer but more descriptive

	// The size of the store
	size uint64
}

// newStore creates a store for the given file.
func newStore(f *os.File) (*store, error) {
	// The file might have existing data if the service has restarted, so we
	// need to get the file's current size.
	info, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(info.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append persists the given bytes to the store.
// Return:
// n is the number of bytes written
// pos is the position where the store holds the record in its file
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	// TODO: The book talks about "persists the given bytes to the store" and
	// 	then says "we write the length of the record...". It seems to consider
	// 	the bytes of p to be the record (uint64(len(p))), can we use better
	// 	naming and add some types here? p -> record with a Record type perhaps?
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	if writeErr := binary.Write(s.buf, enc, uint64(len(p))); writeErr != nil {
		// I really dislike shadowing errors, so I won't. Also, if we're going
		// to use named return values, I feel like we should use them with the
		// naked return
		n, pos, err = 0, 0, writeErr
		return
	}

	w, writeErr := s.buf.Write(p)
	if writeErr != nil {
		n, pos, err = 0, 0, writeErr
		return
	}
	w += lenWidth
	n = uint64(w)
	s.size += n
	return
}

func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// flush the writer buffer in case we're about to read a record that the
	// buffer hasn't flushed to disk yet.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// find out how many bytes we have to read to get the whole record
	size := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}

	// fetch and return the record
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadAt implements io.ReaderAt
func (s *store) ReadAt(dst []byte, offset int64) (int, error) {
	// book has "off" instead of "offset"... really?
	// apparently this implements io.ReaderAt and they also use "off"...
	// whyyyyyy? to save 3 characters?? It's documented but really, off...
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(dst, offset)
}

func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// persist any buffered data before closing the file.
	if err := s.buf.Flush(); err != nil {
		return err
	}
	return s.File.Close()
}
