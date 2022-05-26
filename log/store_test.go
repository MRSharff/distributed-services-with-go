package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	testRecord = []byte("hello word")
	width      = uint64(len(testRecord)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer func() {
		require.NoError(t, os.Remove(f.Name()))
	}()

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	// verify that our service will recover its state after a restart
	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(testRecord)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, testRecord, read)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, offset := uint64(1), int64(0); i < 4; i++ {
		b := make([]byte, lenWidth)
		n, err := s.ReadAt(b, offset)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		offset += int64(n)

		size := enc.Uint64(b)
		b = make([]byte, size)
		n, err = s.ReadAt(b, offset)
		require.NoError(t, err)
		require.Equal(t, testRecord, b)
		require.Equal(t, int(size), n)
		offset += int64(n)
	}
}

func TestStoreClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer func(name string) {
		require.NoError(t, os.Remove(name))
	}(f.Name())

	var s *store
	s, err = newStore(f)
	require.NoError(t, err)
	var n uint64
	n, _, err = s.Append(testRecord)
	require.NoError(t, err)

	// this feels weird to grab the size of the stores backing file after we
	// seemingly have appended to it, but remember that we have a write buffer
	// which is flushed when calling store.Close().
	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)

	// the book only tests that afterSize > beforeSize, but we should know that
	// aftersize is equal to before size plus the length of our test record + lenWidth
	// which is equal to what s.Append first return value is
	require.Equal(t, int64(len(testRecord)+lenWidth), afterSize)
	require.Equal(t, uint64(afterSize), uint64(beforeSize)+n)
}

func openFile(name string) (file *os.File, size int64, err error) {
	f, err := os.OpenFile(
		name,
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, 0, err
	}
	info, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, info.Size(), nil
}
