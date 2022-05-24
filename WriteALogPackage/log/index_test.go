package log

import (
	"github.com/stretchr/testify/require"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024

	idx, err := newIndex(f, c)
	require.NoError(t, err)

	_, _, err = idx.Read(-1)
	require.Error(t, err)

	// The index name is the same as its backing file.
	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		Off uint32
		Pos uint64
	}{
		{0, 0},
		{1, 10},
	}

	var pos uint64
	for _, want := range entries {
		err = idx.Write(want.Off, want.Pos)
		require.NoError(t, err)
		_, pos, err = idx.Read(int64(want.Off))
		require.NoError(t, err)
		require.Equal(t, want.Pos, pos)
	}

	// index and scanner should error when reading past existing entries
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	ownerReadWritePerm := os.FileMode(0600)

	// index should build its state from the existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, ownerReadWritePerm)
	idx, err = newIndex(f, c)
	require.NoError(t, err)
	off, pos, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(1), off)
	require.Equal(t, entries[1].Pos, pos)
}
