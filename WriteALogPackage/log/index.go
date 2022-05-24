package log

// Authors Note:
// We do not handle ungraceful shutdowns to keep the code simple.

import (
	"io"
	"os"
)

// The book has tyonstate in place of tysonmote but there was an error when
// downloading that package.
// go: WriteALogPackage/log imports
//	github.com/tysontate/gommap: github.com/tysontate/gommap@v0.0.1: parsing go.mod:
//	module declares its path as: github.com/tysonmote/gommap
//	        but was required as: github.com/tysontate/gommap
// I'd like to see if I can make my own solution for mmap and learn from that.
import "github.com/tysonmote/gommap"

// Width constants define the number of bytes that make up each index entry
var (
	// entry offsets are uint32s which are 4 bytes
	offWidth uint64 = 4

	// entry positions are uint64s which are 8 bytes
	posWidth uint64 = 8

	// entWidth is used to jump straight to the position of an entry given its
	// offset since the position in the file is offset * endWidth
	entWidth = offWidth + posWidth
)

type index struct {
	// the persisted file
	file *os.File

	mmap gommap.MMap

	// the size of the index (and where to write the next entry appended to the index)
	size uint64
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	info, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(info.Size())

	// grow the file to the max index size before memory-mapping the file (why?)
	// Why? Once it is memory mapped, we cannot resize the file.
	if err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes)); err != nil {
		return nil, err
	}
	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}

// Read takes in an offset and returns the associated record's position in the store.
// The given offset is relative to the segment's base offset; 0 is always the offset
// of the index's first entry, 1 is the second entry, and so on.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	if in == -1 { // todo: What does -1 mean? Seems like -1 means read the last entry
		out = uint32((i.size / entWidth) - 1)
	} else {
		out = uint32(in) // todo: Why do we immediately assign in to out
	}
	pos = uint64(out) * entWidth // todo: And then immediately recast it back to a uint64 to use
	if outOfBounds := i.size < pos+entWidth; outOfBounds {
		return 0, 0, io.EOF
	}
	out = enc.Uint32(i.mmap[pos : pos+offWidth])
	pos = enc.Uint64(i.mmap[pos+offWidth : pos+entWidth])
	return out, pos, nil
}

func (i *index) Write(off uint32, pos uint64) error {
	if uint64(len(i.mmap)) < i.size+entWidth {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offWidth], off)
	enc.PutUint64(i.mmap[i.size+offWidth:i.size+entWidth], pos)
	i.size += uint64(entWidth) // todo: any reason why we cast to uint64, "Redundant type conversion"
	return nil
}

func (i *index) Name() string {
	return i.file.Name()
}

// Close ensures the memory-mapped file has synced its data to the persisted file
// and that the persisted file has flushed its contents to stable storage. It then
// truncates the persisted file to the amount of data that's actually in it and
// closes the file.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}

	// We grew the file past the size of the index so we could get a fully memory-mapped
	// file when we created the index. Now we need to truncate the file to the
	// true size of the index when we close it.
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}
