package log

import (
	"encoding/json"
	"fmt"
	"os"
	"path"

	api "github.com/MRSharff/distributed-services-with-go/api/v1"
)

type segment struct {
	store                  *store
	index                  *index
	baseOffset, nextOffset uint64
	config                 Config
}

// newSegment creates a new segment, typically used when the active segment hits
// its max size.
func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}

	var err error
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	if s.index, err = newIndex(indexFile, c); err != nil {
		return nil, err
	}

	// Set the segments next offset to prepare for the next appended record.
	off, _, err := s.index.Read(-1)
	isIndexEmpty := err != nil // use an explanatory variable here so that it is clear why were are branching.
	if isIndexEmpty {
		// If the index is empty, then the next record appended to the segment would
		// be the first record and its offset would be the segment's base offset.
		s.nextOffset = baseOffset
	} else {
		// if the index has at least one entry, then that means the offset of
		// the next record written should take the offset at the end of the segment
		s.nextOffset = baseOffset + uint64(off) + 1
	}
	return s, nil
}

// Append writes the record to the segment and returns the newly appended
// record's offset.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur
	p, err := json.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	relativeOffset := uint32(s.nextOffset - s.baseOffset)
	if err = s.index.Write(
		// index offsets are relative to base offset
		relativeOffset,
		pos,
	); err != nil {
		return 0, err
	}

	s.nextOffset++
	return cur, nil
}

// Read returns the record for the given offset.
func (s *segment) Read(off uint64) (*api.Record, error) {
	relativeOffset := int64(off - s.baseOffset)
	_, pos, err := s.index.Read(relativeOffset)
	if err != nil {
		return nil, err
	}

	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = json.Unmarshal(p, record)
	return record, err
}

// IsMaxed returns whether the segment has reached its max size, either by
// writing too much to the store or the index.
//
// If you wrote few but long logs, then you'd hit the segment bytes limit;
// if you wrote many but small logs, then you'd hit the index bytes limit.
func (s *segment) IsMaxed() bool {
	storeSize := s.store.size
	maxStoreBytes := s.config.Segment.MaxStoreBytes
	indexSize := s.index.size
	maxIndexBytes := s.config.Segment.MaxIndexBytes
	return storeSize >= maxStoreBytes || indexSize >= maxIndexBytes
}

// Remove closes the segment and removes the index and store files.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// nearestMultiple returns the nearest and lesser multiple of k in j. We take
// the lesser multiple to make sure we stay under the user's disk capacity.
//
//
// Example: nearestMultiple(9, 4) == 8
func nearestMultiple(j, k uint64) uint64 {
	if j >= 0 {
		return (j / k) * k
	}
	return ((j - k + 1) / k) * k
}
