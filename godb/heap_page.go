package godb

import (
	"bytes"
	"encoding/binary"
	"unsafe"
)

/* HeapPage implements the Page interface for pages of HeapFiles. We have
provided our interface to HeapPage below for you to fill in, but you are not
required to implement these methods except for the three methods that the Page
interface requires.  You will want to use an interface like what we provide to
implement the methods of [HeapFile] that insert, delete, and iterate through
tuples.

In GoDB all tuples are fixed length, which means that given a TupleDesc it is
possible to figure out how many tuple "slots" fit on a given page.

In addition, all pages are PageSize bytes.  They begin with a header with a 32
bit integer with the number of slots (tuples), and a second 32 bit integer with
the number of used slots.

Each tuple occupies the same number of bytes.  You can use the go function
unsafe.Sizeof() to determine the size in bytes of an object.  So, a GoDB integer
(represented as an int64) requires unsafe.Sizeof(int64(0)) bytes.  For strings,
we encode them as byte arrays of StringLength, so they are size
((int)(unsafe.Sizeof(byte('a')))) * StringLength bytes.  The size in bytes  of a
tuple is just the sum of the size in bytes of its fields.

Once you have figured out how big a record is, you can determine the number of
slots on on the page as:

remPageSize = PageSize - 8 // bytes after header
numSlots = remPageSize / bytesPerTuple //integer division will round down

To serialize a page to a buffer, you can then:

write the number of slots as an int32
write the number of used slots as an int32
write the tuples themselves to the buffer

You will follow the inverse process to read pages from a buffer.

Note that to process deletions you will likely delete tuples at a specific
position (slot) in the heap page.  This means that after a page is read from
disk, tuples should retain the same slot number. Because GoDB will never evict a
dirty page, it's OK if tuples are renumbered when they are written back to disk.

*/

type heapPage struct {
	// TODO: some code goes here
	dirty     bool
	desc      *TupleDesc
	slots     []*Tuple
	f         *HeapFile
	pageNo    int
	numSlots  int
	usedSlots int
}

type heapFileRID struct {
	pageNum, slotNum int
}

// Construct a new heap page
func newHeapPage(desc *TupleDesc, pageNo int, f *HeapFile) *heapPage {
	// TODO: some code goes here
	numSlots := calNumSlot(desc)
	return &heapPage{
		dirty:     false,
		desc:      desc,
		slots:     make([]*Tuple, numSlots),
		f:         f,
		pageNo:    pageNo,
		numSlots:  numSlots,
		usedSlots: 0,
	} //replace me
}

func calBytesPerTuple(desc *TupleDesc) int {
	cnt := 0
	intSize := (int)(unsafe.Sizeof(int64(0)))
	stringSize := ((int)(unsafe.Sizeof(byte('a')))) * StringLength
	for _, field := range desc.Fields {
		if field.Ftype == IntType {
			cnt += intSize
		} else {
			cnt += stringSize
		}
	}
	return cnt
}

func calNumSlot(desc *TupleDesc) int {
	remPageSize := PageSize - 8 // bytes after header
	numSlots := remPageSize / calBytesPerTuple(desc)
	return numSlots
}

func (h *heapPage) getNumSlots() int {
	// TODO: some code goes here
	return h.numSlots //replace me
}

// Insert the tuple into a free slot on the page, or return an error if there are
// no free slots.  Set the tuples rid and return it.
func (h *heapPage) insertTuple(t *Tuple) (recordID, error) {
	// TODO: some code goes here
	for idx, slot := range h.slots {
		if slot == nil {
			rid := heapFileRID{
				pageNum: h.pageNo,
				slotNum: idx,
			}
			t.Rid = rid
			h.slots[idx] = t
			h.usedSlots += 1
			return rid, nil
		}
	}
	return nil, GoDBError{PageFullError, "slot is full in the page"} //replace me
}

// Delete the tuple in the specified slot number, or return an error if
// the slot is invalid
func (h *heapPage) deleteTuple(rid recordID) error {
	// TODO: some code goes here
	fileRID, ok := rid.(heapFileRID)
	if !ok {
		return GoDBError{TypeMismatchError, "type mismatch"}
	}
	idx := fileRID.slotNum
	if len(h.slots) <= idx || h.slots[idx] == nil {
		return GoDBError{IllegalIdxError, "the slot is invalid"}
	}
	h.slots[idx] = nil
	h.usedSlots -= 1
	return nil //replace me
}

// Page method - return whether or not the page is dirty
func (h *heapPage) isDirty() bool {
	// TODO: some code goes here
	return h.dirty //replace me
}

// Page method - mark the page as dirty
func (h *heapPage) setDirty(dirty bool) {
	// TODO: some code goes here
	h.dirty = dirty
}

// Page method - return the corresponding HeapFile
// for this page.
func (p *heapPage) getFile() *DBFile {
	// TODO: some code goes here
	file := (DBFile)(p.f)
	return &file //replace me
}

// Allocate a new bytes.Buffer and write the heap page to it. Returns an error
// if the write to the the buffer fails. You will likely want to call this from
// your [HeapFile.flushPage] method.  You should write the page header, using
// the binary.Write method in LittleEndian order, followed by the tuples of the
// page, written using the Tuple.writeTo method.
func (h *heapPage) toBuffer() (*bytes.Buffer, error) {
	// TODO: some code goes here
	b := new(bytes.Buffer)
	err := binary.Write(b, binary.LittleEndian, int32(h.numSlots))
	if err != nil {
		return nil, err
	}
	err = binary.Write(b, binary.LittleEndian, int32(h.usedSlots))
	if err != nil {
		return nil, err
	}
	for _, tuple := range h.slots {
		if tuple != nil {
			err = tuple.writeTo(b)
			if err != nil {
				return nil, err
			}
		}
	}
	return b, nil //replace me
}

// Read the contents of the HeapPage from the supplied buffer.
func (h *heapPage) initFromBuffer(buf *bytes.Buffer) error {
	// TODO: some code goes here
	var numSlots, usedSlots int32
	binary.Read(buf, binary.LittleEndian, &numSlots)
	binary.Read(buf, binary.LittleEndian, &usedSlots)
	h.numSlots = int(numSlots)
	h.usedSlots = int(usedSlots)
	//h.slots = make([]*Tuple, h.numSlots)
	for i := 0; i < h.usedSlots; i++ {
		tuple, err := readTupleFrom(buf, h.desc)
		if err != nil {
			return err
		}
		h.slots[i] = tuple
	}
	return nil //replace me
}

// Return a function that iterates through the tuples of the heap page.  Be sure
// to set the rid of the tuple to the rid struct of your choosing beforing
// return it. Return nil, nil when the last tuple is reached.
func (p *heapPage) tupleIter() func() (*Tuple, error) {
	// TODO: some code goes here
	idx := 0
	return func() (*Tuple, error) {
		for idx < p.numSlots && p.slots[idx] == nil {
			idx += 1
		}
		if idx == p.numSlots {
			return nil, nil
		} else {
			rnt := p.slots[idx]
			rnt.Rid = heapFileRID{p.pageNo, idx}
			idx += 1
			return rnt, nil
		}
	}
}
